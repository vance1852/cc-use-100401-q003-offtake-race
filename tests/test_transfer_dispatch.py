"""外输（离泊装载）确认的幂等、并发裁决与可追溯性测试。"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from production_flow.api import JsonApplication
from production_flow.clock import FrozenClock
from production_flow.errors import Conflict, InvalidState
from production_flow.service import SupplyService
from production_flow.storage import connect


def open_service(db_path: Path | str) -> SupplyService:
    connection = connect(db_path)
    return SupplyService(
        connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
    )


def build_service(db_path: Path | str) -> SupplyService:
    service = open_service(db_path)
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部深水生产节点", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_quota_units": "500000"})
    service.create_facility("plan", {"facility_id": "pool-b", "name": "东部浮式处理中心", "kind": "floating-processing", "timezone": "Asia/Shanghai", "capacity_quota_units": "800000"})
    service.create_route("plan", {"route_id": "route-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "crude-oil", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-1", "facility_id": "cluster-a", "product": "crude-oil", "grade": "BRENT", "quantity_quota_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-1", "route_id": "route-a-b", "shipper_id": "tenant-1", "service_date": "2026-09-25", "requested_quota_units": "40000", "priority": 10, "idempotency_key": "nom-key-1"})
    service.allocate("dispatch", "route-a-b", "2026-09-25")
    return service


class TransferDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "flow.sqlite3"
        self.service = build_service(self.db_path)

    def tearDown(self) -> None:
        self.service.connection.close()
        self.tempdir.cleanup()

    def test_identical_retry_replays_first_verdict_and_deducts_once(self) -> None:
        first = self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        second = self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        third = self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        self.assertEqual(first, second)
        self.assertEqual(first, third)
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        nomination = self.service.connection.execute(
            "SELECT state,revision FROM nominations WHERE nomination_id='nom-1'"
        ).fetchone()
        self.assertEqual(nomination["state"], "in_transit")
        self.assertEqual(nomination["revision"], 3)
        transfers = self.service.connection.execute("SELECT * FROM transfers").fetchall()
        self.assertEqual(len(transfers), 1)
        events = self.service.connection.execute(
            "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
        ).fetchone()
        self.assertEqual(events["c"], 1)

    def test_same_transfer_id_with_different_lot_is_business_conflict_with_facts(self) -> None:
        first = self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        with self.assertRaises(Conflict) as caught:
            self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-2", 2)
        self.assertNotIn("sqlite", str(caught.exception).lower())
        details = caught.exception.details
        self.assertEqual(details["reason"], "transfer_id_reused")
        self.assertEqual(details["existing_transfer"]["transfer_id"], "tr-1")
        self.assertEqual(details["existing_transfer"]["inventory_lot_id"], "lot-1")
        # 冲突不改变库存与首次裁决
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        self.assertEqual(
            self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2), first
        )

    def test_different_transfer_id_on_occupied_nomination_reports_occupant(self) -> None:
        self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        with self.assertRaises(Conflict) as caught:
            self.service.dispatch_transfer("dispatch", "tr-2", "nom-1", "lot-1", 2)
        details = caught.exception.details
        self.assertEqual(details["reason"], "nomination_occupied")
        self.assertEqual(details["occupying_transfer"]["transfer_id"], "tr-1")
        self.assertEqual(details["nomination_state"], "in_transit")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")

    def test_stale_revision_returns_current_facts(self) -> None:
        with self.assertRaises(Conflict) as caught:
            self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 7)
        details = caught.exception.details
        self.assertEqual(details["reason"], "revision_mismatch")
        self.assertEqual(details["expected_revision"], 7)
        self.assertEqual(details["current_revision"], 2)
        self.assertEqual(details["current_state"], "allocated")

    def test_insufficient_inventory_reports_current_available(self) -> None:
        self.service.add_inventory_lot("dispatch", {"lot_id": "lot-small", "facility_id": "cluster-a", "product": "crude-oil", "grade": "BRENT", "quantity_quota_units": "10", "unit_cost_cny": "91", "received_at": "2026-09-24T07:00:00Z"})
        with self.assertRaises(Conflict) as caught:
            self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-small", 2)
        details = caught.exception.details
        self.assertEqual(details["reason"], "insufficient_inventory")
        self.assertEqual(details["available_quota_units"], "10.000")
        self.assertEqual(details["required_quota_units"], "40000.000")
        # 失败不留转运、不改库存、不记审计
        self.assertEqual(self.service.inventory_lot("lot-small")["available_quota_units"], "10")
        events = self.service.connection.execute(
            "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
        ).fetchone()
        self.assertEqual(events["c"], 0)

    def test_concurrent_identical_confirmations_have_single_effect(self) -> None:
        results: dict[str, object] = {}
        errors: dict[str, object] = {}
        barrier = threading.Barrier(2)

        def worker(name: str) -> None:
            service = open_service(self.db_path)
            try:
                barrier.wait(timeout=10)
                results[name] = service.dispatch_transfer(
                    "dispatch", "tr-1", "nom-1", "lot-1", 2
                )
            except Exception as exc:  # noqa: BLE001
                errors[name] = exc
            finally:
                service.connection.close()

        threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, {})
        self.assertEqual(results["w0"], results["w1"])
        self.assertEqual(results["w0"]["transfer_id"], "tr-1")
        # 重新打开数据库核验：库存只扣一次、只有一条转运与一条生效审计
        service2 = open_service(self.db_path)
        try:
            self.assertEqual(service2.inventory_lot("lot-1")["available_quota_units"], "20000.000")
            transfer_count = service2.connection.execute("SELECT COUNT(*) c FROM transfers").fetchone()
            self.assertEqual(transfer_count["c"], 1)
            nomination = service2.connection.execute(
                "SELECT state,revision FROM nominations WHERE nomination_id='nom-1'"
            ).fetchone()
            self.assertEqual(nomination["state"], "in_transit")
            self.assertEqual(nomination["revision"], 3)
            events = service2.connection.execute(
                "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
            ).fetchone()
            self.assertEqual(events["c"], 1)
            # 进程重启后按原确认编号重试，仍追溯到首次裁决
            replayed = service2.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
            self.assertEqual(replayed, results["w0"])
            self.assertEqual(service2.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        finally:
            service2.connection.close()

    def test_concurrent_distinct_transfer_ids_only_one_wins(self) -> None:
        outcomes: dict[str, dict[str, object]] = {}
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def worker(name: str, transfer_id: str) -> None:
            service = open_service(self.db_path)
            try:
                barrier.wait(timeout=10)
                result = service.dispatch_transfer(
                    "dispatch", transfer_id, "nom-1", "lot-1", 2
                )
                with lock:
                    outcomes[name] = {"ok": result}
            except Conflict as exc:
                with lock:
                    outcomes[name] = {"conflict": exc}
            finally:
                service.connection.close()

        threads = [
            threading.Thread(target=worker, args=("ship", "tr-ship")),
            threading.Thread(target=worker, args=("shore", "tr-shore")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(set(outcomes), {"ship", "shore"})
        winners = [name for name, value in outcomes.items() if "ok" in value]
        losers = [name for name, value in outcomes.items() if "conflict" in value]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        conflict = outcomes[losers[0]]["conflict"]
        self.assertEqual(conflict.details["reason"], "nomination_occupied")
        self.assertIn(conflict.details["occupying_transfer"]["transfer_id"], {"tr-ship", "tr-shore"})
        service2 = open_service(self.db_path)
        try:
            self.assertEqual(service2.inventory_lot("lot-1")["available_quota_units"], "20000.000")
            self.assertEqual(
                service2.connection.execute("SELECT COUNT(*) c FROM transfers").fetchone()["c"], 1
            )
        finally:
            service2.connection.close()

    def test_api_conflict_is_readable_and_never_leaks_sqlite(self) -> None:
        self.service.dispatch_transfer("dispatch", "tr-1", "nom-1", "lot-1", 2)
        app = JsonApplication(self.service)
        response = app.handle(
            "POST",
            "/transfers",
            {"X-Actor-Id": "dispatch", "Content-Type": "application/json"},
            json.dumps({"transfer_id": "tr-2", "nomination_id": "nom-1", "lot_id": "lot-1", "expected_revision": 2}).encode(),
        )
        self.assertEqual(response.status, 409)
        error = response.body["error"]
        self.assertEqual(error["code"], "conflict")
        self.assertNotIn("sqlite", error["message"].lower())
        self.assertNotIn("unique", error["message"].lower())
        self.assertEqual(error["details"]["reason"], "nomination_occupied")
        self.assertEqual(error["details"]["occupying_transfer"]["transfer_id"], "tr-1")


if __name__ == "__main__":
    unittest.main()
