from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from production_flow.api import JsonApplication
from production_flow.clock import FrozenClock
from production_flow.errors import Conflict
from production_flow.service import SupplyService
from production_flow.storage import connect


class TransferConfirmationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "flow.sqlite3")
        self.connection = connect(self.db_path)
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部深水生产节点", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_quota_units": "500000"})
        self.service.create_facility("plan", {"facility_id": "pool-b", "name": "东部浮式处理中心", "kind": "floating-processing", "timezone": "Asia/Shanghai", "capacity_quota_units": "800000"})
        self.service.create_route("plan", {"route_id": "route-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "crude-oil", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
        self.service.submit_nomination("dispatch", {"nomination_id": "nom-1", "route_id": "route-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_quota_units": "40000", "priority": 10, "idempotency_key": "nom-key-1"})
        self.service.allocate("dispatch", "route-a-b", "2026-09-25")
        self.service.add_inventory_lot("dispatch", {"lot_id": "lot-1", "facility_id": "cluster-a", "product": "crude-oil", "grade": "BRENT", "quantity_quota_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
        self.service.add_inventory_lot("dispatch", {"lot_id": "lot-2", "facility_id": "cluster-a", "product": "crude-oil", "grade": "BRENT", "quantity_quota_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:05:00Z"})

    def tearDown(self) -> None:
        self.connection.close()
        self.tempdir.cleanup()

    def _dispatch(self, **overrides):
        payload = {
            "transfer_id": "transfer-1",
            "nomination_id": "nom-1",
            "lot_id": "lot-1",
            "expected_revision": 2,
        }
        payload.update(overrides)
        return self.service.dispatch_transfer(
            "dispatch",
            payload["transfer_id"],
            payload["nomination_id"],
            payload["lot_id"],
            payload["expected_revision"],
            confirmation_key=payload.get("confirmation_key"),
        )

    def test_identical_retry_returns_first_confirmation_without_double_deduction(self) -> None:
        first = self._dispatch()
        retry = self._dispatch()
        self.assertEqual(first, retry)
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        nomination = self.connection.execute("SELECT state,revision FROM nominations WHERE nomination_id='nom-1'").fetchone()
        self.assertEqual(tuple(nomination), ("in_transit", 3))
        transfers = self.connection.execute("SELECT COUNT(*) c FROM transfers").fetchone()["c"]
        self.assertEqual(transfers, 1)
        audits = self.connection.execute(
            "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
        ).fetchone()["c"]
        self.assertEqual(audits, 1)

    def test_retry_with_old_revision_still_replays_instead_of_conflict(self) -> None:
        first = self._dispatch()
        # 申请此刻已是 revision 3，重复请求仍带 2，必须拿到首次结果而非版本冲突。
        self.assertEqual(first, self._dispatch(expected_revision=2))

    def test_confirmation_key_retry_is_stable_across_keys(self) -> None:
        first = self._dispatch(confirmation_key="console-vessel-key")
        # 海葵一号值班台用自己的键重发同一份确认：业务四元组相同，同样回放首次裁决。
        retry = self._dispatch(confirmation_key="console-anemone-key")
        self.assertEqual(first["transfer_id"], retry["transfer_id"])
        self.assertEqual(first["loaded_quota_units"], retry["loaded_quota_units"])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) c FROM transfers").fetchone()["c"], 1)
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) c FROM transfer_confirmations").fetchone()["c"],
            1,
        )

    def test_different_transfer_number_on_same_nomination_is_business_conflict_with_facts(self) -> None:
        self._dispatch()
        with self.assertRaises(Conflict) as caught:
            self._dispatch(transfer_id="transfer-2")
        self.assertEqual(str(caught.exception), "外输申请已被其他转运编号占用")
        details = caught.exception.details
        self.assertEqual(details["existing_transfer"]["transfer_id"], "transfer-1")
        self.assertEqual(details["nomination"]["state"], "in_transit")
        self.assertEqual(details["differences"]["transfer_id"], {"首次确认": "transfer-1", "本次请求": "transfer-2"})
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
            ).fetchone()["c"],
            1,
        )

    def test_different_lot_choice_reports_readable_conflict_and_current_facts(self) -> None:
        self._dispatch()
        with self.assertRaises(Conflict) as caught:
            self._dispatch(lot_id="lot-2")
        details = caught.exception.details
        self.assertIn("lot_id", details["differences"])
        self.assertEqual(details["existing_transfer"]["inventory_lot_id"], "lot-1")
        self.assertEqual(details["inventory_lot"]["lot_id"], "lot-2")
        # 未被采纳的选择不能扣减第二批库存。
        self.assertEqual(self.service.inventory_lot("lot-2")["available_quota_units"], "60000")

    def test_stale_revision_reports_conflict_with_expected_and_current(self) -> None:
        self._dispatch()
        with self.assertRaises(Conflict) as caught:
            self._dispatch(expected_revision=9)
        details = caught.exception.details
        self.assertEqual(details["differences"]["expected_revision"]["本次请求"], 9)
        self.assertEqual(details["nomination"]["revision"], 3)

    def test_concurrent_identical_confirmations_deduct_exactly_once(self) -> None:
        results: list[dict] = []
        errors: list[Exception] = []

        def worker() -> None:
            connection = connect(self.db_path)
            try:
                service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
                results.append(service.dispatch_transfer("dispatch", "transfer-1", "nom-1", "lot-1", 2, confirmation_key="shared-key"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                connection.close()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertFalse(errors, errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.service.inventory_lot("lot-1")["available_quota_units"], "20000.000")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) c FROM transfers").fetchone()["c"], 1)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='transfer.dispatched'"
            ).fetchone()["c"],
            1,
        )

    def test_ruling_survives_process_restart(self) -> None:
        first = self._dispatch(confirmation_key="vessel-key")
        self.connection.close()
        restarted = connect(self.db_path)
        try:
            service = SupplyService(restarted, self.clock)
            replay = service.dispatch_transfer("dispatch", "transfer-1", "nom-1", "lot-1", 2, confirmation_key="vessel-key")
            self.assertEqual(first, replay)
            self.assertEqual(
                restarted.execute("SELECT COUNT(*) c FROM transfers").fetchone()["c"],
                1,
            )
        finally:
            restarted.close()

    def test_api_conflict_body_never_contains_sqlite_exception(self) -> None:
        self._dispatch()
        app = JsonApplication(self.service)
        response = app.handle(
            "POST",
            "/transfers",
            {"X-Actor-Id": "dispatch", "Content-Type": "application/json"},
            json.dumps({"transfer_id": "transfer-2", "nomination_id": "nom-1", "lot_id": "lot-1", "expected_revision": 2}).encode(),
        )
        self.assertEqual(response.status, 409)
        body = json.dumps(response.body, ensure_ascii=False)
        self.assertEqual(response.body["error"]["code"], "conflict")
        self.assertNotIn("sqlite", body.lower())
        self.assertNotIn("UNIQUE", body)
        self.assertNotIn("IntegrityError", body)
        self.assertIn("details", response.body["error"])


if __name__ == "__main__":
    unittest.main()
