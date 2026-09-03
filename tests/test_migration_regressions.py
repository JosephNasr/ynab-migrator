from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import Mock, patch

from ynab_migrator.checkpoint import CheckpointStore
from ynab_migrator.client import RollingRateLimiter, YNABApiError, YNABClient
from ynab_migrator.locking import WorkdirLock
from ynab_migrator.migration import MigrationEngine
from ynab_migrator.utils import deterministic_import_id


class FakeClient:
    def __init__(self) -> None:
        self.transactions = []
        self.account_transactions = []
        self.created_response = {}
        self.get_transaction_calls = 0
        self.transactions_by_id = {}
        self.bulk_updates = []
        self.month = {"categories": []}
        self.month_patches = []
        self.create_calls = 0
        self.get_plan_calls = 0
        self.plan_payloads = []

    def get_plan(self, plan_id, last_knowledge_of_server=None):
        self.get_plan_calls += 1
        if self.plan_payloads:
            return self.plan_payloads.pop(0)
        return {
            "plan": {
                "accounts": [],
                "payees": [],
                "category_groups": [],
                "categories": [],
                "months": [],
                "transactions": list(self.transactions),
                "subtransactions": [],
                "scheduled_transactions": [],
                "scheduled_subtransactions": [],
            },
            "server_knowledge": self.get_plan_calls,
        }

    def get_transactions(self, plan_id):
        return {"transactions": list(self.transactions)}

    def get_transaction(self, plan_id, transaction_id):
        self.get_transaction_calls += 1
        return {"transaction": dict(self.transactions_by_id.get(transaction_id, {}))}

    def get_accounts(self, plan_id):
        return {"accounts": []}

    def get_account_transactions(self, plan_id, account_id):
        return {"transactions": list(self.account_transactions)}

    def create_transactions(self, plan_id, payload):
        self.create_calls += 1
        return dict(self.created_response)

    def update_transactions(self, plan_id, payloads):
        self.bulk_updates.append(list(payloads))
        return {"transaction_ids": [item["id"] for item in payloads]}

    def get_plan_month(self, plan_id, month):
        return {"month": self.month}

    def patch_month_category(self, plan_id, month, category_id, budgeted):
        self.month_patches.append((month, category_id, budgeted))
        return {"category": {"id": category_id, "budgeted": budgeted}}


class MigrationRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.client = FakeClient()
        self.engine = MigrationEngine(
            source_client=self.client,
            dest_client=self.client,
            source_plan_id="source",
            dest_plan_id="dest",
            workdir=self.root,
            tx_batch_size=100,
        )
        self.checkpoint = CheckpointStore(self.root / "checkpoint.sqlite3")

    def tearDown(self):
        self.checkpoint.close()
        self.tempdir.cleanup()

    def test_mapping_and_success_status_are_atomic(self):
        self.checkpoint.seed_entity_statuses("transaction", ["source-tx"])
        self.checkpoint.set_mapping("transaction", "source-tx", "dest-tx")
        self.assertEqual("dest-tx", self.checkpoint.get_mapping("transaction", "source-tx"))
        self.assertEqual(
            "succeeded",
            self.checkpoint.get_entity_status("transaction", "source-tx")["status"],
        )

    def test_same_source_and_destination_plan_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "plan IDs must be different"):
            MigrationEngine(
                source_client=self.client,
                dest_client=self.client,
                source_plan_id="same-plan",
                dest_plan_id="same-plan",
                workdir=self.root,
            )

    def test_engine_apply_enforces_workdir_lock(self):
        with WorkdirLock(self.root / "apply.lock", "test-holder"):
            with self.assertRaisesRegex(RuntimeError, "another mutating migration is active"):
                self.engine.apply()

    def test_ambiguous_account_match_fails_without_create(self):
        self.client.get_accounts = lambda plan_id: {
            "accounts": [
                {"id": "dest-1", "name": "Checking", "type": "checking"},
                {"id": "dest-2", "name": "Checking", "type": "checking"},
            ]
        }
        self.client.create_account = lambda plan_id, payload: self.fail(
            "ambiguous matching must not create an account"
        )
        with self.assertRaisesRegex(RuntimeError, "ambiguous destination account reuse"):
            self.engine._create_accounts(
                [{"id": "source-account", "name": "Checking", "type": "checking"}],
                self.checkpoint,
                {"warnings": [], "errors": [], "system_entity_stats": {}},
            )

    def test_interrupted_operations_become_ambiguous(self):
        self.checkpoint.set_entity_status("transaction", "tx", "in_progress")
        self.assertEqual(1, self.checkpoint.recover_interrupted_operations())
        self.assertEqual(
            "ambiguous_commit",
            self.checkpoint.get_entity_status("transaction", "tx")["status"],
        )

    def test_rate_limit_history_survives_process_restart(self):
        state_path = self.root / "rate-limit.json"
        RollingRateLimiter(3, state_path=state_path).acquire()
        RollingRateLimiter(3, state_path=state_path).acquire()
        self.assertEqual(2, len(json.loads(state_path.read_text(encoding="utf-8"))))

    def test_client_sends_server_knowledge_for_delta_reads(self):
        client = YNABClient("fake-token")
        client._request = Mock(return_value={})
        client.get_plan("plan", last_knowledge_of_server=42)
        client.get_transactions("plan", last_knowledge_of_server=43)
        self.assertEqual(
            {"last_knowledge_of_server": 42},
            client._request.call_args_list[0].kwargs["params"],
        )
        self.assertEqual(
            {"last_knowledge_of_server": 43},
            client._request.call_args_list[1].kwargs["params"],
        )

    def test_plan_uses_full_export_for_settings_and_all_month_budgets(self):
        source = FakeClient()
        destination = FakeClient()
        source.plan_payloads = [{
            "plan": {
                "name": "Source",
                "date_format": {"format": "MM/DD/YYYY"},
                "currency_format": {"iso_code": "USD"},
                "accounts": [],
                "payees": [],
                "category_groups": [],
                "categories": [],
                "months": [{
                    "month": "2026-01-01",
                    "note": "remember",
                    "categories": [{"id": "category", "budgeted": 1234}],
                }],
                "transactions": [],
                "subtransactions": [],
                "scheduled_transactions": [],
                "scheduled_subtransactions": [],
            },
            "server_knowledge": 10,
        }]
        engine = MigrationEngine(source, destination, "source", "dest", self.root)
        report = engine.plan()
        snapshot = json.loads((self.root / "snapshot.json").read_text(encoding="utf-8"))
        self.assertEqual(1, source.get_plan_calls)
        self.assertEqual(1, destination.get_plan_calls)
        self.assertEqual({"format": "MM/DD/YYYY"}, snapshot["source_settings"]["date_format"])
        self.assertEqual(1234, snapshot["entities"]["month_category_budgets"][0]["budgeted"])
        self.assertEqual(1, report["stats"]["source_counts_after_deleted_filter"]["months"])

    def test_plan_delta_merge_preserves_unchanged_collections_and_tombstones(self):
        current = {
            "accounts": [{"id": "a", "name": "Old"}],
            "transactions": [{"id": "t", "amount": 1}],
        }
        merged = self.engine._merge_plan_delta(
            current,
            {
                "accounts": [{"id": "a", "name": "New"}],
                "transactions": [{"id": "t", "deleted": True}],
                "payees": None,
            },
        )
        self.assertEqual("New", merged["accounts"][0]["name"])
        self.assertTrue(merged["transactions"][0]["deleted"])
        self.assertNotIn("payees", merged)

    def test_starting_balance_capture_uses_one_cached_transaction_collection(self):
        self.engine._destination_plan_cache = {
            "payees": [{"id": "starting-payee", "name": "Starting Balance"}],
            "transactions": [
                {
                    "id": "auto-a",
                    "account_id": "dest-a",
                    "payee_id": "starting-payee",
                    "date": "2026-01-01",
                    "amount": 0,
                },
                {
                    "id": "auto-b",
                    "account_id": "dest-b",
                    "payee_id": "starting-payee",
                    "date": "2026-01-01",
                    "amount": 0,
                },
            ],
        }
        self.checkpoint.set_mapping("account", "source-a", "dest-a")
        self.checkpoint.set_mapping("account", "source-b", "dest-b")
        self.checkpoint.set_metadata(
            "newly_created_source_account_ids", ["source-a", "source-b"]
        )
        self.engine._capture_destination_auto_starting_balance_candidates(
            [{"id": "source-a"}, {"id": "source-b"}],
            self.checkpoint,
            {"system_entity_stats": {}},
        )
        self.assertEqual(
            {"source-a": ["auto-a"], "source-b": ["auto-b"]},
            self.checkpoint.get_metadata("auto_starting_balance_candidates"),
        )

    @patch("ynab_migrator.migration.time.sleep", return_value=None)
    def test_ids_only_create_response_is_reconciled_after_visibility_delay(self, _sleep):
        import_id = "MG:example"
        calls = {"count": 0}

        def delayed_transactions(plan_id):
            calls["count"] += 1
            if calls["count"] < 2:
                return {"transactions": []}
            return {"transactions": [{"id": "dest-tx", "import_id": import_id}]}

        self.client.get_transactions = delayed_transactions
        self.checkpoint.seed_entity_statuses("transaction", ["source-tx"])
        self.engine._apply_transaction_batch_response(
            checkpoint=self.checkpoint,
            response_data={"transaction_ids": ["dest-tx"]},
            source_by_import={import_id: "source-tx"},
            existing_import_map={},
            report={"errors": [], "warnings": []},
        )
        self.assertEqual("dest-tx", self.checkpoint.get_mapping("transaction", "source-tx"))

    @patch("ynab_migrator.migration.time.sleep", return_value=None)
    def test_ids_only_batches_share_one_grouped_delta_reconciliation(self, _sleep):
        self.engine._destination_plan_cache = {"transactions": []}
        self.engine._destination_server_knowledge = 20
        self.client.plan_payloads = [{
            "plan": {
                "transactions": [
                    {"id": "dest-a", "import_id": "MG:a"},
                    {"id": "dest-b", "import_id": "MG:b"},
                ]
            },
            "server_knowledge": 21,
        }]
        existing_import_map = {}
        for import_id, source_id, dest_id in (
            ("MG:a", "source-a", "dest-a"),
            ("MG:b", "source-b", "dest-b"),
        ):
            self.checkpoint.seed_entity_statuses("transaction", [source_id])
            self.engine._apply_transaction_batch_response(
                checkpoint=self.checkpoint,
                response_data={"transaction_ids": [dest_id]},
                source_by_import={import_id: source_id},
                existing_import_map=existing_import_map,
                report={"errors": [], "warnings": []},
                defer_reconciliation=True,
            )
        report = {"errors": [], "warnings": []}
        self.engine._reconcile_pending_transaction_imports(
            self.checkpoint,
            existing_import_map,
            report,
        )
        self.assertEqual(1, self.client.get_plan_calls)
        self.assertEqual("dest-a", self.checkpoint.get_mapping("transaction", "source-a"))
        self.assertEqual("dest-b", self.checkpoint.get_mapping("transaction", "source-b"))
        self.assertEqual([], report["errors"])

    def test_legacy_cursor_cannot_hide_recoverable_transaction(self):
        import_id = deterministic_import_id("source", "source-tx")
        self.client.transactions = [{"id": "dest-tx", "import_id": import_id}]
        self.checkpoint.set_cursor("transactions_idx", 379)
        self.checkpoint.set_mapping("account", "source-account", "dest-account")
        self.engine._create_transactions(
            [{
                "id": "source-tx", "account_id": "source-account", "date": "2026-01-01",
                "amount": -100, "approved": True, "subtransactions": [],
            }],
            {"source-account": {"id": "source-account"}},
            {},
            self.checkpoint,
            {"errors": [], "warnings": []},
        )
        self.assertEqual("dest-tx", self.checkpoint.get_mapping("transaction", "source-tx"))
        self.assertEqual(0, self.client.create_calls)

    def test_split_parent_category_is_ignored_without_warning(self):
        report = {"errors": [], "warnings": []}
        payload, error = self.engine._build_transaction_payload(
            tx={
                "id": "tx",
                "account_id": "source-account",
                "date": "2026-01-01",
                "amount": -100,
                "category_id": "synthetic-parent-category",
                "subtransactions": [
                    {"amount": -100, "category_id": "source-category", "deleted": False}
                ],
            },
            source_accounts_by_id={},
            source_payees_by_id={},
            account_map={"source-account": "dest-account"},
            category_map={"source-category": "dest-category"},
            payee_map={},
            include_import_id=True,
            report=report,
        )
        self.assertIsNone(error)
        self.assertEqual(None, payload["category_id"])
        self.assertEqual("dest-category", payload["subtransactions"][0]["category_id"])
        self.assertEqual([], report["warnings"])

    @patch("ynab_migrator.migration.time.sleep", return_value=None)
    def test_single_409_is_reconciled_as_idempotent_success(self, _sleep):
        import_id = "MG:existing"

        def conflict(plan_id, payload):
            raise YNABApiError(409, "same import_id")

        self.client.create_transactions = conflict
        self.client.transactions = [{"id": "dest-existing", "import_id": import_id}]
        self.checkpoint.seed_entity_statuses("transaction", ["source-tx"])
        success = self.engine._submit_single_transaction_entry(
            entry={
                "source_id": "source-tx",
                "import_id": import_id,
                "payload": {"account_id": "dest-account", "import_id": import_id},
                "tx": {"id": "source-tx"},
            },
            checkpoint=self.checkpoint,
            report={"errors": [], "warnings": []},
            existing_import_map={},
        )
        self.assertTrue(success)
        self.assertEqual("dest-existing", self.checkpoint.get_mapping("transaction", "source-tx"))

    def test_transfer_uses_returned_counterpart_and_bulk_status_patch(self):
        self.client.created_response = {
            "transaction": {
                "id": "dest-primary",
                "account_id": "dest-a",
                "date": "2026-01-01",
                "amount": -100,
                "transfer_transaction_id": "dest-counterpart",
            }
        }
        self.client.transactions_by_id = {
            "dest-counterpart": {
                "id": "dest-counterpart",
                "account_id": "dest-b",
                "date": "2026-01-01",
                "amount": 100,
                "transfer_transaction_id": "dest-primary",
            }
        }
        self.checkpoint.set_mapping("account", "source-a", "dest-a")
        self.checkpoint.set_mapping("account", "source-b", "dest-b")
        self.checkpoint.set_mapping("payee", "source-transfer-payee", "dest-transfer-payee")
        primary = {
            "id": "source-primary", "account_id": "source-a", "transfer_account_id": "source-b",
            "date": "2026-01-01", "amount": -100, "cleared": "cleared",
        }
        counterpart = {
            "id": "source-counterpart", "account_id": "source-b", "transfer_account_id": "source-a",
            "date": "2026-01-01", "amount": 100, "cleared": "uncleared",
        }
        self.engine._process_transfer_transaction_pair(
            primary_tx=primary,
            counterpart_tx=counterpart,
            source_accounts_by_id={
                "source-a": {"transfer_payee_id": "unused"},
                "source-b": {"transfer_payee_id": "source-transfer-payee"},
            },
            source_payees_by_id={},
            checkpoint=self.checkpoint,
            report={"errors": [], "warnings": []},
            account_map=self.checkpoint.get_mapping_dict("account"),
            category_map={},
            payee_map=self.checkpoint.get_mapping_dict("payee"),
        )
        self.assertEqual(1, self.client.get_transaction_calls)
        self.assertEqual("dest-primary", self.checkpoint.get_mapping("transaction", "source-primary"))
        self.assertEqual("dest-counterpart", self.checkpoint.get_mapping("transaction", "source-counterpart"))
        self.engine._flush_transfer_cleared_patches(self.checkpoint, {"warnings": []})
        self.assertEqual(1, len(self.client.bulk_updates))
        self.assertEqual(2, len(self.client.bulk_updates[0]))

    def test_transfer_counterparts_are_validated_after_one_grouped_delta_refresh(self):
        self.client.created_response = {
            "transaction": {
                "id": "dest-primary",
                "account_id": "dest-a",
                "date": "2026-01-01",
                "amount": -100,
                "transfer_transaction_id": "dest-counterpart",
            }
        }
        self.engine._destination_plan_cache = {"transactions": []}
        self.engine._destination_server_knowledge = 10
        self.client.plan_payloads = [{
            "plan": {
                "transactions": [{
                    "id": "dest-counterpart",
                    "account_id": "dest-b",
                    "date": "2026-01-01",
                    "amount": 100,
                    "transfer_transaction_id": "dest-primary",
                }]
            },
            "server_knowledge": 11,
        }]
        self.checkpoint.set_mapping("account", "source-a", "dest-a")
        self.checkpoint.set_mapping("account", "source-b", "dest-b")
        self.checkpoint.set_mapping("payee", "source-transfer-payee", "dest-transfer-payee")
        primary = {
            "id": "source-primary", "account_id": "source-a", "transfer_account_id": "source-b",
            "date": "2026-01-01", "amount": -100,
        }
        counterpart = {
            "id": "source-counterpart", "account_id": "source-b", "transfer_account_id": "source-a",
            "date": "2026-01-01", "amount": 100,
        }
        report = {"errors": [], "warnings": []}
        self.engine._process_transfer_transaction_pair(
            primary_tx=primary,
            counterpart_tx=counterpart,
            source_accounts_by_id={
                "source-a": {"transfer_payee_id": "unused"},
                "source-b": {"transfer_payee_id": "source-transfer-payee"},
            },
            source_payees_by_id={},
            checkpoint=self.checkpoint,
            report=report,
            account_map=self.checkpoint.get_mapping_dict("account"),
            category_map={},
            payee_map=self.checkpoint.get_mapping_dict("payee"),
        )
        self.engine._reconcile_pending_transfer_pairs(
            self.checkpoint,
            report,
            self.checkpoint.get_mapping_dict("account"),
        )
        self.assertEqual(1, self.client.get_plan_calls)
        self.assertEqual(0, self.client.get_transaction_calls)
        self.assertEqual("dest-primary", self.checkpoint.get_mapping("transaction", "source-primary"))
        self.assertEqual("dest-counterpart", self.checkpoint.get_mapping("transaction", "source-counterpart"))

    def test_transfer_with_nonreciprocal_counterpart_is_not_mapped(self):
        self.client.created_response = {
            "transaction": {
                "id": "dest-primary",
                "account_id": "dest-a",
                "date": "2026-01-01",
                "amount": -100,
                "transfer_transaction_id": "dest-counterpart",
            }
        }
        self.client.transactions_by_id = {
            "dest-counterpart": {
                "id": "dest-counterpart",
                "account_id": "dest-b",
                "date": "2026-01-01",
                "amount": 100,
                "transfer_transaction_id": "some-other-transaction",
            }
        }
        self.checkpoint.set_mapping("account", "source-a", "dest-a")
        self.checkpoint.set_mapping("account", "source-b", "dest-b")
        self.checkpoint.set_mapping("payee", "source-transfer-payee", "dest-transfer-payee")
        primary = {
            "id": "source-primary", "account_id": "source-a", "transfer_account_id": "source-b",
            "date": "2026-01-01", "amount": -100,
        }
        counterpart = {
            "id": "source-counterpart", "account_id": "source-b", "transfer_account_id": "source-a",
            "date": "2026-01-01", "amount": 100,
        }
        report = {"errors": [], "warnings": []}
        self.engine._process_transfer_transaction_pair(
            primary_tx=primary,
            counterpart_tx=counterpart,
            source_accounts_by_id={
                "source-a": {"transfer_payee_id": "unused"},
                "source-b": {"transfer_payee_id": "source-transfer-payee"},
            },
            source_payees_by_id={},
            checkpoint=self.checkpoint,
            report=report,
            account_map=self.checkpoint.get_mapping_dict("account"),
            category_map={},
            payee_map=self.checkpoint.get_mapping_dict("payee"),
        )
        self.assertIsNone(self.checkpoint.get_mapping("transaction", "source-primary"))
        self.assertIsNone(self.checkpoint.get_mapping("transaction", "source-counterpart"))
        self.assertEqual("ambiguous_commit", self.checkpoint.get_entity_status("transaction", "source-primary")["status"])
        self.assertTrue(report["errors"])

    def test_equal_month_budget_is_a_noop(self):
        self.client.month = {"categories": [{"id": "dest-category", "budgeted": 0}]}
        self.checkpoint.set_mapping("category", "source-category", "dest-category")
        self.engine._apply_month_budgets(
            [{"month": "2026-01-01", "category_id": "source-category", "budgeted": 0}],
            self.checkpoint,
            {"warnings": [], "errors": []},
            set(),
        )
        self.assertEqual([], self.client.month_patches)
        self.assertEqual(
            "succeeded",
            self.checkpoint.get_entity_status("month_budget", "2026-01-01:source-category")["status"],
        )


if __name__ == "__main__":
    unittest.main()
