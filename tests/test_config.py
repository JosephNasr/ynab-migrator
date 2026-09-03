from __future__ import annotations

import logging
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ynab_migrator.cli import (
    _build_engine,
    _build_parser,
    _choose_apply_scope,
    _choose_budget,
    _confirm_budget_selection,
    _load_budget_selection,
    _load_runtime_config,
    _save_budget_selection,
    main,
)
from ynab_migrator.config import load_config


VALID_CONFIG = """\
source_token = "source-secret"
dest_token = "dest-secret"
"""


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_config(self, content: str = VALID_CONFIG) -> Path:
        path = self.root / "ynab-migrator.toml"
        path.write_text(content, encoding="utf-8")
        return path

    def test_tokens_come_from_toml_and_operational_values_are_fixed(self) -> None:
        config_path = self._write_config()
        with patch("ynab_migrator.cli.DEFAULT_CONFIG_FILE", str(config_path)):
            args = _load_runtime_config(Namespace(command="resume"))
        self.assertEqual("source-secret", args.source_token)
        self.assertEqual("dest-secret", args.dest_token)
        self.assertEqual("./.ynab_migrator", args.workdir)
        self.assertEqual(200, args.tx_batch_size)
        self.assertEqual(190, args.rate_limit_per_hour)
        self.assertFalse(args.json)

    def test_engine_builder_resolves_configured_workdir(self) -> None:
        args = Namespace(
            source_plan_id="source-id",
            dest_plan_id="dest-id",
            workdir=str(self.root / "workdir"),
            tx_batch_size=200,
        )
        engine = _build_engine(
            args,
            logger=logging.getLogger("test.config"),
            source_client=Mock(),
            dest_client=Mock(),
        )
        self.assertEqual((self.root / "workdir").resolve(), engine.paths.workdir)

    def test_every_configuration_flag_is_rejected(self) -> None:
        parser = _build_parser()
        removed_flags = (
            "--config",
            "--source-token",
            "--dest-token",
            "--source-plan-id",
            "--dest-plan-id",
            "--workdir",
            "--tx-batch-size",
            "--rate-limit-per-hour",
            "--max-retries",
            "--base-retry-delay",
            "--max-retry-delay",
            "--json",
            "--no-json",
            "--verbose",
            "--no-verbose",
        )
        for flag in removed_flags:
            with self.subTest(flag=flag), redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    parser.parse_args([flag, "value", "resume"])
                self.assertEqual(2, raised.exception.code)

    def test_missing_config_file_is_rejected(self) -> None:
        missing = self.root / "missing.toml"
        with patch("ynab_migrator.cli.DEFAULT_CONFIG_FILE", str(missing)):
            with self.assertRaisesRegex(FileNotFoundError, "required config file not found"):
                _load_runtime_config(Namespace(command="plan"))

    def test_unknown_missing_and_wrong_typed_values_are_rejected(self) -> None:
        unknown_path = self._write_config(VALID_CONFIG + "surprise = true\n")
        with self.assertRaisesRegex(ValueError, "unsupported config keys"):
            load_config(unknown_path)

        missing_path = self._write_config(VALID_CONFIG.replace('dest_token = "dest-secret"\n', ""))
        with self.assertRaisesRegex(ValueError, "missing config keys.*dest_token"):
            load_config(missing_path)

        wrong_type_path = self._write_config(VALID_CONFIG.replace('source_token = "source-secret"', "source_token = 190"))
        with self.assertRaisesRegex(ValueError, "must be a string"):
            load_config(wrong_type_path)

    def test_budget_selector_receives_only_names_and_ids(self) -> None:
        class TTYBuffer(StringIO):
            def isatty(self) -> bool:
                return True

        client = SimpleNamespace(
            get_plans=lambda: {
                "plans": [
                    {"id": "budget-b", "name": "Beta", "last_modified_on": "secret-metadata"},
                    {"id": "budget-a", "name": "Alpha", "first_month": "2020-01-01"},
                ]
            }
        )
        with (
            patch("ynab_migrator.cli.sys.stdin", TTYBuffer()),
            patch("ynab_migrator.cli.sys.stdout", TTYBuffer()),
            patch("ynab_migrator.cli._run_budget_selector_curses", return_value=0) as selector,
        ):
            selected = _choose_budget(client, "source")
        displayed_budgets = selector.call_args.args[1]
        self.assertEqual([{"id": "budget-a", "name": "Alpha"}, {"id": "budget-b", "name": "Beta"}], displayed_budgets)
        self.assertEqual({"id": "budget-a", "name": "Alpha"}, selected)

    def test_budget_confirmation_requires_explicit_yes(self) -> None:
        source = {"id": "source-id", "name": "Source Name"}
        destination = {"id": "dest-id", "name": "Destination Name"}
        with patch("builtins.input", return_value="yes"), redirect_stdout(StringIO()):
            self.assertTrue(_confirm_budget_selection("plan", source, destination))
        with patch("builtins.input", return_value=""), redirect_stdout(StringIO()):
            self.assertFalse(_confirm_budget_selection("plan", source, destination))

    def test_saved_budget_selection_is_token_bound_and_contains_no_tokens(self) -> None:
        workdir = self.root / "workdir"
        _save_budget_selection(
            workdir=workdir,
            source_budget={"id": "source-id", "name": "Source Name"},
            dest_budget={"id": "dest-id", "name": "Destination Name"},
            source_token="source-secret",
            dest_token="dest-secret",
        )
        source, destination = _load_budget_selection(
            workdir,
            source_token="source-secret",
            dest_token="dest-secret",
        )
        self.assertEqual({"id": "source-id", "name": "Source Name"}, source)
        self.assertEqual({"id": "dest-id", "name": "Destination Name"}, destination)
        stored = (workdir / "budget_selection.json").read_text(encoding="utf-8")
        self.assertNotIn("source-secret", stored)
        self.assertNotIn("dest-secret", stored)
        with self.assertRaisesRegex(RuntimeError, "source token changed"):
            _load_budget_selection(
                workdir,
                source_token="replacement-source-secret",
                dest_token="dest-secret",
            )

    def test_followup_command_loads_saved_budgets_without_selection(self) -> None:
        workdir = self.root / "workdir"
        _save_budget_selection(
            workdir=workdir,
            source_budget={"id": "source-id", "name": "Source Name"},
            dest_budget={"id": "dest-id", "name": "Destination Name"},
            source_token="source-secret",
            dest_token="dest-secret",
        )

        def runtime_config(args: Namespace) -> Namespace:
            args.source_token = "source-secret"
            args.dest_token = "dest-secret"
            args.workdir = str(workdir)
            args.tx_batch_size = 200
            args.rate_limit_per_hour = 190
            args.max_retries = 8
            args.base_retry_delay = 1.0
            args.max_retry_delay = 120.0
            args.json = False
            args.verbose = False
            return args

        with (
            patch("ynab_migrator.cli._load_runtime_config", side_effect=runtime_config),
            patch("ynab_migrator.cli._build_clients", return_value=(object(), object())),
            patch("ynab_migrator.cli._choose_budget") as choose_budget,
            patch("ynab_migrator.cli._confirm_budget_selection", return_value=False) as confirm,
            redirect_stderr(StringIO()),
        ):
            self.assertEqual(1, main(["doctor"]))
        choose_budget.assert_not_called()
        confirm.assert_called_once_with(
            "doctor",
            {"id": "source-id", "name": "Source Name"},
            {"id": "dest-id", "name": "Destination Name"},
        )

    def test_successful_plan_persists_budget_selection(self) -> None:
        workdir = self.root / "workdir"

        def runtime_config(args: Namespace) -> Namespace:
            args.source_token = "source-secret"
            args.dest_token = "dest-secret"
            args.workdir = str(workdir)
            args.tx_batch_size = 200
            args.rate_limit_per_hour = 190
            args.max_retries = 8
            args.base_retry_delay = 1.0
            args.max_retry_delay = 120.0
            args.json = False
            args.verbose = False
            return args

        source_client = Mock()
        dest_client = Mock()
        engine = SimpleNamespace(
            plan=Mock(
                return_value={
                    "mode": "plan",
                    "stats": {
                        "source_counts_after_deleted_filter": {},
                        "estimated_apply_requests": {},
                    },
                    "manual_action_items": [],
                }
            )
        )
        with (
            patch("ynab_migrator.cli._load_runtime_config", side_effect=runtime_config),
            patch(
                "ynab_migrator.cli._build_clients",
                return_value=(source_client, dest_client),
            ),
            patch(
                "ynab_migrator.cli._choose_budget",
                side_effect=[
                    {"id": "source-id", "name": "Source Name"},
                    {"id": "dest-id", "name": "Destination Name"},
                ],
            ),
            patch("ynab_migrator.cli._confirm_budget_selection", return_value=True),
            patch("ynab_migrator.cli._build_engine", return_value=engine),
            redirect_stdout(StringIO()),
            redirect_stderr(StringIO()),
        ):
            self.assertEqual(0, main(["plan"]))

        source, destination = _load_budget_selection(
            workdir,
            source_token="source-secret",
            dest_token="dest-secret",
        )
        self.assertEqual({"id": "source-id", "name": "Source Name"}, source)
        self.assertEqual({"id": "dest-id", "name": "Destination Name"}, destination)

    def test_keyboard_interrupt_exits_cleanly_without_traceback(self) -> None:
        workdir = self.root / "workdir"
        _save_budget_selection(
            workdir=workdir,
            source_budget={"id": "source-id", "name": "Source Name"},
            dest_budget={"id": "dest-id", "name": "Destination Name"},
            source_token="source-secret",
            dest_token="dest-secret",
        )

        def runtime_config(args: Namespace) -> Namespace:
            args.source_token = "source-secret"
            args.dest_token = "dest-secret"
            args.workdir = str(workdir)
            args.tx_batch_size = 200
            args.rate_limit_per_hour = 190
            args.max_retries = 8
            args.base_retry_delay = 1.0
            args.max_retry_delay = 120.0
            args.json = False
            args.verbose = False
            return args

        source_client = Mock()
        dest_client = Mock()
        engine = SimpleNamespace(resume=Mock(side_effect=KeyboardInterrupt))
        stderr = StringIO()
        with (
            patch("ynab_migrator.cli._load_runtime_config", side_effect=runtime_config),
            patch(
                "ynab_migrator.cli._build_clients",
                return_value=(source_client, dest_client),
            ),
            patch("ynab_migrator.cli._confirm_budget_selection", return_value=True),
            patch("ynab_migrator.cli._build_engine", return_value=engine),
            redirect_stdout(StringIO()),
            redirect_stderr(stderr),
        ):
            self.assertEqual(130, main(["resume"]))

        rendered = stderr.getvalue()
        self.assertIn("Interrupted safely", rendered)
        self.assertIn("ynab-migrator resume", rendered)
        self.assertNotIn("Traceback", rendered)

    def test_cancelled_confirmation_does_not_start_command(self) -> None:
        def runtime_config(args: Namespace) -> Namespace:
            args.source_token = "source-secret"
            args.dest_token = "dest-secret"
            args.workdir = str(self.root / "workdir")
            args.tx_batch_size = 200
            args.rate_limit_per_hour = 190
            args.max_retries = 8
            args.base_retry_delay = 1.0
            args.max_retry_delay = 120.0
            args.json = False
            args.verbose = False
            return args

        engine = SimpleNamespace(plan=Mock())
        with (
            patch("ynab_migrator.cli._load_runtime_config", side_effect=runtime_config),
            patch("ynab_migrator.cli._build_clients", return_value=(object(), object())),
            patch(
                "ynab_migrator.cli._choose_budget",
                side_effect=[
                    {"id": "source-id", "name": "Source Name"},
                    {"id": "dest-id", "name": "Destination Name"},
                ],
            ),
            patch("ynab_migrator.cli._build_engine", return_value=engine) as engine_builder,
            patch("ynab_migrator.cli._confirm_budget_selection", return_value=False),
            redirect_stderr(StringIO()),
        ):
            self.assertEqual(1, main(["plan"]))
        engine_builder.assert_not_called()
        engine.plan.assert_not_called()

    def test_same_token_reuses_one_budget_discovery_request(self) -> None:
        def runtime_config(args: Namespace) -> Namespace:
            args.source_token = "shared-secret"
            args.dest_token = "shared-secret"
            args.workdir = str(self.root / "workdir")
            args.tx_batch_size = 200
            args.rate_limit_per_hour = 190
            args.max_retries = 8
            args.base_retry_delay = 1.0
            args.max_retry_delay = 120.0
            args.json = False
            args.verbose = False
            return args

        shared_client = Mock()
        shared_client.get_plans.return_value = {
            "plans": [
                {"id": "source-id", "name": "Source"},
                {"id": "dest-id", "name": "Destination"},
            ]
        }
        with (
            patch("ynab_migrator.cli._load_runtime_config", side_effect=runtime_config),
            patch("ynab_migrator.cli._build_clients", return_value=(shared_client, shared_client)),
            patch(
                "ynab_migrator.cli._choose_budget",
                side_effect=[
                    {"id": "source-id", "name": "Source"},
                    {"id": "dest-id", "name": "Destination"},
                ],
            ) as choose_budget,
            patch("ynab_migrator.cli._confirm_budget_selection", return_value=False),
            redirect_stderr(StringIO()),
        ):
            self.assertEqual(1, main(["plan"]))
        shared_client.get_plans.assert_called_once_with()
        self.assertEqual(2, choose_budget.call_count)
        self.assertEqual(
            shared_client.get_plans.return_value["plans"],
            choose_budget.call_args_list[0].kwargs["raw_plans"],
        )

    def test_json_changes_output_format_without_bypassing_scope_selection(self) -> None:
        class TTYBuffer(StringIO):
            def isatty(self) -> bool:
                return True

        with (
            patch("ynab_migrator.cli.sys.stdin", TTYBuffer()),
            patch("ynab_migrator.cli.sys.stdout", TTYBuffer()),
            patch("ynab_migrator.cli.sys.stderr", TTYBuffer()),
            patch("ynab_migrator.cli._run_apply_selector_curses", return_value=0) as selector,
        ):
            selected = _choose_apply_scope(logging.getLogger("test.config"), as_json=True)
        selector.assert_called_once()
        self.assertNotEqual("auto", selected["selection"])


if __name__ == "__main__":
    unittest.main()
