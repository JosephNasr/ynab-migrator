from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import curses
except Exception:  # noqa: BLE001
    curses = None

from .client import RetryConfig, YNABClient
from .config import DEFAULT_CONFIG_FILE, load_config
from .migration import (
    APPLY_PROFILE_DEFAULT,
    MigrationEngine,
    get_apply_entity_prompt_options,
    resolve_apply_entities,
)
from .runtime_logging import build_runtime_logger, command_log_path
from .utils import atomic_write_json, now_utc_iso, read_json


BUDGET_SELECTION_FILE = "budget_selection.json"
BUDGET_SELECTION_SCHEMA_VERSION = 1


def _token_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _budget_selection_path(workdir: Path) -> Path:
    return workdir / BUDGET_SELECTION_FILE


def _validate_saved_budget(value: Any, role: str) -> Dict[str, str]:
    if not isinstance(value, dict):
        raise RuntimeError(f"saved {role} budget is missing or invalid; rerun `ynab-migrator plan`")
    budget_id = value.get("id")
    budget_name = value.get("name")
    if not isinstance(budget_id, str) or not budget_id.strip():
        raise RuntimeError(f"saved {role} budget ID is invalid; rerun `ynab-migrator plan`")
    if not isinstance(budget_name, str) or not budget_name.strip():
        raise RuntimeError(f"saved {role} budget name is invalid; rerun `ynab-migrator plan`")
    clean_name = " ".join(
        "".join(character if character.isprintable() else " " for character in budget_name).split()
    )
    if not clean_name:
        raise RuntimeError(f"saved {role} budget name is invalid; rerun `ynab-migrator plan`")
    return {"id": budget_id.strip(), "name": clean_name}


def _save_budget_selection(
    workdir: Path,
    source_budget: Dict[str, str],
    dest_budget: Dict[str, str],
    source_token: str,
    dest_token: str,
) -> Path:
    path = _budget_selection_path(workdir)
    atomic_write_json(
        path,
        {
            "schema_version": BUDGET_SELECTION_SCHEMA_VERSION,
            "saved_at": now_utc_iso(),
            "source_token_fingerprint": _token_fingerprint(source_token),
            "dest_token_fingerprint": _token_fingerprint(dest_token),
            "source_budget": _validate_saved_budget(source_budget, "source"),
            "destination_budget": _validate_saved_budget(dest_budget, "destination"),
        },
    )
    return path


def _load_budget_selection(
    workdir: Path,
    source_token: str,
    dest_token: str,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    path = _budget_selection_path(workdir)
    if not path.is_file():
        raise FileNotFoundError(
            f"saved budget selection not found at {path}; run `ynab-migrator plan` first"
        )
    try:
        payload = read_json(path)
    except (OSError, ValueError, TypeError) as error:
        raise RuntimeError(
            f"saved budget selection at {path} cannot be read; rerun `ynab-migrator plan`"
        ) from error
    if not isinstance(payload, dict) or payload.get("schema_version") != BUDGET_SELECTION_SCHEMA_VERSION:
        raise RuntimeError("saved budget selection is incompatible; rerun `ynab-migrator plan`")
    if payload.get("source_token_fingerprint") != _token_fingerprint(source_token):
        raise RuntimeError("source token changed since planning; rerun `ynab-migrator plan`")
    if payload.get("dest_token_fingerprint") != _token_fingerprint(dest_token):
        raise RuntimeError("destination token changed since planning; rerun `ynab-migrator plan`")

    source_budget = _validate_saved_budget(payload.get("source_budget"), "source")
    dest_budget = _validate_saved_budget(payload.get("destination_budget"), "destination")
    if source_budget["id"] == dest_budget["id"]:
        raise RuntimeError("saved source and destination budgets must be different; rerun `ynab-migrator plan`")
    return source_budget, dest_budget


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ynab-migrator",
        description=(
            "Resumable YNAB plan migration CLI. "
            "Tombstoned/deleted entities are always ignored and never replayed."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("plan", help="Build immutable snapshot and preflight report")
    subparsers.add_parser("apply", help="Apply migration using snapshot and checkpoint")
    subparsers.add_parser("verify", help="Verify migrated subset parity")
    subparsers.add_parser("resume", help="Alias for apply; continue from checkpoint")
    subparsers.add_parser("doctor", help="Diagnose checkpoint coverage and recoverable mappings")
    return parser


def _load_runtime_config(args: argparse.Namespace) -> argparse.Namespace:
    config_path = Path(DEFAULT_CONFIG_FILE)
    if not config_path.is_file():
        raise FileNotFoundError(
            f"required config file not found: {config_path.resolve()}; "
            f"copy ynab-migrator.example.toml to {DEFAULT_CONFIG_FILE}"
        )
    config = load_config(config_path)
    for key, value in config.items():
        setattr(args, key, value.strip() if isinstance(value, str) else value)
    fixed_runtime_values = {
        "workdir": "./.ynab_migrator",
        "tx_batch_size": 200,
        "rate_limit_per_hour": 190,
        "max_retries": 8,
        "base_retry_delay": 1.0,
        "max_retry_delay": 120.0,
        "json": False,
        "verbose": False,
    }
    for key, value in fixed_runtime_values.items():
        setattr(args, key, value)
    missing = [
        key
        for key in ("source_token", "dest_token")
        if not isinstance(getattr(args, key, None), str) or not getattr(args, key).strip()
    ]
    if missing:
        rendered = ", ".join(key.replace("_", "-") for key in missing)
        raise ValueError(f"empty required configuration in {config_path}: {rendered}")
    return args


def _build_clients(
    args: argparse.Namespace,
    logger: logging.Logger,
) -> Tuple[YNABClient, YNABClient]:
    retry = RetryConfig(
        max_retries=max(0, int(args.max_retries)),
        base_delay_seconds=max(0.1, float(args.base_retry_delay)),
        max_delay_seconds=max(1.0, float(args.max_retry_delay)),
    )
    workdir = Path(args.workdir).resolve()
    source_token_key = _token_fingerprint(args.source_token)
    dest_token_key = _token_fingerprint(args.dest_token)
    source_client = YNABClient(
        token=args.source_token,
        rate_limit_per_hour=max(1, int(args.rate_limit_per_hour)),
        retry_config=retry,
        logger=logger.getChild("client.source"),
        rate_limiter_state_path=workdir / f"rate-limit-{source_token_key}.json",
    )
    dest_client = YNABClient(
        token=args.dest_token,
        rate_limit_per_hour=max(1, int(args.rate_limit_per_hour)),
        retry_config=retry,
        logger=logger.getChild("client.dest"),
        rate_limiter=source_client.rate_limiter if args.source_token == args.dest_token else None,
        rate_limiter_state_path=(
            None if args.source_token == args.dest_token
            else workdir / f"rate-limit-{dest_token_key}.json"
        ),
    )
    return source_client, dest_client


def _build_engine(
    args: argparse.Namespace,
    logger: logging.Logger,
    source_client: YNABClient,
    dest_client: YNABClient,
) -> MigrationEngine:
    return MigrationEngine(
        source_client=source_client,
        dest_client=dest_client,
        source_plan_id=args.source_plan_id,
        dest_plan_id=args.dest_plan_id,
        workdir=Path(args.workdir).resolve(),
        tx_batch_size=max(1, int(args.tx_batch_size)),
        logger=logger.getChild("migration"),
    )


def _run_apply_selector_curses(options: List[Dict[str, Any]]) -> Optional[int]:
    if curses is None:
        return None

    def _selector(stdscr: Any) -> Optional[int]:
        selected_index = 0
        stdscr.keypad(True)
        try:
            curses.curs_set(0)
        except Exception:  # noqa: BLE001
            pass

        while True:
            stdscr.erase()
            stdscr.addstr(0, 0, "Choose migration scope (Up/Down + Enter)")
            stdscr.addstr(1, 0, "The chosen option will auto-include required dependencies.")
            row = 3
            for idx, option in enumerate(options):
                detail = ""
                dependencies = option.get("dependencies") or []
                if option.get("value") == "everything":
                    detail = "all entities"
                elif dependencies:
                    detail = "includes: " + ", ".join(str(dep) for dep in dependencies)
                line = str(option.get("label"))
                if detail:
                    line = f"{line} ({detail})"
                if idx == selected_index:
                    stdscr.addstr(row, 0, f"> {line}", curses.A_REVERSE)
                else:
                    stdscr.addstr(row, 0, f"  {line}")
                row += 1
            stdscr.addstr(row + 1, 0, "Press q to cancel.")
            stdscr.refresh()

            key = stdscr.getch()
            if key in (curses.KEY_UP, ord("k"), ord("K")):
                selected_index = (selected_index - 1) % len(options)
                continue
            if key in (curses.KEY_DOWN, ord("j"), ord("J")):
                selected_index = (selected_index + 1) % len(options)
                continue
            if key in (10, 13, curses.KEY_ENTER):
                return selected_index
            if key in (ord("q"), ord("Q"), 27):
                return None

    return curses.wrapper(_selector)


def _run_budget_selector_curses(
    role: str,
    budgets: List[Dict[str, str]],
) -> Optional[int]:
    if curses is None:
        return None

    def _selector(stdscr: Any) -> Optional[int]:
        selected_index = 0
        stdscr.keypad(True)
        try:
            curses.curs_set(0)
        except Exception:  # noqa: BLE001
            pass

        while True:
            stdscr.erase()
            height, width = stdscr.getmaxyx()
            max_width = max(1, width - 1)
            visible_count = max(1, height - 4)
            first_visible = min(
                max(0, selected_index - visible_count + 1),
                max(0, len(budgets) - visible_count),
            )
            last_visible = min(len(budgets), first_visible + visible_count)
            stdscr.addnstr(0, 0, f"Choose {role} budget (Up/Down + Enter)", max_width)
            row = 2
            for idx in range(first_visible, last_visible):
                budget = budgets[idx]
                line = f"{budget['name']} ({budget['id']})"
                if idx == selected_index:
                    stdscr.addnstr(row, 0, f"> {line}", max_width, curses.A_REVERSE)
                else:
                    stdscr.addnstr(row, 0, f"  {line}", max_width)
                row += 1
            footer = f"{selected_index + 1}/{len(budgets)} | Press q to cancel."
            stdscr.addnstr(min(height - 1, row + 1), 0, footer, max_width)
            stdscr.refresh()

            key = stdscr.getch()
            if key in (curses.KEY_UP, ord("k"), ord("K")):
                selected_index = (selected_index - 1) % len(budgets)
                continue
            if key in (curses.KEY_DOWN, ord("j"), ord("J")):
                selected_index = (selected_index + 1) % len(budgets)
                continue
            if key in (10, 13, curses.KEY_ENTER):
                return selected_index
            if key in (ord("q"), ord("Q"), 27):
                return None

    return curses.wrapper(_selector)


def _choose_budget(
    client: YNABClient,
    role: str,
    raw_plans: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, str]:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise RuntimeError("budget selection requires an interactive terminal")
    if curses is None:
        raise RuntimeError("budget selection requires curses support")

    if raw_plans is None:
        loaded_plans = client.get_plans().get("plans", [])
        raw_plans = loaded_plans if isinstance(loaded_plans, list) else []
    budgets_by_id: Dict[str, Dict[str, str]] = {}
    for raw_plan in raw_plans:
        if not isinstance(raw_plan, dict) or raw_plan.get("deleted") is True:
            continue
        budget_id = raw_plan.get("id")
        budget_name = raw_plan.get("name")
        if not isinstance(budget_id, str) or not isinstance(budget_name, str):
            continue
        budget_id = budget_id.strip()
        budget_name = " ".join(
            "".join(character if character.isprintable() else " " for character in budget_name).split()
        )
        if not budget_id or not budget_name:
            continue
        budgets_by_id[budget_id] = {"id": budget_id, "name": budget_name}
    budgets = sorted(
        budgets_by_id.values(),
        key=lambda budget: (budget["name"].casefold(), budget["id"]),
    )
    if not budgets:
        raise RuntimeError(f"the {role} token does not expose any selectable budgets")

    selected_index = _run_budget_selector_curses(role, budgets)
    if selected_index is None:
        raise RuntimeError(f"{role} budget selection cancelled")
    return budgets[selected_index]


def _confirm_budget_selection(
    command: str,
    source_budget: Dict[str, str],
    dest_budget: Dict[str, str],
) -> bool:
    print(f"Source budget: {source_budget['name']} ({source_budget['id']})")
    print(f"Destination budget: {dest_budget['name']} ({dest_budget['id']})")
    try:
        answer = input(
            f"Start {command} from {source_budget['name']!r} "
            f"to {dest_budget['name']!r}? [y/N] "
        )
    except EOFError:
        return False
    return answer.strip().casefold() in {"y", "yes"}


def _choose_apply_scope(logger: logging.Logger, as_json: bool) -> Dict[str, Any]:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        logger.info("Interactive apply scope prompt skipped (non-interactive terminal); defaulting to Everything.")
        return {
            "selection": "auto",
            "selected_entities": None,
            "apply_profile": None,
        }
    if curses is None:
        logger.info("Interactive apply scope prompt unavailable on this platform; defaulting to Everything.")
        return {
            "selection": "auto",
            "selected_entities": None,
            "apply_profile": None,
        }

    prompt_options = get_apply_entity_prompt_options()
    label_by_value = {
        str(option.get("value")): str(option.get("label", option.get("value")))
        for option in prompt_options
        if option.get("value")
    }
    options: List[Dict[str, Any]] = []
    for option in prompt_options:
        value = str(option.get("value", "")).strip()
        if not value:
            continue
        label = str(option.get("label", value))
        selected_entities_raw = option.get("selected_entities")
        selected_entities = (
            [str(item) for item in selected_entities_raw]
            if isinstance(selected_entities_raw, list) and selected_entities_raw
            else [value]
        )
        apply_profile = str(option.get("apply_profile", APPLY_PROFILE_DEFAULT))
        dependencies_raw = option.get("dependencies")
        dependency_labels = [
            str(label_by_value.get(str(dep), dep))
            for dep in (dependencies_raw if isinstance(dependencies_raw, list) else [])
        ]
        options.append(
            {
                "value": value,
                "label": label,
                "dependencies": dependency_labels,
                "apply_profile": apply_profile,
                "selected_entities": selected_entities,
            }
        )
    options.append(
        {
            "value": "everything",
            "label": "Everything",
            "dependencies": [],
            "apply_profile": APPLY_PROFILE_DEFAULT,
            "selected_entities": ["everything"],
        }
    )

    selected_index = _run_apply_selector_curses(options)
    if selected_index is None:
        raise RuntimeError("apply cancelled by user before execution")

    selected_option = options[selected_index]
    selection = str(selected_option["value"])
    apply_profile = str(selected_option.get("apply_profile", APPLY_PROFILE_DEFAULT))
    selected_entities_raw = selected_option.get("selected_entities", ["everything"])
    if isinstance(selected_entities_raw, list):
        selected_entities = [str(item) for item in selected_entities_raw]
    else:
        selected_entities = [str(selected_entities_raw)]
    if apply_profile == APPLY_PROFILE_DEFAULT:
        effective_entities = resolve_apply_entities(selected_entities)
    else:
        effective_entities = selected_entities
    print(
        "apply scope: "
        + str(selected_option["label"])
        + " -> "
        + ", ".join(effective_entities),
        file=sys.stderr if as_json else sys.stdout,
    )
    logger.info(
        "Apply scope selected: %s (profile: %s, effective entities: %s)",
        selection,
        apply_profile,
        ",".join(effective_entities),
    )
    return {
        "selection": selection,
        "selected_entities": selected_entities,
        "apply_profile": apply_profile,
    }


def _emit(report: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True))
        return

    mode = report.get("mode", "unknown")
    print(f"{mode}: completed")

    if mode == "plan":
        stats = report.get("stats", {})
        counts = stats.get("source_counts_after_deleted_filter", {})
        print(f"source transactions (after deleted filter): {counts.get('transactions', 0)}")
        print(f"source scheduled (after deleted filter): {counts.get('scheduled_transactions', 0)}")
        print(f"month budget entries: {counts.get('month_category_budgets', 0)}")
        estimate = stats.get("estimated_apply_requests", {})
        print(f"estimated requests: {estimate.get('total_estimated', 0)}")
        print(f"estimated hours at 200 req/hour: {estimate.get('hours_at_200_req_per_hour', 0)}")
        if report.get("budget_selection"):
            print(f"saved budget selection: {report['budget_selection']}")
        manual_items = report.get("manual_action_items", [])
        required_count = sum(1 for item in manual_items if item.get("severity") == "required")
        recommended_count = sum(1 for item in manual_items if item.get("severity") == "recommended")
        print(f"manual action items: {len(manual_items)} (required: {required_count}, recommended: {recommended_count})")
        for item in manual_items:
            severity = str(item.get("severity") or "info").upper()
            action = item.get("action") or item.get("title") or "Review plan_report.json manual_action_items"
            print(f"{severity}: {action}")
    elif mode == "apply":
        mapping_counts = report.get("mapping_counts", {})
        apply_entities = report.get("apply_entities", [])
        apply_profile = report.get("apply_profile")
        if apply_profile:
            print(f"apply profile: {apply_profile}")
        if isinstance(apply_entities, list) and apply_entities:
            print(f"apply entities: {', '.join(str(item) for item in apply_entities)}")
        print(f"mapped transactions: {mapping_counts.get('transactions', 0)}")
        print(f"mapped scheduled transactions: {mapping_counts.get('scheduled_transactions', 0)}")
        print(f"errors: {len(report.get('errors', []))}")
        print(f"warnings: {len(report.get('warnings', []))}")
        print(f"checkpoint: {report.get('checkpoint')}")
    elif mode == "verify":
        print(f"passed: {report.get('passed')}")
        print(f"mismatches: {report.get('mismatch_count', 0)}")
    elif mode == "doctor":
        print(f"safe to resume: {report.get('safe_to_resume')}")
        print(f"recoverable transaction mappings: {len(report.get('recoverable_transaction_mappings', []))}")
        print(f"stale transaction mappings: {len(report.get('stale_transaction_mappings', []))}")
        print(f"mapping collisions: {len(report.get('transaction_mapping_collisions', []))}")


def main(argv: Any = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        args = _load_runtime_config(args)
    except Exception as error:  # noqa: BLE001
        print(f"error: {error}", file=sys.stderr)
        return 2
    workdir = Path(args.workdir).resolve()
    logger = logging.getLogger("ynab_migrator.startup")
    command_started = False

    try:
        source_client, dest_client = _build_clients(args, logger=logger)
        if args.command == "plan":
            if args.source_token == args.dest_token:
                shared_plans_raw = source_client.get_plans().get("plans", [])
                shared_plans = shared_plans_raw if isinstance(shared_plans_raw, list) else []
                source_budget = _choose_budget(source_client, "source", raw_plans=shared_plans)
                dest_budget = _choose_budget(dest_client, "destination", raw_plans=shared_plans)
            else:
                source_budget = _choose_budget(source_client, "source")
                dest_budget = _choose_budget(dest_client, "destination")
        else:
            source_budget, dest_budget = _load_budget_selection(
                workdir=workdir,
                source_token=args.source_token,
                dest_token=args.dest_token,
            )
        if source_budget["id"] == dest_budget["id"]:
            raise RuntimeError("source and destination budgets must be different")
        args.source_plan_id = source_budget["id"]
        args.dest_plan_id = dest_budget["id"]

        scope: Dict[str, Any] = {}
        if args.command == "apply":
            scope = _choose_apply_scope(logger=logger.getChild("cli"), as_json=bool(args.json))

        if not _confirm_budget_selection(args.command, source_budget, dest_budget):
            raise RuntimeError("command cancelled before execution")

        logger = build_runtime_logger(
            workdir=workdir,
            command=args.command,
            verbose=bool(args.verbose),
        )
        command_started = True
        source_client.logger = logger.getChild("client.source")
        dest_client.logger = logger.getChild("client.dest")
        engine = _build_engine(
            args,
            logger=logger,
            source_client=source_client,
            dest_client=dest_client,
        )
        logger.info(
            "Starting %s (source=%s, destination=%s). Log file: %s",
            args.command,
            args.source_plan_id,
            args.dest_plan_id,
            command_log_path(workdir, args.command),
        )
        if args.command == "plan":
            report = engine.plan()
            selection_path = _save_budget_selection(
                workdir=workdir,
                source_budget=source_budget,
                dest_budget=dest_budget,
                source_token=args.source_token,
                dest_token=args.dest_token,
            )
            report["budget_selection"] = str(selection_path)
            logger.info("Saved budget selection to %s", selection_path)
        elif args.command == "apply":
            report = engine.apply(
                selected_entities=scope.get("selected_entities"),
                apply_profile=scope.get("apply_profile"),
            )
        elif args.command == "verify":
            report = engine.verify()
        elif args.command == "resume":
            report = engine.resume()
        elif args.command == "doctor":
            report = engine.doctor()
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2
    except KeyboardInterrupt:
        if command_started:
            logger.warning(
                "Command %s interrupted by user; completed work remains checkpointed",
                args.command,
            )
        print(
            "\nInterrupted safely. Completed work remains checkpointed; "
            "run `ynab-migrator resume` when ready.",
            file=sys.stderr,
        )
        return 130
    except Exception as error:  # noqa: BLE001
        if command_started:
            if args.verbose:
                logger.exception("Command %s failed", args.command)
            else:
                logger.error("Command %s failed: %s", args.command, error)
        if args.json:
            print(
                json.dumps(
                    {
                        "error": error.__class__.__name__,
                        "message": str(error),
                    },
                    indent=2,
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
        else:
            print(f"error: {error}", file=sys.stderr)
        return 1

    logger.info("Finished %s", report.get("mode", "unknown"))
    _emit(report, as_json=args.json)
    if report.get("mode") == "apply" and report.get("complete") is False:
        return 1
    if report.get("mode") == "verify" and report.get("passed") is False:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
