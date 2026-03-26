from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

from .client import RetryConfig, YNABClient
from .migration import MigrationEngine
from .runtime_logging import build_runtime_logger, command_log_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ynab-migrator",
        description=(
            "Resumable YNAB plan migration CLI. "
            "Tombstoned/deleted entities are always ignored and never replayed."
        ),
    )

    parser.add_argument("--source-token", required=True, help="Source YNAB API token (read access)")
    parser.add_argument("--dest-token", required=True, help="Destination YNAB API token (write access)")
    parser.add_argument("--source-plan-id", required=True, help="Source YNAB plan ID")
    parser.add_argument("--dest-plan-id", required=True, help="Destination YNAB plan ID")
    parser.add_argument(
        "--workdir",
        default="./.ynab_migrator",
        help="Directory to store snapshot, checkpoint, and reports",
    )
    parser.add_argument("--tx-batch-size", type=int, default=50, help="Transaction create batch size")
    parser.add_argument(
        "--rate-limit-per-hour",
        type=int,
        default=200,
        help="Client-side throttle limit per token per rolling hour",
    )
    parser.add_argument("--max-retries", type=int, default=8, help="Max retry attempts for transient failures")
    parser.add_argument(
        "--base-retry-delay",
        type=float,
        default=1.0,
        help="Base retry delay in seconds for exponential backoff",
    )
    parser.add_argument(
        "--max-retry-delay",
        type=float,
        default=120.0,
        help="Max retry delay in seconds for exponential backoff",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print report as pretty JSON",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed technical runtime logs (API request telemetry and retry details)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("plan", help="Build immutable snapshot and preflight report")
    subparsers.add_parser("apply", help="Apply migration using snapshot and checkpoint")
    subparsers.add_parser("verify", help="Verify migrated subset parity")
    subparsers.add_parser("resume", help="Alias for apply; continue from checkpoint")
    return parser


def _build_engine(args: argparse.Namespace, logger: logging.Logger) -> MigrationEngine:
    retry = RetryConfig(
        max_retries=max(0, int(args.max_retries)),
        base_delay_seconds=max(0.1, float(args.base_retry_delay)),
        max_delay_seconds=max(1.0, float(args.max_retry_delay)),
    )
    workdir = Path(args.workdir).resolve()
    source_client = YNABClient(
        token=args.source_token,
        rate_limit_per_hour=max(1, int(args.rate_limit_per_hour)),
        retry_config=retry,
        logger=logger.getChild("client.source"),
    )
    dest_client = YNABClient(
        token=args.dest_token,
        rate_limit_per_hour=max(1, int(args.rate_limit_per_hour)),
        retry_config=retry,
        logger=logger.getChild("client.dest"),
    )
    return MigrationEngine(
        source_client=source_client,
        dest_client=dest_client,
        source_plan_id=args.source_plan_id,
        dest_plan_id=args.dest_plan_id,
        workdir=workdir,
        tx_batch_size=max(1, int(args.tx_batch_size)),
        logger=logger.getChild("migration"),
    )


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
        print(f"mapped transactions: {mapping_counts.get('transactions', 0)}")
        print(f"mapped scheduled transactions: {mapping_counts.get('scheduled_transactions', 0)}")
        print(f"errors: {len(report.get('errors', []))}")
        print(f"warnings: {len(report.get('warnings', []))}")
        print(f"checkpoint: {report.get('checkpoint')}")
    elif mode == "verify":
        print(f"passed: {report.get('passed')}")
        print(f"mismatches: {report.get('mismatch_count', 0)}")


def main(argv: Any = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    workdir = Path(args.workdir).resolve()
    logger = build_runtime_logger(workdir=workdir, command=args.command, verbose=bool(args.verbose))
    logger.info(
        "Starting %s (source=%s, destination=%s). Log file: %s",
        args.command,
        args.source_plan_id,
        args.dest_plan_id,
        command_log_path(workdir, args.command),
    )
    engine = _build_engine(args, logger=logger)

    try:
        if args.command == "plan":
            report = engine.plan()
        elif args.command == "apply":
            report = engine.apply()
        elif args.command == "verify":
            report = engine.verify()
        elif args.command == "resume":
            report = engine.resume()
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2
    except Exception as error:  # noqa: BLE001
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
