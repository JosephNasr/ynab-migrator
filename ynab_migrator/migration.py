from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .checkpoint import CheckpointStore
from .client import YNABApiError, YNABClient
from .utils import (
    atomic_write_json,
    canonical_json,
    clean_deleted,
    deterministic_import_id,
    ensure_dir,
    is_deleted,
    normalize_name,
    now_utc_iso,
    read_json,
    stable_hash,
    summarize_exception,
)


SNAPSHOT_FILE = "snapshot.json"
PLAN_REPORT_FILE = "plan_report.json"
APPLY_REPORT_FILE = "apply_report.json"
VERIFY_REPORT_FILE = "verify_report.json"
CHECKPOINT_FILE = "checkpoint.sqlite3"
SNAPSHOT_SCHEMA_VERSION = 3

INTERNAL_MASTER_CATEGORY_GROUP_NAME = "Internal Master Category"
HIDDEN_CATEGORIES_GROUP_NAME = "Hidden Categories"
CREDIT_CARD_PAYMENTS_GROUP_NAME = "Credit Card Payments"
INTERNAL_INFLOW_CATEGORY_NAME = "Inflow: Ready to Assign"
INTERNAL_UNCATEGORIZED_CATEGORY_NAME = "Uncategorized"
STARTING_BALANCE_PAYEE_NAME = "Starting Balance"
SUPPORTED_ACCOUNT_CREATE_TYPES = {"checking", "savings", "cash", "creditCard"}
ALLOWED_FLAG_COLORS = {"red", "orange", "yellow", "green", "blue", "purple"}
APPLY_ENTITY_EXECUTION_ORDER: Tuple[str, ...] = (
    "accounts",
    "category_groups",
    "categories",
    "payees",
    "transactions",
    "scheduled_transactions",
    "month_budgets",
)
APPLY_ENTITY_LABELS: Dict[str, str] = {
    "accounts": "Accounts",
    "category_groups": "Category Groups",
    "categories": "Categories",
    "payees": "Payees",
    "transactions": "Transactions",
    "scheduled_transactions": "Scheduled Transactions",
    "month_budgets": "Month Budgets",
}
APPLY_ENTITY_DEPENDENCIES: Dict[str, Set[str]] = {
    "accounts": set(),
    "category_groups": set(),
    "categories": {"accounts", "category_groups"},
    "payees": set(),
    "transactions": {"accounts", "categories", "payees"},
    "scheduled_transactions": {"accounts", "categories", "payees"},
    "month_budgets": {"categories"},
}


def _apply_entity_closure(entity: str, visited: Optional[Set[str]] = None) -> Set[str]:
    if entity not in APPLY_ENTITY_DEPENDENCIES:
        raise ValueError(
            "unknown apply entity scope "
            f"'{entity}'. Supported values: {', '.join(APPLY_ENTITY_EXECUTION_ORDER)}"
        )
    if visited is None:
        visited = set()
    if entity in visited:
        return visited
    visited.add(entity)
    for dependency in APPLY_ENTITY_DEPENDENCIES[entity]:
        _apply_entity_closure(dependency, visited)
    return visited


def resolve_apply_entities(selected_entities: Optional[Iterable[str]]) -> List[str]:
    if selected_entities is None:
        return list(APPLY_ENTITY_EXECUTION_ORDER)

    normalized: List[str] = []
    for raw in selected_entities:
        if raw is None:
            continue
        token = str(raw).strip().lower()
        if not token:
            continue
        if token in {"everything", "all", "*"}:
            return list(APPLY_ENTITY_EXECUTION_ORDER)
        normalized.append(token)

    if not normalized:
        return list(APPLY_ENTITY_EXECUTION_ORDER)

    scoped: Set[str] = set()
    for token in normalized:
        scoped.update(_apply_entity_closure(token))

    return [entity for entity in APPLY_ENTITY_EXECUTION_ORDER if entity in scoped]


def get_apply_entity_prompt_options() -> List[Dict[str, Any]]:
    execution_index = {name: index for index, name in enumerate(APPLY_ENTITY_EXECUTION_ORDER)}
    entities_sorted = sorted(
        APPLY_ENTITY_EXECUTION_ORDER,
        key=lambda name: (
            max(0, len(_apply_entity_closure(name)) - 1),
            execution_index[name],
        ),
    )

    options: List[Dict[str, Any]] = []
    for entity in entities_sorted:
        dependencies = [
            dependency
            for dependency in APPLY_ENTITY_EXECUTION_ORDER
            if dependency in _apply_entity_closure(entity) and dependency != entity
        ]
        options.append(
            {
                "entity": entity,
                "label": APPLY_ENTITY_LABELS.get(entity, entity),
                "dependencies": dependencies,
            }
        )
    return options


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _canonical_entity_projection(entity: Dict[str, Any], fields: Iterable[str]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for field in fields:
        output[field] = entity.get(field)
    return output


def _is_supported_account_create_type(source_type: Any) -> bool:
    return isinstance(source_type, str) and source_type in SUPPORTED_ACCOUNT_CREATE_TYPES


def _coerce_account_type_for_create(source_type: Any) -> Tuple[str, bool]:
    if _is_supported_account_create_type(source_type):
        return source_type, False
    return "cash", True


def _normalize_flag_color(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized in ALLOWED_FLAG_COLORS:
        return normalized
    return None


@dataclass
class MigrationPaths:
    workdir: Path
    snapshot_path: Path
    checkpoint_path: Path
    plan_report_path: Path
    apply_report_path: Path
    verify_report_path: Path

    @classmethod
    def from_workdir(cls, workdir: Path) -> "MigrationPaths":
        return cls(
            workdir=workdir,
            snapshot_path=workdir / SNAPSHOT_FILE,
            checkpoint_path=workdir / CHECKPOINT_FILE,
            plan_report_path=workdir / PLAN_REPORT_FILE,
            apply_report_path=workdir / APPLY_REPORT_FILE,
            verify_report_path=workdir / VERIFY_REPORT_FILE,
        )


class MigrationEngine:
    def __init__(
        self,
        source_client: YNABClient,
        dest_client: YNABClient,
        source_plan_id: str,
        dest_plan_id: str,
        workdir: Path,
        tx_batch_size: int = 50,
        logger: Optional[logging.Logger] = None,
    ):
        self.source_client = source_client
        self.dest_client = dest_client
        self.source_plan_id = source_plan_id
        self.dest_plan_id = dest_plan_id
        self.tx_batch_size = max(1, tx_batch_size)
        self.paths = MigrationPaths.from_workdir(workdir)
        self.logger = logger or logging.getLogger("ynab_migrator.migration")
        ensure_dir(self.paths.workdir)

    def _log_stage(self, mode: str, stage: str, event: str, **fields: Any) -> None:
        pretty_mode = mode.strip().upper()
        pretty_stage = stage.replace("_", " ").strip()

        if event == "start":
            base = f"{pretty_mode} | {pretty_stage}: started"
        elif event == "complete":
            base = f"{pretty_mode} | {pretty_stage}: completed"
        elif event == "progress":
            current = fields.pop("current", None)
            total = fields.pop("total", None)
            if current is not None and total is not None:
                base = f"{pretty_mode} | {pretty_stage}: {current}/{total}"
            else:
                base = f"{pretty_mode} | {pretty_stage}: progress"
        else:
            base = f"{pretty_mode} | {pretty_stage}: {event}"

        field_items = [f"{key}={value}" for key, value in fields.items() if value is not None]
        if field_items:
            base = f"{base} ({', '.join(field_items)})"
        self.logger.info(base)

    def _log_progress(self, mode: str, stage: str, current: int, total: int) -> None:
        if total <= 0:
            return
        if current == 1 or current == total or current % 25 == 0:
            self._log_stage(mode, stage, "progress", current=current, total=total)

    # ---------- Public commands ----------
    def plan(self) -> Dict[str, Any]:
        self._log_stage(
            "plan",
            "command",
            "start",
            source_plan_id=self.source_plan_id,
            dest_plan_id=self.dest_plan_id,
            workdir=self.paths.workdir,
        )
        report: Dict[str, Any] = {
            "mode": "plan",
            "generated_at": now_utc_iso(),
            "source_plan_id": self.source_plan_id,
            "dest_plan_id": self.dest_plan_id,
            "warnings": [],
            "unsupported": [],
        }

        self._log_stage("plan", "fetch_source_plan", "start")
        source_payload = self.source_client.get_plan(self.source_plan_id)
        source_plan = source_payload.get("plan", {})
        self._log_stage("plan", "fetch_source_plan", "complete")

        self._log_stage("plan", "fetch_source_settings", "start")
        source_settings = self.source_client.get_plan_settings(self.source_plan_id).get("settings", {})
        self._log_stage("plan", "fetch_source_settings", "complete")

        self._log_stage("plan", "fetch_source_months", "start")
        months_payload = self.source_client.get_plan_months(self.source_plan_id)
        month_summaries = clean_deleted(months_payload.get("months", []))
        self._log_stage("plan", "fetch_source_months", "complete", months=len(month_summaries))

        # Enforce tombstone filtering at extraction time.
        self._log_stage("plan", "extract_entities", "start")
        accounts = clean_deleted(source_plan.get("accounts", []))
        category_groups = clean_deleted(source_plan.get("category_groups", []))
        categories = clean_deleted(source_plan.get("categories", []))
        payees = clean_deleted(source_plan.get("payees", []))
        transactions = self._extract_transactions(source_plan)
        scheduled_transactions = self._extract_scheduled_transactions(source_plan, report)
        month_category_budgets, month_notes = self._extract_month_budgets(month_summaries)
        system_entities = self._build_source_system_entities(category_groups, categories, report)
        non_migratable_fields = self._extract_non_migratable_fields(
            source_accounts=accounts,
            month_notes=month_notes,
            report=report,
        )
        self._log_stage(
            "plan",
            "extract_entities",
            "complete",
            accounts=len(accounts),
            category_groups=len(category_groups),
            categories=len(categories),
            payees=len(payees),
            transactions=len(transactions),
            scheduled_transactions=len(scheduled_transactions),
            month_category_budgets=len(month_category_budgets),
        )

        self._log_stage("plan", "fetch_destination_snapshot", "start")
        destination_snapshot = self.dest_client.get_plan(self.dest_plan_id).get("plan", {})
        destination_accounts = clean_deleted(destination_snapshot.get("accounts", []))
        destination_counts = {
            "accounts": len(destination_accounts),
            "category_groups": len(clean_deleted(destination_snapshot.get("category_groups", []))),
            "categories": len(clean_deleted(destination_snapshot.get("categories", []))),
            "payees": len(clean_deleted(destination_snapshot.get("payees", []))),
            "transactions": len(clean_deleted(destination_snapshot.get("transactions", []))),
            "scheduled_transactions": len(
                clean_deleted(destination_snapshot.get("scheduled_transactions", []))
            ),
        }
        self._log_stage("plan", "fetch_destination_snapshot", "complete")

        self._log_stage("plan", "build_manual_action_items", "start")
        manual_action_items = self._build_plan_manual_action_items(
            source_accounts=accounts,
            destination_accounts=destination_accounts,
            source_scheduled_transactions=scheduled_transactions,
            non_migratable_fields=non_migratable_fields,
        )
        manual_action_required_count = sum(
            1 for item in manual_action_items if item.get("severity") == "required"
        )
        manual_action_recommended_count = sum(
            1 for item in manual_action_items if item.get("severity") == "recommended"
        )
        if manual_action_required_count:
            self._append_warning(
                report,
                (
                    f"{manual_action_required_count} required manual pre-apply action items detected; "
                    "review plan_report.json -> manual_action_items before running apply."
                ),
            )
        self._log_stage(
            "plan",
            "build_manual_action_items",
            "complete",
            total=len(manual_action_items),
            required=manual_action_required_count,
            recommended=manual_action_recommended_count,
        )

        snapshot: Dict[str, Any] = {
            "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
            "generated_at": now_utc_iso(),
            "source_plan_id": self.source_plan_id,
            "dest_plan_id": self.dest_plan_id,
            "source_plan_name": source_plan.get("name"),
            "source_settings": source_settings,
            "system_entities": system_entities,
            "non_migratable_fields": non_migratable_fields,
            "entities": {
                "accounts": accounts,
                "category_groups": category_groups,
                "categories": categories,
                "payees": payees,
                "transactions": transactions,
                "scheduled_transactions": scheduled_transactions,
                "months": month_summaries,
                "month_category_budgets": month_category_budgets,
            },
            "integrity": {
                "source_hash": stable_hash(
                    {
                        "accounts": accounts,
                        "category_groups": category_groups,
                        "categories": categories,
                        "payees": payees,
                        "transactions": transactions,
                        "scheduled_transactions": scheduled_transactions,
                        "month_category_budgets": month_category_budgets,
                        "system_entities": system_entities,
                        "non_migratable_fields": non_migratable_fields,
                        "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
                    }
                ),
                "extraction_filters": {
                    "deleted_records_ignored": True,
                },
            },
            "unsupported_defaults": [
                "payee_locations are read-only in API",
                "money_movements are read-only in API",
                "account metadata beyond name/type/balance is not writable",
                "account notes are read-only in API",
                "month notes are read-only in API",
                "category metadata beyond name/note/group/goal_target/goal_target_date is not writable",
            ],
        }

        report["stats"] = {
            "source_counts_after_deleted_filter": {
                "accounts": len(accounts),
                "category_groups": len(category_groups),
                "categories": len(categories),
                "payees": len(payees),
                "transactions": len(transactions),
                "scheduled_transactions": len(scheduled_transactions),
                "months": len(month_summaries),
                "month_category_budgets": len(month_category_budgets),
            },
            "system_entities": {
                "source_internal_master_category_group_ids": len(
                    system_entities["source_internal_master_category_group_ids"]
                ),
                "source_hidden_category_group_ids": len(
                    system_entities["source_hidden_category_group_ids"]
                ),
                "source_credit_card_payments_group_ids": len(
                    system_entities["source_credit_card_payments_group_ids"]
                ),
                "source_internal_inflow_category_ids": len(
                    system_entities["source_internal_inflow_category_ids"]
                ),
                "source_internal_uncategorized_category_ids": len(
                    system_entities["source_internal_uncategorized_category_ids"]
                ),
                "source_credit_card_payment_category_ids": len(
                    system_entities["source_credit_card_payment_category_ids"]
                ),
            },
            "non_migratable_fields": {
                "account_notes": len(non_migratable_fields.get("account_notes", [])),
                "month_notes": len(non_migratable_fields.get("month_notes", [])),
            },
            "manual_action_items": {
                "total": len(manual_action_items),
                "required": manual_action_required_count,
                "recommended": manual_action_recommended_count,
            },
            "destination_current_counts": destination_counts,
            "estimated_apply_requests": self._estimate_apply_requests(snapshot),
            "tx_batch_size": self.tx_batch_size,
        }
        report["manual_action_items"] = manual_action_items
        report["unsupported"].extend(snapshot["unsupported_defaults"])

        self._log_stage(
            "plan",
            "write_artifacts",
            "start",
            snapshot_path=self.paths.snapshot_path,
            plan_report_path=self.paths.plan_report_path,
        )
        atomic_write_json(self.paths.snapshot_path, snapshot)
        atomic_write_json(self.paths.plan_report_path, report)
        self._log_stage(
            "plan",
            "command",
            "complete",
            warnings=len(report.get("warnings", [])),
            unsupported=len(report.get("unsupported", [])),
            manual_action_items=len(manual_action_items),
        )
        return report

    def apply(self, selected_entities: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        requested_apply_entities = resolve_apply_entities(selected_entities)
        self._log_stage(
            "apply",
            "command",
            "start",
            source_plan_id=self.source_plan_id,
            dest_plan_id=self.dest_plan_id,
            workdir=self.paths.workdir,
            requested_entities=",".join(requested_apply_entities),
        )
        snapshot = self._load_snapshot()
        checkpoint = CheckpointStore(self.paths.checkpoint_path)
        try:
            if checkpoint.get_metadata("source_plan_id") is None:
                checkpoint.set_metadata("source_plan_id", self.source_plan_id)
                checkpoint.set_metadata("dest_plan_id", self.dest_plan_id)
                checkpoint.set_metadata("snapshot_hash", snapshot["integrity"]["source_hash"])
                checkpoint.set_metadata("snapshot_schema_version", SNAPSHOT_SCHEMA_VERSION)
                checkpoint.set_metadata("apply_reference_date", _today_utc().isoformat())
                checkpoint.add_event("INFO", "initialized checkpoint")
            else:
                source_plan_id = checkpoint.get_metadata("source_plan_id")
                dest_plan_id = checkpoint.get_metadata("dest_plan_id")
                if source_plan_id != self.source_plan_id or dest_plan_id != self.dest_plan_id:
                    raise RuntimeError("checkpoint plan IDs do not match current command arguments")
                snapshot_hash = checkpoint.get_metadata("snapshot_hash")
                if snapshot_hash != snapshot["integrity"]["source_hash"]:
                    raise RuntimeError(
                        "snapshot hash differs from checkpoint metadata; rerun with a clean workdir"
                    )
                snapshot_schema_version = checkpoint.get_metadata("snapshot_schema_version")
                if snapshot_schema_version != SNAPSHOT_SCHEMA_VERSION:
                    raise RuntimeError(
                        "checkpoint schema version is incompatible; rerun from a clean workdir"
                    )

            existing_apply_entities_raw = checkpoint.get_metadata("apply_entities")
            existing_apply_entities: List[str] = []
            if isinstance(existing_apply_entities_raw, list) and existing_apply_entities_raw:
                existing_apply_entities = resolve_apply_entities(existing_apply_entities_raw)
            elif existing_apply_entities_raw not in (None, [], ()):
                raise RuntimeError("checkpoint apply_entities metadata is invalid; rerun from a clean workdir")

            if existing_apply_entities:
                if selected_entities is None:
                    effective_apply_entities = existing_apply_entities
                else:
                    requested_set = set(requested_apply_entities)
                    existing_set = set(existing_apply_entities)
                    if requested_set == existing_set:
                        effective_apply_entities = existing_apply_entities
                    elif requested_set.issuperset(existing_set):
                        effective_apply_entities = [
                            entity
                            for entity in APPLY_ENTITY_EXECUTION_ORDER
                            if entity in requested_set
                        ]
                        checkpoint.set_metadata("apply_entities", effective_apply_entities)
                        checkpoint.add_event(
                            "INFO",
                            "expanded apply_entities scope to " + ",".join(effective_apply_entities),
                        )
                    else:
                        raise RuntimeError(
                            "requested apply entity scope conflicts with checkpoint scope; "
                            "use `resume` to continue checkpoint scope or rerun with a clean workdir"
                        )
            else:
                effective_apply_entities = requested_apply_entities
                checkpoint.set_metadata("apply_entities", effective_apply_entities)

            effective_apply_entity_set = set(effective_apply_entities)
            self._log_stage(
                "apply",
                "entity_scope",
                "complete",
                entities=",".join(effective_apply_entities),
            )

            source_system_entities = snapshot["system_entities"]
            non_migratable_fields = snapshot.get("non_migratable_fields", {})
            account_note_count = len(non_migratable_fields.get("account_notes", []))
            month_note_count = len(non_migratable_fields.get("month_notes", []))
            report: Dict[str, Any] = {
                "mode": "apply",
                "generated_at": now_utc_iso(),
                "source_plan_id": self.source_plan_id,
                "dest_plan_id": self.dest_plan_id,
                "warnings": [],
                "errors": [],
                "apply_entities": effective_apply_entities,
                "exclusions": checkpoint.get_metadata("exclusions", []),
                "non_migratable_fields": non_migratable_fields,
                "system_entity_stats": {
                    "source_internal_master_category_group_ids": len(
                        source_system_entities["source_internal_master_category_group_ids"]
                    ),
                    "source_hidden_category_group_ids": len(
                        source_system_entities["source_hidden_category_group_ids"]
                    ),
                    "source_credit_card_payments_group_ids": len(
                        source_system_entities["source_credit_card_payments_group_ids"]
                    ),
                    "source_internal_inflow_category_ids": len(
                        source_system_entities["source_internal_inflow_category_ids"]
                    ),
                    "source_internal_uncategorized_category_ids": len(
                        source_system_entities["source_internal_uncategorized_category_ids"]
                    ),
                    "source_credit_card_payment_category_ids": len(
                        source_system_entities["source_credit_card_payment_category_ids"]
                    ),
                    "mapped_system_category_group_ids": 0,
                    "mapped_system_category_ids": 0,
                    "mapped_internal_category_group_ids": 0,
                    "mapped_hidden_category_group_ids": 0,
                    "mapped_credit_card_payments_group_ids": 0,
                    "mapped_internal_inflow_category_ids": 0,
                    "mapped_internal_uncategorized_category_ids": 0,
                    "mapped_credit_card_payment_category_ids": 0,
                    "skipped_system_category_group_creates": 0,
                    "skipped_system_category_creates": 0,
                    "skipped_internal_month_budget_patches": 0,
                    "skipped_missing_month_budget_targets": 0,
                    "captured_auto_starting_balance_candidates": 0,
                    "deleted_auto_starting_balance_transactions": 0,
                    "starting_balance_fallback_no_candidate": 0,
                    "starting_balance_delete_failures": 0,
                    "coerced_account_types": 0,
                    "reused_accounts_by_name_type": 0,
                    "reused_unsupported_accounts_by_name_type": 0,
                    "ambiguous_account_name_type_matches": 0,
                    "ambiguous_unsupported_account_name_type_matches": 0,
                    "reused_category_groups_by_name": 0,
                    "ambiguous_category_group_name_matches": 0,
                    "reused_categories_by_name_group": 0,
                    "ambiguous_category_name_group_matches": 0,
                    "reused_payees_by_exact_name": 0,
                    "ambiguous_payee_exact_name_matches": 0,
                },
            }
            if account_note_count:
                self._append_warning(
                    report,
                    f"{account_note_count} account notes are read-only in YNAB API and were recorded as non-migratable."
                )
            if month_note_count:
                self._append_warning(
                    report,
                    f"{month_note_count} month notes are read-only in YNAB API and were recorded as non-migratable."
                )

            source_entities = snapshot["entities"]
            source_accounts = source_entities["accounts"]
            source_category_groups = source_entities["category_groups"]
            source_categories = source_entities["categories"]
            source_payees = source_entities["payees"]
            source_transactions = source_entities["transactions"]
            source_scheduled_transactions = source_entities["scheduled_transactions"]
            month_category_budgets = source_entities["month_category_budgets"]

            source_accounts_by_id = {account["id"]: account for account in source_accounts}
            source_payees_by_id = {payee["id"]: payee for payee in source_payees}
            source_system_group_ids = set(
                source_system_entities["source_internal_master_category_group_ids"]
            )
            source_system_group_ids.update(source_system_entities["source_hidden_category_group_ids"])
            source_system_group_ids.update(source_system_entities["source_credit_card_payments_group_ids"])
            source_internal_category_ids = set(source_system_entities["source_internal_inflow_category_ids"])
            source_internal_category_ids.update(
                source_system_entities["source_internal_uncategorized_category_ids"]
            )
            source_system_category_ids = set(source_internal_category_ids)
            source_system_category_ids.update(
                source_system_entities["source_credit_card_payment_category_ids"]
            )

            needs_system_group_mapping = bool(
                effective_apply_entity_set.intersection(
                    {
                        "category_groups",
                        "categories",
                        "transactions",
                        "scheduled_transactions",
                        "month_budgets",
                    }
                )
            )
            needs_system_category_mapping = bool(
                effective_apply_entity_set.intersection(
                    {
                        "categories",
                        "transactions",
                        "scheduled_transactions",
                        "month_budgets",
                    }
                )
            )

            if needs_system_group_mapping:
                self._log_stage("apply", "resolve_system_entities_pre_accounts", "start")
                self._resolve_and_map_destination_system_entities(
                    source_system_entities=source_system_entities,
                    checkpoint=checkpoint,
                    report=report,
                    map_credit_card_payment_categories=False,
                )
                self._log_stage("apply", "resolve_system_entities_pre_accounts", "complete")

            if "accounts" in effective_apply_entity_set:
                self._log_stage("apply", "accounts", "start", total=len(source_accounts))
                self._create_accounts(source_accounts, checkpoint, report)
                self._log_stage("apply", "accounts", "complete")

            if needs_system_category_mapping:
                self._log_stage("apply", "resolve_system_entities_post_accounts", "start")
                self._resolve_and_map_destination_system_entities(
                    source_system_entities=source_system_entities,
                    checkpoint=checkpoint,
                    report=report,
                    map_credit_card_payment_categories=True,
                )
                self._log_stage("apply", "resolve_system_entities_post_accounts", "complete")

            if "transactions" in effective_apply_entity_set:
                self._log_stage("apply", "capture_auto_starting_balance", "start")
                self._capture_destination_auto_starting_balance_candidates(
                    source_accounts=source_accounts,
                    checkpoint=checkpoint,
                    report=report,
                )
                self._log_stage("apply", "capture_auto_starting_balance", "complete")

            if "category_groups" in effective_apply_entity_set:
                self._log_stage("apply", "category_groups", "start", total=len(source_category_groups))
                self._create_category_groups(
                    source_groups=source_category_groups,
                    checkpoint=checkpoint,
                    report=report,
                    source_system_group_ids=source_system_group_ids,
                )
                self._log_stage("apply", "category_groups", "complete")

            if "categories" in effective_apply_entity_set:
                self._log_stage("apply", "categories", "start", total=len(source_categories))
                self._create_categories(
                    source_categories=source_categories,
                    checkpoint=checkpoint,
                    report=report,
                    source_system_category_ids=source_system_category_ids,
                )
                self._log_stage("apply", "categories", "complete")

            if "payees" in effective_apply_entity_set:
                self._log_stage("apply", "payees", "start", total=len(source_payees))
                self._seed_payee_name_mappings(source_payees, checkpoint, report)
                self._log_stage("apply", "payees", "complete")

            if "transactions" in effective_apply_entity_set:
                self._log_stage("apply", "transactions", "start", total=len(source_transactions))
                self._create_transactions(
                    source_transactions,
                    source_accounts_by_id,
                    source_payees_by_id,
                    checkpoint,
                    report,
                )
                self._log_stage("apply", "transactions", "complete")

            if "scheduled_transactions" in effective_apply_entity_set:
                self._log_stage("apply", "payees_refresh", "start", total=len(source_payees))
                self._seed_payee_name_mappings(source_payees, checkpoint, report)
                self._log_stage("apply", "payees_refresh", "complete")
                self._log_stage(
                    "apply",
                    "scheduled_transactions",
                    "start",
                    total=len(source_scheduled_transactions),
                )
                self._create_scheduled_transactions(
                    source_scheduled_transactions,
                    source_accounts_by_id,
                    source_payees_by_id,
                    checkpoint,
                    report,
                )
                self._log_stage("apply", "scheduled_transactions", "complete")

            if "month_budgets" in effective_apply_entity_set:
                self._log_stage(
                    "apply",
                    "month_budgets",
                    "start",
                    total=len(month_category_budgets),
                )
                self._apply_month_budgets(
                    month_category_budgets=month_category_budgets,
                    checkpoint=checkpoint,
                    report=report,
                    source_internal_category_ids=source_internal_category_ids,
                )
                self._log_stage("apply", "month_budgets", "complete")

            report["mapping_counts"] = {
                "accounts": len(checkpoint.list_mappings("account")),
                "category_groups": len(checkpoint.list_mappings("category_group")),
                "categories": len(checkpoint.list_mappings("category")),
                "payees": len(checkpoint.list_mappings("payee")),
                "transactions": len(checkpoint.list_mappings("transaction")),
                "scheduled_transactions": len(checkpoint.list_mappings("scheduled_transaction")),
            }
            report["checkpoint"] = str(self.paths.checkpoint_path)
            report["exclusions"] = checkpoint.get_metadata("exclusions", [])
            report["events"] = checkpoint.list_events(limit=500)

            atomic_write_json(self.paths.apply_report_path, report)
            self._log_stage(
                "apply",
                "command",
                "complete",
                errors=len(report.get("errors", [])),
                warnings=len(report.get("warnings", [])),
                apply_report_path=self.paths.apply_report_path,
            )
            return report
        finally:
            checkpoint.close()

    def resume(self) -> Dict[str, Any]:
        return self.apply()

    def verify(self) -> Dict[str, Any]:
        self._log_stage(
            "verify",
            "command",
            "start",
            source_plan_id=self.source_plan_id,
            dest_plan_id=self.dest_plan_id,
            workdir=self.paths.workdir,
        )
        snapshot = self._load_snapshot()
        checkpoint = CheckpointStore(self.paths.checkpoint_path)
        try:
            source = snapshot["entities"]
            source_system_entities = snapshot["system_entities"]
            non_migratable_fields = snapshot.get("non_migratable_fields", {})
            exclusions = checkpoint.get_metadata("exclusions", [])
            exclusion_set = {(entry["entity"], entry["source_id"]) for entry in exclusions}
            source_internal_category_ids = set(source_system_entities["source_internal_inflow_category_ids"])
            source_internal_category_ids.update(
                source_system_entities["source_internal_uncategorized_category_ids"]
            )

            report: Dict[str, Any] = {
                "mode": "verify",
                "generated_at": now_utc_iso(),
                "source_plan_id": self.source_plan_id,
                "dest_plan_id": self.dest_plan_id,
                "mismatches": [],
                "warnings": [],
                "exclusions": exclusions,
                "non_migratable_fields": non_migratable_fields,
            }
            account_note_count = len(non_migratable_fields.get("account_notes", []))
            month_note_count = len(non_migratable_fields.get("month_notes", []))
            if account_note_count:
                self._append_warning(
                    report,
                    f"{account_note_count} account notes are read-only in YNAB API and were not verified for parity."
                )
            if month_note_count:
                self._append_warning(
                    report,
                    f"{month_note_count} month notes are read-only in YNAB API and were not verified for parity."
                )

            self._log_stage("verify", "fetch_destination_snapshot", "start")
            dest_plan = self.dest_client.get_plan(self.dest_plan_id).get("plan", {})
            dest_accounts = {a["id"]: a for a in clean_deleted(dest_plan.get("accounts", []))}
            dest_groups = {
                group["id"]: group
                for group in clean_deleted(dest_plan.get("category_groups", []))
            }
            dest_categories = {
                category["id"]: category
                for category in clean_deleted(dest_plan.get("categories", []))
            }
            dest_transactions_raw = clean_deleted(dest_plan.get("transactions", []))
            dest_subtransactions = clean_deleted(dest_plan.get("subtransactions", []))
            dest_scheduled_raw = clean_deleted(dest_plan.get("scheduled_transactions", []))
            dest_scheduled_subs = clean_deleted(dest_plan.get("scheduled_subtransactions", []))
            self._log_stage("verify", "fetch_destination_snapshot", "complete")

            subtransactions_by_tx_id: Dict[str, List[Dict[str, Any]]] = {}
            for sub in dest_subtransactions:
                subtransactions_by_tx_id.setdefault(sub["transaction_id"], []).append(sub)

            scheduled_subs_by_id: Dict[str, List[Dict[str, Any]]] = {}
            for sub in dest_scheduled_subs:
                scheduled_subs_by_id.setdefault(sub["scheduled_transaction_id"], []).append(sub)

            dest_transactions = {}
            for tx in dest_transactions_raw:
                tx_copy = dict(tx)
                tx_copy["subtransactions"] = subtransactions_by_tx_id.get(tx["id"], [])
                dest_transactions[tx["id"]] = tx_copy

            dest_scheduled = {}
            for scheduled in dest_scheduled_raw:
                sch_copy = dict(scheduled)
                sch_copy["subtransactions"] = scheduled_subs_by_id.get(scheduled["id"], [])
                dest_scheduled[scheduled["id"]] = sch_copy

            account_map = checkpoint.get_mapping_dict("account")
            group_map = checkpoint.get_mapping_dict("category_group")
            category_map = checkpoint.get_mapping_dict("category")
            transaction_map = checkpoint.get_mapping_dict("transaction")
            scheduled_map = checkpoint.get_mapping_dict("scheduled_transaction")

            source_accounts_by_id = {account["id"]: account for account in source["accounts"]}
            source_groups_by_id = {group["id"]: group for group in source["category_groups"]}
            source_categories_by_id = {category["id"]: category for category in source["categories"]}
            source_payees_by_id = {payee["id"]: payee for payee in source["payees"]}
            source_transactions_by_id = {tx["id"]: tx for tx in source["transactions"]}
            source_scheduled_by_id = {
                scheduled["id"]: scheduled for scheduled in source["scheduled_transactions"]
            }
            payee_map = checkpoint.get_mapping_dict("payee")
            payee_map = self._augment_payee_map_by_name(
                source_payees=source["payees"],
                existing_payee_map=payee_map,
            )

            # Accounts
            self._log_stage("verify", "accounts_parity", "start", mapped=len(account_map))
            for source_id, dest_id in account_map.items():
                source_account = source_accounts_by_id.get(source_id)
                dest_account = dest_accounts.get(dest_id)
                if source_account is None:
                    continue
                if dest_account is None:
                    self._record_mismatch(report, "account", source_id, "missing destination account")
                    continue
                if _is_supported_account_create_type(source_account.get("type")):
                    expected = _canonical_entity_projection(source_account, ["name", "type", "balance"])
                    actual = _canonical_entity_projection(dest_account, ["name", "type", "balance"])
                else:
                    expected = _canonical_entity_projection(source_account, ["name", "balance"])
                    actual = _canonical_entity_projection(dest_account, ["name", "balance"])
                if expected != actual:
                    self._record_mismatch(report, "account", source_id, "field mismatch", expected, actual)
            self._log_stage("verify", "accounts_parity", "complete")

            # Category groups
            self._log_stage("verify", "category_groups_parity", "start", mapped=len(group_map))
            for source_id, dest_id in group_map.items():
                source_group = source_groups_by_id.get(source_id)
                dest_group = dest_groups.get(dest_id)
                if source_group is None:
                    continue
                if dest_group is None:
                    self._record_mismatch(report, "category_group", source_id, "missing destination group")
                    continue
                if source_group.get("name") != dest_group.get("name"):
                    self._record_mismatch(
                        report,
                        "category_group",
                        source_id,
                        "name mismatch",
                        {"name": source_group.get("name")},
                        {"name": dest_group.get("name")},
                    )
            self._log_stage("verify", "category_groups_parity", "complete")

            # Categories
            self._log_stage("verify", "categories_parity", "start", mapped=len(category_map))
            for source_id, dest_id in category_map.items():
                source_category = source_categories_by_id.get(source_id)
                dest_category = dest_categories.get(dest_id)
                if source_category is None:
                    continue
                if dest_category is None:
                    self._record_mismatch(report, "category", source_id, "missing destination category")
                    continue
                expected = {
                    "name": source_category.get("name"),
                    "note": source_category.get("note"),
                    "goal_target": source_category.get("goal_target"),
                    "goal_target_date": source_category.get("goal_target_date"),
                    "category_group_id": group_map.get(source_category.get("category_group_id")),
                }
                actual = {
                    "name": dest_category.get("name"),
                    "note": dest_category.get("note"),
                    "goal_target": dest_category.get("goal_target"),
                    "goal_target_date": dest_category.get("goal_target_date"),
                    "category_group_id": dest_category.get("category_group_id"),
                }
                if expected != actual:
                    self._record_mismatch(report, "category", source_id, "field mismatch", expected, actual)
            self._log_stage("verify", "categories_parity", "complete")

            # Transactions
            self._log_stage("verify", "transactions_parity", "start", mapped=len(transaction_map))
            for source_id, dest_id in transaction_map.items():
                if ("transaction", source_id) in exclusion_set:
                    continue
                source_tx = source_transactions_by_id.get(source_id)
                dest_tx = dest_transactions.get(dest_id)
                if source_tx is None:
                    continue
                if dest_tx is None:
                    self._record_mismatch(report, "transaction", source_id, "missing destination transaction")
                    continue
                expected_tx, _ = self._build_transaction_payload(
                    tx=source_tx,
                    source_accounts_by_id=source_accounts_by_id,
                    source_payees_by_id=source_payees_by_id,
                    account_map=account_map,
                    category_map=category_map,
                    payee_map=payee_map,
                    include_import_id=True,
                    report=report,
                )
                if not expected_tx:
                    continue
                actual_tx = self._normalize_destination_transaction(dest_tx)
                if canonical_json(expected_tx) != canonical_json(actual_tx):
                    self._record_mismatch(
                        report,
                        "transaction",
                        source_id,
                        "field mismatch",
                        expected_tx,
                        actual_tx,
                    )
            self._log_stage("verify", "transactions_parity", "complete")

            # Scheduled transactions
            self._log_stage(
                "verify",
                "scheduled_transactions_parity",
                "start",
                mapped=len(scheduled_map),
            )
            reference_date = date.fromisoformat(checkpoint.get_metadata("apply_reference_date"))
            for source_id, dest_id in scheduled_map.items():
                if ("scheduled_transaction", source_id) in exclusion_set:
                    continue
                source_sched = source_scheduled_by_id.get(source_id)
                dest_sched = dest_scheduled.get(dest_id)
                if source_sched is None:
                    continue
                if dest_sched is None:
                    self._record_mismatch(
                        report,
                        "scheduled_transaction",
                        source_id,
                        "missing destination scheduled transaction",
                    )
                    continue
                expected_sched, _ = self._build_scheduled_payload(
                    scheduled=source_sched,
                    source_accounts_by_id=source_accounts_by_id,
                    source_payees_by_id=source_payees_by_id,
                    account_map=account_map,
                    category_map=category_map,
                    payee_map=payee_map,
                    reference_date=reference_date,
                    report=report,
                )
                if not expected_sched:
                    continue
                actual_sched = self._normalize_destination_scheduled(dest_sched)
                if canonical_json(expected_sched) != canonical_json(actual_sched):
                    self._record_mismatch(
                        report,
                        "scheduled_transaction",
                        source_id,
                        "field mismatch",
                        expected_sched,
                        actual_sched,
                    )
            self._log_stage("verify", "scheduled_transactions_parity", "complete")

            # Month budget parity (migrated subset parity)
            self._log_stage("verify", "month_budget_parity", "start")
            month_cache: Dict[str, Dict[str, Any]] = {}
            total_month_budget_entries = len(source["month_category_budgets"])
            for idx, entry in enumerate(source["month_category_budgets"], start=1):
                self._log_progress("verify", "month_budget_parity", idx, total_month_budget_entries)
                source_category_id = entry["category_id"]
                if source_category_id in source_internal_category_ids:
                    continue
                if ("month_budget", f"{entry['month']}:{source_category_id}") in exclusion_set:
                    continue
                dest_category_id = category_map.get(source_category_id)
                if not dest_category_id:
                    self._record_mismatch(
                        report,
                        "month_budget",
                        f"{entry['month']}:{source_category_id}",
                        "missing category mapping",
                    )
                    continue

                month = entry["month"]
                if month not in month_cache:
                    month_cache[month] = self.dest_client.get_plan_month(self.dest_plan_id, month).get(
                        "month", {}
                    )
                categories = month_cache[month].get("categories", [])
                categories_map = {c["id"]: c for c in clean_deleted(categories)}
                dest_month_category = categories_map.get(dest_category_id)
                if not dest_month_category:
                    self._record_mismatch(
                        report,
                        "month_budget",
                        f"{entry['month']}:{source_category_id}",
                        "missing destination month category",
                    )
                    continue
                expected_budgeted = _safe_int(entry["budgeted"])
                actual_budgeted = _safe_int(dest_month_category.get("budgeted"))
                if expected_budgeted != actual_budgeted:
                    self._record_mismatch(
                        report,
                        "month_budget",
                        f"{entry['month']}:{source_category_id}",
                        "budgeted mismatch",
                        {"budgeted": expected_budgeted},
                        {"budgeted": actual_budgeted},
                    )
            self._log_stage("verify", "month_budget_parity", "complete")

            report["passed"] = len(report["mismatches"]) == 0
            report["mismatch_count"] = len(report["mismatches"])
            report["mapping_counts"] = {
                "accounts": len(account_map),
                "category_groups": len(group_map),
                "categories": len(category_map),
                "transactions": len(transaction_map),
                "scheduled_transactions": len(scheduled_map),
            }
            atomic_write_json(self.paths.verify_report_path, report)
            self._log_stage(
                "verify",
                "command",
                "complete",
                mismatch_count=report.get("mismatch_count"),
                verify_report_path=self.paths.verify_report_path,
            )
            return report
        finally:
            checkpoint.close()

    # ---------- Plan internals ----------
    def _extract_transactions(self, source_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        transactions = clean_deleted(source_plan.get("transactions", []))
        subtransactions = clean_deleted(source_plan.get("subtransactions", []))
        sub_by_tx_id: Dict[str, List[Dict[str, Any]]] = {}
        for sub in subtransactions:
            sub_by_tx_id.setdefault(sub.get("transaction_id"), []).append(sub)

        extracted: List[Dict[str, Any]] = []
        for tx in transactions:
            tx_copy = dict(tx)
            tx_copy["subtransactions"] = clean_deleted(sub_by_tx_id.get(tx.get("id"), []))
            extracted.append(tx_copy)
        extracted.sort(key=lambda item: (item.get("date") or "", item.get("id") or ""))
        return extracted

    def _extract_scheduled_transactions(
        self,
        source_plan: Dict[str, Any],
        report: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        scheduled = clean_deleted(source_plan.get("scheduled_transactions", []))
        scheduled_subtransactions = clean_deleted(source_plan.get("scheduled_subtransactions", []))
        sub_by_scheduled_id: Dict[str, List[Dict[str, Any]]] = {}
        for sub in scheduled_subtransactions:
            sub_by_scheduled_id.setdefault(sub.get("scheduled_transaction_id"), []).append(sub)

        extracted: List[Dict[str, Any]] = []
        for scheduled_tx in scheduled:
            scheduled_copy = dict(scheduled_tx)
            scheduled_copy["subtransactions"] = clean_deleted(
                sub_by_scheduled_id.get(scheduled_tx.get("id"), [])
            )
            extracted.append(scheduled_copy)
        extracted.sort(key=lambda item: (item.get("date_next") or "", item.get("id") or ""))

        unsupported_split_count = sum(
            1 for scheduled_tx in extracted if scheduled_tx.get("subtransactions")
        )
        if unsupported_split_count:
            self._append_warning(
                report,
                f"{unsupported_split_count} scheduled transactions have subtransactions and will be excluded."
            )
        return extracted

    def _extract_month_budgets(
        self, month_summaries: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        month_budgets: List[Dict[str, Any]] = []
        month_notes: List[Dict[str, Any]] = []
        ordered_months = sorted(month_summaries, key=lambda item: item.get("month", ""))
        for idx, month_summary in enumerate(ordered_months, start=1):
            self._log_progress("plan", "month_budget_extraction", idx, len(ordered_months))
            month = month_summary.get("month")
            if not month:
                continue
            month_detail = self.source_client.get_plan_month(self.source_plan_id, month).get("month", {})
            month_note = month_detail.get("note")
            if month_note:
                month_notes.append({"month": month, "note": month_note})
            categories = clean_deleted(month_detail.get("categories", []))
            for category in categories:
                category_id = category.get("id")
                if not category_id:
                    continue
                month_budgets.append(
                    {
                        "month": month,
                        "category_id": category_id,
                        "budgeted": _safe_int(category.get("budgeted")),
                    }
                )
        month_budgets.sort(key=lambda item: (item["month"], item["category_id"]))
        month_notes.sort(key=lambda item: (item.get("month") or ""))
        return month_budgets, month_notes

    def _extract_non_migratable_fields(
        self,
        source_accounts: List[Dict[str, Any]],
        month_notes: List[Dict[str, Any]],
        report: Dict[str, Any],
    ) -> Dict[str, Any]:
        account_notes = []
        for account in source_accounts:
            note = account.get("note")
            if not note:
                continue
            account_notes.append(
                {
                    "account_id": account.get("id"),
                    "account_name": account.get("name"),
                    "note": note,
                }
            )
        if account_notes:
            self._append_warning(
                report,
                f"{len(account_notes)} account notes detected; account notes are read-only and will be reported only."
            )
        if month_notes:
            self._append_warning(
                report,
                f"{len(month_notes)} month notes detected; month notes are read-only and will be reported only."
            )
        return {
            "account_notes": account_notes,
            "month_notes": month_notes,
        }

    def _estimate_apply_requests(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        entities = snapshot["entities"]
        system_entities = snapshot["system_entities"]
        transaction_count = len(entities["transactions"])
        tx_requests = int(math.ceil(transaction_count / float(self.tx_batch_size)))
        system_group_count = len(
            set(system_entities["source_internal_master_category_group_ids"])
            | set(system_entities["source_hidden_category_group_ids"])
            | set(system_entities["source_credit_card_payments_group_ids"])
        )
        system_category_count = len(
            set(system_entities["source_internal_inflow_category_ids"])
            | set(system_entities["source_internal_uncategorized_category_ids"])
            | set(system_entities["source_credit_card_payment_category_ids"])
        )
        internal_category_id_set = set(system_entities["source_internal_inflow_category_ids"])
        internal_category_id_set.update(system_entities["source_internal_uncategorized_category_ids"])
        source_payees_by_id = {
            payee.get("id"): payee
            for payee in entities["payees"]
            if isinstance(payee, dict) and payee.get("id")
        }
        starting_balance_tx_count = sum(
            1
            for tx in entities["transactions"]
            if self._is_starting_balance_transaction(tx, source_payees_by_id)
        )
        internal_month_budget_count = sum(
            1
            for entry in entities["month_category_budgets"]
            if entry.get("category_id") in internal_category_id_set
        )
        estimate = {
            "create_accounts": len(entities["accounts"]),
            "create_category_groups": max(0, len(entities["category_groups"]) - system_group_count),
            "create_categories": max(0, len(entities["categories"]) - system_category_count),
            "delete_auto_starting_balance_transactions": starting_balance_tx_count,
            "create_transactions": tx_requests,
            "create_scheduled_transactions": len(entities["scheduled_transactions"]),
            "patch_month_budgets": max(
                0, len(entities["month_category_budgets"]) - internal_month_budget_count
            ),
            "overhead_reads": 16,
        }
        estimate["total_estimated"] = sum(estimate.values())
        estimate["hours_at_200_req_per_hour"] = round(estimate["total_estimated"] / 200.0, 2)
        return estimate

    def _build_plan_manual_action_items(
        self,
        source_accounts: List[Dict[str, Any]],
        destination_accounts: List[Dict[str, Any]],
        source_scheduled_transactions: List[Dict[str, Any]],
        non_migratable_fields: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        action_items: List[Dict[str, Any]] = []

        destination_accounts_by_name_type: Dict[Tuple[str, str], List[str]] = {}
        for account in destination_accounts:
            account_id = account.get("id")
            account_name = account.get("name")
            account_type = account.get("type")
            if not account_id or not isinstance(account_name, str) or not isinstance(account_type, str):
                continue
            key = (account_name, account_type)
            destination_accounts_by_name_type.setdefault(key, []).append(str(account_id))

        unsupported_source_accounts = sorted(
            (
                account
                for account in source_accounts
                if account.get("id") and _coerce_account_type_for_create(account.get("type"))[1]
            ),
            key=lambda item: (item.get("name") or "", item.get("id") or ""),
        )
        for source_account in unsupported_source_accounts:
            source_id = str(source_account.get("id"))
            source_name = str(source_account.get("name") or "")
            source_type = str(source_account.get("type") or "")
            mapped_type, _ = _coerce_account_type_for_create(source_type)
            key = (source_name, source_type)
            matches = destination_accounts_by_name_type.get(key, [])
            if len(matches) == 1:
                continue

            if len(matches) == 0:
                action_items.append(
                    {
                        "severity": "required",
                        "category": "pre_apply",
                        "entity": "account",
                        "source_id": source_id,
                        "title": "Create destination account for unsupported source type",
                        "action": (
                            f"Create destination account '{source_name}' with exact type '{source_type}' "
                            "before apply so it maps directly instead of being created as cash."
                        ),
                        "details": {
                            "account_name": source_name,
                            "source_type": source_type,
                            "fallback_create_type": mapped_type,
                        },
                    }
                )
            else:
                action_items.append(
                    {
                        "severity": "required",
                        "category": "pre_apply",
                        "entity": "account",
                        "source_id": source_id,
                        "title": "Resolve ambiguous destination account matches",
                        "action": (
                            f"Ensure exactly one destination account matches name '{source_name}' "
                            f"and type '{source_type}' before apply."
                        ),
                        "details": {
                            "account_name": source_name,
                            "source_type": source_type,
                            "matched_destination_account_ids": matches,
                        },
                    }
                )

        account_notes = non_migratable_fields.get("account_notes", [])
        if isinstance(account_notes, list) and account_notes:
            account_note_entries = []
            for entry in account_notes:
                if not isinstance(entry, dict):
                    continue
                account_name = entry.get("account_name")
                note = entry.get("note")
                if not account_name or note is None:
                    continue
                account_note_entries.append(
                    {
                        "account_name": str(account_name),
                        "account_note": str(note),
                    }
                )
            action_items.append(
                {
                    "severity": "recommended",
                    "category": "post_apply",
                    "entity": "account_note",
                    "source_id": None,
                    "title": "Copy account notes manually",
                    "action": (
                        f"Manually copy {len(account_notes)} account note(s) after apply "
                        "(YNAB API does not allow writing account notes)."
                    ),
                    "details": {
                        "count": len(account_notes),
                        "accounts": account_note_entries,
                    },
                }
            )

        month_notes = non_migratable_fields.get("month_notes", [])
        if isinstance(month_notes, list) and month_notes:
            month_note_entries = []
            for entry in month_notes:
                if not isinstance(entry, dict):
                    continue
                month_value = entry.get("month")
                note = entry.get("note")
                if not month_value or note is None:
                    continue
                month_note_entries.append(
                    {
                        "month": str(month_value),
                        "month_note": str(note),
                    }
                )
            action_items.append(
                {
                    "severity": "recommended",
                    "category": "post_apply",
                    "entity": "month_note",
                    "source_id": None,
                    "title": "Copy month notes manually",
                    "action": (
                        f"Manually copy {len(month_notes)} month note(s) after apply "
                        "(YNAB API does not allow writing month notes)."
                    ),
                    "details": {
                        "count": len(month_notes),
                        "months": month_note_entries,
                    },
                }
            )

        split_scheduled_count = sum(
            1
            for scheduled_tx in source_scheduled_transactions
            if clean_deleted(scheduled_tx.get("subtransactions", []))
        )
        if split_scheduled_count:
            action_items.append(
                {
                    "severity": "recommended",
                    "category": "post_apply",
                    "entity": "scheduled_transaction",
                    "source_id": None,
                    "title": "Recreate split scheduled transactions manually",
                    "action": (
                        f"Manually recreate {split_scheduled_count} split scheduled transaction(s) "
                        "after apply (YNAB API cannot create scheduled subtransactions)."
                    ),
                    "details": {
                        "count": split_scheduled_count,
                    },
                }
            )

        return action_items

    # ---------- Apply internals ----------
    def _create_accounts(
        self,
        source_accounts: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        cursor_name = "accounts_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        accounts = sorted(source_accounts, key=lambda item: (item.get("name") or "", item.get("id") or ""))
        destination_accounts = clean_deleted(self.dest_client.get_accounts(self.dest_plan_id).get("accounts", []))
        destination_accounts_by_name: Dict[str, List[Dict[str, Any]]] = {}
        for destination_account in destination_accounts:
            destination_id = destination_account.get("id")
            destination_name = destination_account.get("name")
            if not destination_id or not isinstance(destination_name, str):
                continue
            destination_accounts_by_name.setdefault(destination_name, []).append(destination_account)

        existing_account_map = checkpoint.get_mapping_dict("account")
        used_destination_account_ids: Set[str] = {
            str(dest_id) for dest_id in existing_account_map.values() if dest_id
        }

        for idx in range(cursor, len(accounts)):
            self._log_progress("apply", "accounts", idx + 1, len(accounts))
            try:
                account = accounts[idx]
                if is_deleted(account):
                    continue

                source_id = account.get("id")
                if not source_id:
                    continue
                if checkpoint.get_mapping("account", source_id):
                    continue

                source_type = account.get("type")
                account_type, coerced = _coerce_account_type_for_create(source_type)
                source_name = account.get("name")
                type_matches: List[Dict[str, Any]] = []
                if isinstance(source_name, str):
                    name_matches = destination_accounts_by_name.get(source_name, [])
                    type_matches = [
                        candidate
                        for candidate in name_matches
                        if candidate.get("id") and candidate.get("type") == source_type
                    ]

                if len(type_matches) == 1:
                    matched_account = type_matches[0]
                    matched_dest_id = str(matched_account.get("id"))
                    if matched_dest_id in used_destination_account_ids:
                        if not coerced:
                            self._record_issue(
                                report,
                                "warnings",
                                "account",
                                source_id,
                                "exact destination account name+type match already mapped to another source account; creating new account",
                                details={
                                    "account_name": source_name,
                                    "source_type": source_type,
                                    "matched_destination_account_id": matched_dest_id,
                                },
                            )
                            self._increment_system_counter(
                                report,
                                "ambiguous_account_name_type_matches",
                            )
                    else:
                        checkpoint.set_mapping("account", source_id, matched_dest_id)
                        used_destination_account_ids.add(matched_dest_id)
                        checkpoint.add_event(
                            "INFO",
                            f"account mapped {source_id} -> {matched_dest_id} "
                            "(reused existing destination account by exact name+type)",
                        )
                        source_transfer_payee = account.get("transfer_payee_id")
                        dest_transfer_payee = matched_account.get("transfer_payee_id")
                        if source_transfer_payee and dest_transfer_payee:
                            checkpoint.set_mapping("payee", source_transfer_payee, dest_transfer_payee)
                        self._increment_system_counter(report, "reused_accounts_by_name_type")
                        if coerced:
                            self._increment_system_counter(
                                report,
                                "reused_unsupported_accounts_by_name_type",
                            )
                        continue
                elif len(type_matches) > 1 and not coerced:
                    self._record_issue(
                        report,
                        "warnings",
                        "account",
                        source_id,
                        "multiple destination accounts matched exact name+type; creating new account",
                        details={
                            "account_name": source_name,
                            "source_type": source_type,
                            "matched_destination_account_ids": [
                                str(candidate.get("id"))
                                for candidate in type_matches
                                if candidate.get("id")
                            ],
                        },
                    )
                    self._increment_system_counter(
                        report,
                        "ambiguous_account_name_type_matches",
                    )

                if coerced:
                    fallback_reason = "no exact destination account name+type match found"
                    warning_payload: Dict[str, Any] = {
                        "entity": "account",
                        "source_id": source_id,
                        "message": "unsupported account type could not be reused by exact name+type; creating as cash",
                        "account_name": account.get("name"),
                        "source_type": source_type,
                        "mapped_type": account_type,
                    }

                    if len(type_matches) > 1:
                        fallback_reason = "multiple destination accounts matched exact name+type"
                        warning_payload["matched_destination_account_ids"] = [
                            str(candidate.get("id"))
                            for candidate in type_matches
                            if candidate.get("id")
                        ]
                        self._increment_system_counter(
                                report,
                                "ambiguous_unsupported_account_name_type_matches",
                            )
                    elif len(type_matches) == 1:
                        matched_account = type_matches[0]
                        matched_dest_id = str(matched_account.get("id"))
                        fallback_reason = (
                            "exact name+type destination account already mapped to another source account"
                        )
                        warning_payload["matched_destination_account_id"] = matched_dest_id

                    warning_payload["decision_reason"] = fallback_reason
                    report.setdefault("warnings", []).append(warning_payload)
                    self.logger.warning(
                        (
                            "account (source_id=%s, account_name=%s, source_type=%s, mapped_type=%s): "
                            "unsupported type reuse by exact name+type failed (%s); creating as cash"
                        ),
                        source_id,
                        account.get("name"),
                        source_type,
                        account_type,
                        fallback_reason,
                    )
                    self._increment_system_counter(report, "coerced_account_types")

                payload = {
                    "name": account.get("name"),
                    "type": account_type,
                    # Opening balance is seeded by replayed transactions (including Starting Balance),
                    # not by account creation payload.
                    "balance": 0,
                }
                try:
                    created = self.dest_client.create_account(self.dest_plan_id, payload).get("account", {})
                    dest_id = created.get("id")
                    if not dest_id:
                        raise RuntimeError("account creation response missing account.id")
                    checkpoint.set_mapping("account", source_id, dest_id)
                    used_destination_account_ids.add(str(dest_id))
                    checkpoint.add_event("INFO", f"account mapped {source_id} -> {dest_id}")

                    source_transfer_payee = account.get("transfer_payee_id")
                    dest_transfer_payee = created.get("transfer_payee_id")
                    if source_transfer_payee and dest_transfer_payee:
                        checkpoint.set_mapping("payee", source_transfer_payee, dest_transfer_payee)
                except Exception as error:  # noqa: BLE001
                    self._record_issue(
                        report,
                        "errors",
                        "account",
                        source_id,
                        f"failed to create account: {summarize_exception(error)}",
                        details={
                            "account_name": account.get("name"),
                            "source_type": account.get("type"),
                            "mapped_type": payload.get("type"),
                        },
                    )
                    checkpoint.add_event(
                        "ERROR", f"account creation failed for {source_id}: {summarize_exception(error)}"
                    )
            finally:
                checkpoint.set_cursor(cursor_name, idx + 1)

    def _capture_destination_auto_starting_balance_candidates(
        self,
        source_accounts: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        existing_candidates = checkpoint.get_metadata("auto_starting_balance_candidates")
        if isinstance(existing_candidates, dict):
            captured_count = sum(
                len(value)
                for value in existing_candidates.values()
                if isinstance(value, list)
            )
            report.setdefault("system_entity_stats", {})[
                "captured_auto_starting_balance_candidates"
            ] = int(captured_count)
            return

        account_map = checkpoint.get_mapping_dict("account")
        destination_payees = clean_deleted(self.dest_client.get_payees(self.dest_plan_id).get("payees", []))
        destination_payees_by_id = {
            payee.get("id"): payee
            for payee in destination_payees
            if isinstance(payee, dict) and payee.get("id")
        }

        candidates_by_source_account: Dict[str, List[str]] = {}
        for account in sorted(source_accounts, key=lambda item: (item.get("name") or "", item.get("id") or "")):
            source_account_id = account.get("id")
            if not source_account_id:
                continue
            destination_account_id = account_map.get(source_account_id)
            if not destination_account_id:
                continue
            destination_transactions = clean_deleted(
                self.dest_client.get_account_transactions(self.dest_plan_id, destination_account_id).get(
                    "transactions", []
                )
            )
            candidates: List[Tuple[str, str]] = []
            for tx in destination_transactions:
                if not self._is_auto_starting_balance_candidate(tx, destination_payees_by_id):
                    continue
                transaction_id = tx.get("id")
                if not transaction_id:
                    continue
                candidates.append((tx.get("date") or "", str(transaction_id)))
            if not candidates:
                continue
            candidates_by_source_account[source_account_id] = [
                transaction_id for _, transaction_id in sorted(candidates)
            ]

        checkpoint.set_metadata("auto_starting_balance_candidates", candidates_by_source_account)
        if checkpoint.get_metadata("starting_balance_tx_candidate_map") is None:
            checkpoint.set_metadata("starting_balance_tx_candidate_map", {})
        if checkpoint.get_metadata("deleted_auto_starting_balance_candidates") is None:
            checkpoint.set_metadata("deleted_auto_starting_balance_candidates", [])

        captured_count = sum(len(value) for value in candidates_by_source_account.values())
        report.setdefault("system_entity_stats", {})["captured_auto_starting_balance_candidates"] = int(
            captured_count
        )

    def _create_category_groups(
        self,
        source_groups: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        source_system_group_ids: Set[str],
    ) -> None:
        cursor_name = "category_groups_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        groups = sorted(source_groups, key=lambda item: (item.get("name") or "", item.get("id") or ""))
        destination_plan = self.dest_client.get_plan(self.dest_plan_id).get("plan", {})
        destination_groups = clean_deleted(destination_plan.get("category_groups", []))
        destination_groups_by_name: Dict[str, List[Dict[str, Any]]] = {}
        for destination_group in destination_groups:
            destination_id = destination_group.get("id")
            destination_name = destination_group.get("name")
            if not destination_id or not isinstance(destination_name, str):
                continue
            destination_groups_by_name.setdefault(destination_name, []).append(destination_group)

        existing_group_map = checkpoint.get_mapping_dict("category_group")
        used_destination_group_ids: Set[str] = {
            str(dest_id) for dest_id in existing_group_map.values() if dest_id
        }

        for idx in range(cursor, len(groups)):
            self._log_progress("apply", "category_groups", idx + 1, len(groups))
            group = groups[idx]
            if is_deleted(group):
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            source_id = group.get("id")
            if not source_id:
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            if checkpoint.get_mapping("category_group", source_id):
                if source_id in source_system_group_ids:
                    self._increment_system_counter(report, "skipped_system_category_group_creates")
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            group_name = group.get("name")
            if isinstance(group_name, str):
                existing_matches = destination_groups_by_name.get(group_name, [])
                if len(existing_matches) == 1:
                    existing_dest_id = str(existing_matches[0].get("id"))
                    if existing_dest_id in used_destination_group_ids:
                        self._record_issue(
                            report,
                            "warnings",
                            "category_group",
                            source_id,
                            "exact destination category group name match already mapped; creating new category group",
                            details={
                                "category_group_name": group_name,
                                "matched_destination_category_group_id": existing_dest_id,
                            },
                        )
                        self._increment_system_counter(report, "ambiguous_category_group_name_matches")
                    else:
                        checkpoint.set_mapping("category_group", source_id, existing_dest_id)
                        used_destination_group_ids.add(existing_dest_id)
                        checkpoint.add_event(
                            "INFO",
                            f"category_group mapped {source_id} -> {existing_dest_id} "
                            "(reused existing destination category group by exact name)",
                        )
                        self._increment_system_counter(report, "reused_category_groups_by_name")
                        checkpoint.set_cursor(cursor_name, idx + 1)
                        continue
                elif len(existing_matches) > 1:
                    self._record_issue(
                        report,
                        "warnings",
                        "category_group",
                        source_id,
                        "multiple destination category groups matched exact name; creating new category group",
                        details={
                            "category_group_name": group_name,
                            "matched_destination_category_group_ids": [
                                str(match.get("id"))
                                for match in existing_matches
                                if match.get("id")
                            ],
                        },
                    )
                    self._increment_system_counter(report, "ambiguous_category_group_name_matches")

            payload = {"name": group.get("name")}
            try:
                created = self.dest_client.create_category_group(self.dest_plan_id, payload).get(
                    "category_group", {}
                )
                dest_id = created.get("id")
                if not dest_id:
                    raise RuntimeError("category group response missing id")
                checkpoint.set_mapping("category_group", source_id, dest_id)
                used_destination_group_ids.add(str(dest_id))
                if isinstance(payload.get("name"), str):
                    destination_groups_by_name.setdefault(payload["name"], []).append(created)
                checkpoint.add_event("INFO", f"category_group mapped {source_id} -> {dest_id}")
            except Exception as error:  # noqa: BLE001
                self._record_issue(
                    report,
                    "errors",
                    "category_group",
                    source_id,
                    f"failed to create category group: {summarize_exception(error)}",
                    details={
                        "category_group_name": group.get("name"),
                    },
                )
                checkpoint.add_event(
                    "ERROR",
                    f"category_group creation failed for {source_id}: {summarize_exception(error)}",
                )
            finally:
                checkpoint.set_cursor(cursor_name, idx + 1)

    def _extract_destination_categories(
        self,
        categories_response: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        categories: List[Dict[str, Any]] = []

        flat_categories = categories_response.get("categories")
        if isinstance(flat_categories, list):
            for item in flat_categories:
                if isinstance(item, dict):
                    categories.append(item)

        category_groups = categories_response.get("category_groups")
        if isinstance(category_groups, list):
            for group in category_groups:
                if not isinstance(group, dict):
                    continue
                nested_categories = group.get("categories")
                if not isinstance(nested_categories, list):
                    continue
                group_id = group.get("id")
                for item in nested_categories:
                    if not isinstance(item, dict):
                        continue
                    if not item.get("category_group_id") and group_id:
                        merged = dict(item)
                        merged["category_group_id"] = group_id
                        categories.append(merged)
                    else:
                        categories.append(item)

        deduped_by_id: Dict[str, Dict[str, Any]] = {}
        without_id: List[Dict[str, Any]] = []
        for category in clean_deleted(categories):
            category_id = category.get("id")
            if category_id:
                deduped_by_id[str(category_id)] = category
            else:
                without_id.append(category)
        return list(deduped_by_id.values()) + without_id

    def _create_categories(
        self,
        source_categories: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        source_system_category_ids: Set[str],
    ) -> None:
        cursor_name = "categories_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        categories = sorted(
            source_categories,
            key=lambda item: (item.get("category_group_id") or "", item.get("name") or "", item.get("id") or ""),
        )
        categories_response = self.dest_client.get_categories(self.dest_plan_id)
        destination_categories = self._extract_destination_categories(categories_response)
        has_flat_shape = isinstance(categories_response.get("categories"), list)
        has_grouped_shape = isinstance(categories_response.get("category_groups"), list)
        if not destination_categories and not has_flat_shape and not has_grouped_shape:
            self._record_issue(
                report,
                "warnings",
                "category",
                None,
                "destination categories response is missing both 'categories' and 'category_groups'; "
                "proceeding without preexisting category reuse",
                details={
                    "response_keys": sorted(str(key) for key in categories_response.keys()),
                },
            )

        categories_by_group_and_name: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for destination_category in destination_categories:
            destination_id = destination_category.get("id")
            destination_name = destination_category.get("name")
            destination_group_id = destination_category.get("category_group_id")
            if (
                not destination_id
                or not isinstance(destination_name, str)
                or not isinstance(destination_group_id, str)
            ):
                continue
            key = (destination_group_id, destination_name)
            categories_by_group_and_name.setdefault(key, []).append(destination_category)

        existing_category_map = checkpoint.get_mapping_dict("category")
        used_destination_category_ids: Set[str] = {
            str(dest_id) for dest_id in existing_category_map.values() if dest_id
        }

        for idx in range(cursor, len(categories)):
            self._log_progress("apply", "categories", idx + 1, len(categories))
            category = categories[idx]
            if is_deleted(category):
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            source_id = category.get("id")
            if not source_id:
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            if checkpoint.get_mapping("category", source_id):
                if source_id in source_system_category_ids:
                    self._increment_system_counter(report, "skipped_system_category_creates")
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            source_group_id = category.get("category_group_id")
            dest_group_id = checkpoint.get_mapping("category_group", source_group_id) if source_group_id else None
            if not dest_group_id:
                self._record_issue(
                    report,
                    "errors",
                    "category",
                    source_id,
                    f"missing destination category group mapping for {source_group_id}",
                    details={
                        "category_name": category.get("name"),
                        "source_category_group_id": source_group_id,
                    },
                )
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            category_name = category.get("name")
            if isinstance(category_name, str):
                existing_matches = categories_by_group_and_name.get((str(dest_group_id), category_name), [])
                if len(existing_matches) == 1:
                    existing_dest_id = str(existing_matches[0].get("id"))
                    if existing_dest_id in used_destination_category_ids:
                        self._record_issue(
                            report,
                            "warnings",
                            "category",
                            source_id,
                            "exact destination category name+group match already mapped; creating new category",
                            details={
                                "category_name": category_name,
                                "source_category_group_id": source_group_id,
                                "dest_category_group_id": dest_group_id,
                                "matched_destination_category_id": existing_dest_id,
                            },
                        )
                        self._increment_system_counter(report, "ambiguous_category_name_group_matches")
                    else:
                        checkpoint.set_mapping("category", source_id, existing_dest_id)
                        used_destination_category_ids.add(existing_dest_id)
                        checkpoint.add_event(
                            "INFO",
                            f"category mapped {source_id} -> {existing_dest_id} "
                            "(reused existing destination category by exact name+group)",
                        )
                        self._increment_system_counter(report, "reused_categories_by_name_group")
                        checkpoint.set_cursor(cursor_name, idx + 1)
                        continue
                elif len(existing_matches) > 1:
                    self._record_issue(
                        report,
                        "warnings",
                        "category",
                        source_id,
                        "multiple destination categories matched exact name+group; creating new category",
                        details={
                            "category_name": category_name,
                            "source_category_group_id": source_group_id,
                            "dest_category_group_id": dest_group_id,
                            "matched_destination_category_ids": [
                                str(match.get("id"))
                                for match in existing_matches
                                if match.get("id")
                            ],
                        },
                    )
                    self._increment_system_counter(report, "ambiguous_category_name_group_matches")

            payload = {
                "name": category.get("name"),
                "note": category.get("note"),
                "category_group_id": dest_group_id,
            }
            if category.get("goal_target") is not None:
                payload["goal_target"] = _safe_int(category.get("goal_target"))
            if category.get("goal_target_date"):
                payload["goal_target_date"] = category.get("goal_target_date")
            try:
                created = self.dest_client.create_category(self.dest_plan_id, payload).get("category", {})
                dest_id = created.get("id")
                if not dest_id:
                    raise RuntimeError("category creation response missing id")
                checkpoint.set_mapping("category", source_id, dest_id)
                used_destination_category_ids.add(str(dest_id))
                if isinstance(payload.get("name"), str):
                    key = (str(dest_group_id), payload["name"])
                    categories_by_group_and_name.setdefault(key, []).append(created)
                checkpoint.add_event("INFO", f"category mapped {source_id} -> {dest_id}")
            except Exception as error:  # noqa: BLE001
                self._record_issue(
                    report,
                    "errors",
                    "category",
                    source_id,
                    f"failed to create category: {summarize_exception(error)}",
                    details={
                        "category_name": category.get("name"),
                        "source_category_group_id": source_group_id,
                        "dest_category_group_id": dest_group_id,
                    },
                )
                checkpoint.add_event(
                    "ERROR",
                    f"category creation failed for {source_id}: {summarize_exception(error)}",
                )
            finally:
                checkpoint.set_cursor(cursor_name, idx + 1)

    def _seed_payee_name_mappings(
        self,
        source_payees: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        destination_payees = clean_deleted(self.dest_client.get_payees(self.dest_plan_id).get("payees", []))
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for payee in destination_payees:
            if payee.get("transfer_account_id"):
                continue
            destination_id = payee.get("id")
            destination_name = payee.get("name")
            if not destination_id or not isinstance(destination_name, str):
                continue
            by_name.setdefault(destination_name, []).append(payee)

        existing_payee_map = checkpoint.get_mapping_dict("payee")
        used_destination_payee_ids: Set[str] = {
            str(dest_id) for dest_id in existing_payee_map.values() if dest_id
        }

        for source_payee in source_payees:
            if is_deleted(source_payee):
                continue
            source_id = source_payee.get("id")
            if not source_id:
                continue
            if source_payee.get("transfer_account_id"):
                continue
            if checkpoint.get_mapping("payee", source_id):
                continue

            source_name = source_payee.get("name")
            if not isinstance(source_name, str):
                continue
            matches = by_name.get(source_name, [])
            if len(matches) == 1:
                matched_dest_id = str(matches[0].get("id"))
                if matched_dest_id in used_destination_payee_ids:
                    self._record_issue(
                        report,
                        "warnings",
                        "payee",
                        source_id,
                        "exact destination payee name match already mapped; leaving unmapped",
                        details={
                            "payee_name": source_name,
                            "matched_destination_payee_id": matched_dest_id,
                        },
                    )
                    self._increment_system_counter(report, "ambiguous_payee_exact_name_matches")
                    continue
                checkpoint.set_mapping("payee", source_id, matched_dest_id)
                used_destination_payee_ids.add(matched_dest_id)
                checkpoint.add_event("INFO", f"payee mapped by exact name {source_id} -> {matched_dest_id}")
                self._increment_system_counter(report, "reused_payees_by_exact_name")
            elif len(matches) > 1:
                self._record_issue(
                    report,
                    "warnings",
                    "payee",
                    source_id,
                    "multiple destination payees matched exact name; leaving unmapped",
                    details={
                        "payee_name": source_name,
                        "matched_destination_payee_ids": [
                            str(item.get("id"))
                            for item in matches
                            if item.get("id")
                        ],
                    },
                )
                self._increment_system_counter(report, "ambiguous_payee_exact_name_matches")

    def _create_transactions(
        self,
        source_transactions: List[Dict[str, Any]],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        cursor_name = "transactions_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        transactions = sorted(source_transactions, key=lambda item: (item.get("date") or "", item.get("id") or ""))
        transactions_by_id = {
            str(tx.get("id")): tx for tx in transactions if isinstance(tx, dict) and tx.get("id")
        }

        existing_import_map = self._load_destination_transaction_import_map()
        account_map = checkpoint.get_mapping_dict("account")
        category_map = checkpoint.get_mapping_dict("category")
        payee_map = checkpoint.get_mapping_dict("payee")
        runtime_batch_limit = _safe_int(
            checkpoint.get_metadata("transactions_runtime_batch_limit"),
            self.tx_batch_size,
        )
        runtime_batch_limit = max(1, min(self.tx_batch_size, runtime_batch_limit))
        batching = report.setdefault("batching", {})
        batching["tx_batch_target"] = self.tx_batch_size
        batching["tx_batch_runtime_limit"] = runtime_batch_limit

        while cursor < len(transactions):
            self._log_progress("apply", "transactions", cursor + 1, len(transactions))
            batch_entries: List[Dict[str, Any]] = []
            scan_idx = cursor
            while scan_idx < len(transactions):
                self._log_progress("apply", "transactions", scan_idx + 1, len(transactions))
                tx = transactions[scan_idx]
                if is_deleted(tx):
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue
                source_id = tx.get("id")
                if not source_id:
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue
                if checkpoint.get_mapping("transaction", source_id):
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue

                transfer_partner_source_id = tx.get("transfer_transaction_id")
                if transfer_partner_source_id:
                    pair_ids = sorted([str(source_id), str(transfer_partner_source_id)])
                    canonical_source_id = pair_ids[0]
                    if str(source_id) != canonical_source_id:
                        scan_idx += 1
                        cursor = scan_idx
                        checkpoint.set_cursor(cursor_name, cursor)
                        continue
                    partner_tx = transactions_by_id.get(str(transfer_partner_source_id))
                    if isinstance(partner_tx, dict) and not is_deleted(partner_tx):
                        self._process_transfer_transaction_pair(
                            primary_tx=tx,
                            counterpart_tx=partner_tx,
                            source_accounts_by_id=source_accounts_by_id,
                            source_payees_by_id=source_payees_by_id,
                            checkpoint=checkpoint,
                            report=report,
                            account_map=account_map,
                            category_map=category_map,
                            payee_map=payee_map,
                        )
                        scan_idx += 1
                        cursor = scan_idx
                        checkpoint.set_cursor(cursor_name, cursor)
                        continue

                import_id = deterministic_import_id(self.source_plan_id, source_id)
                if import_id in existing_import_map:
                    checkpoint.set_mapping("transaction", source_id, existing_import_map[import_id])
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue

                if self._is_starting_balance_transaction(tx, source_payees_by_id):
                    # Preserve ordering while keeping batch efficiency for non-starting-balance entries.
                    if batch_entries:
                        break
                    self._process_starting_balance_transaction(
                        tx=tx,
                        source_accounts_by_id=source_accounts_by_id,
                        source_payees_by_id=source_payees_by_id,
                        checkpoint=checkpoint,
                        report=report,
                        account_map=account_map,
                        category_map=category_map,
                        payee_map=payee_map,
                        existing_import_map=existing_import_map,
                    )
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue

                payload, error = self._build_transaction_payload(
                    tx=tx,
                    source_accounts_by_id=source_accounts_by_id,
                    source_payees_by_id=source_payees_by_id,
                    account_map=account_map,
                    category_map=category_map,
                    payee_map=payee_map,
                    include_import_id=True,
                    report=report,
                )
                if payload is None:
                    self._record_issue(
                        report,
                        "errors",
                        "transaction",
                        source_id,
                        error or "payload build failed",
                        details=self._transaction_issue_details(tx),
                    )
                    scan_idx += 1
                    cursor = scan_idx
                    checkpoint.set_cursor(cursor_name, cursor)
                    continue

                payload["import_id"] = import_id
                batch_entries.append(
                    {
                        "source_id": source_id,
                        "import_id": import_id,
                        "payload": payload,
                        "tx": tx,
                    }
                )
                scan_idx += 1
                if len(batch_entries) >= runtime_batch_limit:
                    break

            if not batch_entries:
                continue

            max_success_batch_size, saw_size_limit = self._submit_transaction_batch_entries(
                batch_entries=batch_entries,
                checkpoint=checkpoint,
                report=report,
                existing_import_map=existing_import_map,
            )
            if saw_size_limit and max_success_batch_size > 0 and max_success_batch_size < runtime_batch_limit:
                runtime_batch_limit = max_success_batch_size
                batching["tx_batch_runtime_limit"] = runtime_batch_limit
                checkpoint.set_metadata("transactions_runtime_batch_limit", runtime_batch_limit)
                self._log_stage(
                    "apply",
                    "transactions_batch_limit",
                    "complete",
                    effective_batch_size=runtime_batch_limit,
                )

            cursor = scan_idx
            checkpoint.set_cursor(cursor_name, cursor)

    def _submit_transaction_batch_entries(
        self,
        batch_entries: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        existing_import_map: Dict[str, str],
    ) -> Tuple[int, bool]:
        if not batch_entries:
            return 0, False

        if len(batch_entries) == 1:
            success = self._submit_single_transaction_entry(
                entry=batch_entries[0],
                checkpoint=checkpoint,
                report=report,
                existing_import_map=existing_import_map,
            )
            return (1 if success else 0), False

        source_by_import = {
            str(entry["import_id"]): str(entry["source_id"])
            for entry in batch_entries
            if entry.get("import_id") and entry.get("source_id")
        }
        payloads = [
            dict(entry["payload"])
            for entry in batch_entries
            if isinstance(entry.get("payload"), dict)
        ]
        if not payloads:
            return 0, False

        try:
            response_data = self.dest_client.create_transactions(self.dest_plan_id, payloads)
            self._apply_transaction_batch_response(
                checkpoint=checkpoint,
                response_data=response_data,
                source_by_import=source_by_import,
                existing_import_map=existing_import_map,
                report=report,
            )
            return len(payloads), False
        except Exception as batch_error:  # noqa: BLE001
            size_limit_hit = self._is_probable_batch_size_error(batch_error)
            self._record_issue(
                report,
                "warnings",
                "transaction_batch",
                None,
                f"batch failed, splitting batch and retrying: {summarize_exception(batch_error)}",
                details={
                    "batch_size": len(batch_entries),
                    "first_source_transaction_id": str(batch_entries[0].get("source_id") or ""),
                    "last_source_transaction_id": str(batch_entries[-1].get("source_id") or ""),
                    "probable_batch_size_limit": size_limit_hit,
                },
            )

        midpoint = max(1, len(batch_entries) // 2)
        left_success, left_size_limit = self._submit_transaction_batch_entries(
            batch_entries=batch_entries[:midpoint],
            checkpoint=checkpoint,
            report=report,
            existing_import_map=existing_import_map,
        )
        right_success, right_size_limit = self._submit_transaction_batch_entries(
            batch_entries=batch_entries[midpoint:],
            checkpoint=checkpoint,
            report=report,
            existing_import_map=existing_import_map,
        )
        return max(left_success, right_success), (size_limit_hit or left_size_limit or right_size_limit)

    def _submit_single_transaction_entry(
        self,
        entry: Dict[str, Any],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        existing_import_map: Dict[str, str],
    ) -> bool:
        source_id = str(entry.get("source_id") or "")
        import_id = str(entry.get("import_id") or "")
        payload = entry.get("payload")
        source_tx = entry.get("tx") if isinstance(entry.get("tx"), dict) else {}
        if not source_id or not import_id or not isinstance(payload, dict):
            return False

        try:
            response_data = self.dest_client.create_transactions(self.dest_plan_id, payload)
            self._apply_transaction_batch_response(
                checkpoint=checkpoint,
                response_data=response_data,
                source_by_import={import_id: source_id},
                existing_import_map=existing_import_map,
                report=report,
            )
            return True
        except Exception as single_error:  # noqa: BLE001
            self._record_issue(
                report,
                "errors",
                "transaction",
                source_id,
                f"single transaction failed: {summarize_exception(single_error)}",
                details=self._transaction_issue_details(source_tx),
            )
            return False

    @staticmethod
    def _extract_saved_transactions(response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        created_objects: List[Dict[str, Any]] = []
        if isinstance(response_data.get("transaction"), dict):
            created_objects = [response_data["transaction"]]
        elif isinstance(response_data.get("transactions"), list):
            created_objects = [item for item in response_data["transactions"] if isinstance(item, dict)]
        return created_objects

    def _process_transfer_transaction_pair(
        self,
        primary_tx: Dict[str, Any],
        counterpart_tx: Dict[str, Any],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        account_map: Dict[str, str],
        category_map: Dict[str, str],
        payee_map: Dict[str, str],
    ) -> None:
        primary_source_id = str(primary_tx.get("id") or "")
        counterpart_source_id = str(counterpart_tx.get("id") or "")
        if not primary_source_id or not counterpart_source_id:
            return
        if checkpoint.get_mapping("transaction", primary_source_id) and checkpoint.get_mapping(
            "transaction", counterpart_source_id
        ):
            return

        payload, error = self._build_transaction_payload(
            tx=primary_tx,
            source_accounts_by_id=source_accounts_by_id,
            source_payees_by_id=source_payees_by_id,
            account_map=account_map,
            category_map=category_map,
            payee_map=payee_map,
            include_import_id=False,
            report=report,
        )
        if payload is None:
            self._record_issue(
                report,
                "errors",
                "transaction",
                primary_source_id,
                error or "payload build failed",
                details=self._transaction_issue_details(primary_tx),
            )
            return
        payload.pop("import_id", None)

        try:
            response_data = self.dest_client.create_transactions(self.dest_plan_id, payload)
            saved_transactions = self._extract_saved_transactions(response_data)

            primary_dest_account_id = account_map.get(primary_tx.get("account_id"))
            counterpart_dest_account_id = account_map.get(counterpart_tx.get("account_id"))
            primary_dest_id: Optional[str] = None
            counterpart_dest_id: Optional[str] = None

            for saved in saved_transactions:
                saved_id = saved.get("id")
                saved_account_id = saved.get("account_id")
                if not saved_id:
                    continue
                saved_id_str = str(saved_id)
                if primary_dest_account_id and saved_account_id == primary_dest_account_id:
                    primary_dest_id = saved_id_str
                if counterpart_dest_account_id and saved_account_id == counterpart_dest_account_id:
                    counterpart_dest_id = saved_id_str

            if primary_dest_id is None and saved_transactions:
                candidate = saved_transactions[0].get("id")
                if candidate:
                    primary_dest_id = str(candidate)

            if primary_dest_id and counterpart_dest_id is None:
                try:
                    fetched = self.dest_client.get_transaction(self.dest_plan_id, primary_dest_id).get(
                        "transaction", {}
                    )
                    linked_dest_id = fetched.get("transfer_transaction_id")
                    if linked_dest_id:
                        counterpart_dest_id = str(linked_dest_id)
                except Exception:  # noqa: BLE001
                    counterpart_dest_id = None

            if primary_dest_id:
                checkpoint.set_mapping("transaction", primary_source_id, primary_dest_id)
            if counterpart_dest_id:
                checkpoint.set_mapping("transaction", counterpart_source_id, counterpart_dest_id)

            if not primary_dest_id or not counterpart_dest_id:
                missing_source_id = primary_source_id if not primary_dest_id else counterpart_source_id
                self._record_issue(
                    report,
                    "errors",
                    "transaction",
                    missing_source_id,
                    "could not resolve destination transfer transaction ID after create",
                    details={
                        "primary_source_transaction_id": primary_source_id,
                        "counterpart_source_transaction_id": counterpart_source_id,
                        "primary_dest_transaction_id": primary_dest_id,
                        "counterpart_dest_transaction_id": counterpart_dest_id,
                    },
                )
                return

            self._sync_transfer_pair_cleared_status(
                primary_source_tx=primary_tx,
                counterpart_source_tx=counterpart_tx,
                primary_dest_id=primary_dest_id,
                counterpart_dest_id=counterpart_dest_id,
                report=report,
            )
        except Exception as error_obj:  # noqa: BLE001
            self._record_issue(
                report,
                "errors",
                "transaction",
                primary_source_id,
                f"single transaction failed: {summarize_exception(error_obj)}",
                details=self._transaction_issue_details(primary_tx),
            )

    def _sync_transfer_pair_cleared_status(
        self,
        primary_source_tx: Dict[str, Any],
        counterpart_source_tx: Dict[str, Any],
        primary_dest_id: str,
        counterpart_dest_id: str,
        report: Dict[str, Any],
    ) -> None:
        desired = [
            (primary_dest_id, primary_source_tx.get("cleared"), str(primary_source_tx.get("id") or "")),
            (
                counterpart_dest_id,
                counterpart_source_tx.get("cleared"),
                str(counterpart_source_tx.get("id") or ""),
            ),
        ]
        valid_cleared = {"cleared", "uncleared", "reconciled"}
        for dest_id, cleared_value, source_id in desired:
            if not isinstance(cleared_value, str) or cleared_value not in valid_cleared:
                continue
            try:
                self.dest_client.update_transaction(
                    self.dest_plan_id,
                    dest_id,
                    {"cleared": cleared_value},
                )
            except Exception as error_obj:  # noqa: BLE001
                self._record_issue(
                    report,
                    "warnings",
                    "transaction",
                    source_id or None,
                    f"failed to sync transfer cleared status: {summarize_exception(error_obj)}",
                    details={
                        "dest_transaction_id": dest_id,
                        "desired_cleared": cleared_value,
                    },
                )

    def _is_probable_batch_size_error(self, error: Exception) -> bool:
        rendered = summarize_exception(error).lower()
        if isinstance(error, YNABApiError):
            if int(getattr(error, "status_code", 0) or 0) == 413:
                return True
            body = error.body if isinstance(error.body, dict) else {}
            payload = ""
            if isinstance(body.get("error"), dict):
                err = body["error"]
                payload = f"{err.get('name', '')} {err.get('detail', '')}".lower()
            combined = f"{rendered} {payload}".strip()
            if "data_limit_reached" in combined:
                return True
            if int(getattr(error, "status_code", 0) or 0) in (400, 403):
                for token in ("too many", "maximum", "max ", "payload", "data limit", "data_limit"):
                    if token in combined:
                        return True
            return False
        for token in ("payload too large", "too many", "maximum", "data limit", "data_limit"):
            if token in rendered:
                return True
        return False

    def _process_starting_balance_transaction(
        self,
        tx: Dict[str, Any],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        account_map: Dict[str, str],
        category_map: Dict[str, str],
        payee_map: Dict[str, str],
        existing_import_map: Dict[str, str],
    ) -> None:
        source_id = tx.get("id")
        if not source_id:
            return
        import_id = deterministic_import_id(self.source_plan_id, source_id)
        if import_id in existing_import_map:
            checkpoint.set_mapping("transaction", source_id, existing_import_map[import_id])
            return

        payload, error = self._build_transaction_payload(
            tx=tx,
            source_accounts_by_id=source_accounts_by_id,
            source_payees_by_id=source_payees_by_id,
            account_map=account_map,
            category_map=category_map,
            payee_map=payee_map,
            include_import_id=True,
            report=report,
        )
        if payload is None:
            self._record_issue(
                report,
                "errors",
                "transaction",
                source_id,
                error or "payload build failed",
                details=self._transaction_issue_details(tx),
            )
            return

        payload["import_id"] = import_id
        self._delete_captured_auto_starting_balance_for_source_transaction(
            tx=tx,
            checkpoint=checkpoint,
            report=report,
        )

        try:
            response_data = self.dest_client.create_transactions(self.dest_plan_id, payload)
            self._apply_transaction_batch_response(
                checkpoint=checkpoint,
                response_data=response_data,
                source_by_import={import_id: source_id},
                existing_import_map=existing_import_map,
                report=report,
            )
        except Exception as error_obj:  # noqa: BLE001
            self._record_issue(
                report,
                "errors",
                "transaction",
                source_id,
                f"single transaction failed: {summarize_exception(error_obj)}",
                details=self._transaction_issue_details(tx),
            )

    def _delete_captured_auto_starting_balance_for_source_transaction(
        self,
        tx: Dict[str, Any],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        source_tx_id = tx.get("id")
        source_account_id = tx.get("account_id")
        if not source_tx_id or not source_account_id:
            return
        candidate_id = self._resolve_auto_starting_balance_candidate_id(
            checkpoint=checkpoint,
            source_tx_id=source_tx_id,
            source_account_id=source_account_id,
        )
        if not candidate_id:
            self._record_issue(
                report,
                "warnings",
                "transaction",
                source_tx_id,
                "no captured destination auto Starting Balance transaction found; keeping fallback behavior",
                details={
                    "source_account_id": source_account_id,
                    "starting_balance": True,
                },
            )
            self._increment_system_counter(report, "starting_balance_fallback_no_candidate")
            return

        deleted_ids = checkpoint.get_metadata("deleted_auto_starting_balance_candidates", [])
        if not isinstance(deleted_ids, list):
            deleted_ids = []
        if candidate_id in deleted_ids:
            return

        try:
            self.dest_client.delete_transaction(self.dest_plan_id, candidate_id)
            deleted_ids.append(candidate_id)
            checkpoint.set_metadata("deleted_auto_starting_balance_candidates", deleted_ids)
            self._increment_system_counter(report, "deleted_auto_starting_balance_transactions")
            checkpoint.add_event(
                "INFO",
                f"deleted auto starting-balance transaction {candidate_id} before replaying {source_tx_id}",
            )
        except YNABApiError as error:
            if error.status_code == 404:
                deleted_ids.append(candidate_id)
                checkpoint.set_metadata("deleted_auto_starting_balance_candidates", deleted_ids)
                self._record_issue(
                    report,
                    "warnings",
                    "transaction",
                    source_tx_id,
                    f"captured auto Starting Balance transaction {candidate_id} already absent; continuing",
                    details={
                        "source_account_id": source_account_id,
                        "candidate_transaction_id": candidate_id,
                        "starting_balance": True,
                    },
                )
                return
            self._increment_system_counter(report, "starting_balance_delete_failures")
            self._record_issue(
                report,
                "warnings",
                "transaction",
                source_tx_id,
                (
                    "failed to delete captured auto Starting Balance transaction "
                    f"{candidate_id}: {summarize_exception(error)}"
                ),
                details={
                    "source_account_id": source_account_id,
                    "candidate_transaction_id": candidate_id,
                    "starting_balance": True,
                },
            )

    def _resolve_auto_starting_balance_candidate_id(
        self,
        checkpoint: CheckpointStore,
        source_tx_id: str,
        source_account_id: str,
    ) -> Optional[str]:
        assigned = checkpoint.get_metadata("starting_balance_tx_candidate_map", {})
        if not isinstance(assigned, dict):
            assigned = {}
        if source_tx_id in assigned:
            candidate_id = assigned.get(source_tx_id)
            if candidate_id:
                return str(candidate_id)
            return None

        candidates_by_account = checkpoint.get_metadata("auto_starting_balance_candidates", {})
        if not isinstance(candidates_by_account, dict):
            candidates_by_account = {}
        used_ids = {str(value) for value in assigned.values() if value}
        account_candidates = candidates_by_account.get(source_account_id, [])
        selected: Optional[str] = None
        if isinstance(account_candidates, list):
            for candidate_id in account_candidates:
                if not candidate_id:
                    continue
                candidate_id_str = str(candidate_id)
                if candidate_id_str in used_ids:
                    continue
                selected = candidate_id_str
                break
        assigned[source_tx_id] = selected or ""
        checkpoint.set_metadata("starting_balance_tx_candidate_map", assigned)
        return selected

    def _apply_transaction_batch_response(
        self,
        checkpoint: CheckpointStore,
        response_data: Dict[str, Any],
        source_by_import: Dict[str, str],
        existing_import_map: Dict[str, str],
        report: Dict[str, Any],
    ) -> None:
        created_objects: List[Dict[str, Any]] = []
        if isinstance(response_data.get("transaction"), dict):
            created_objects = [response_data["transaction"]]
        elif isinstance(response_data.get("transactions"), list):
            created_objects = [item for item in response_data["transactions"] if isinstance(item, dict)]

        for created in created_objects:
            import_id = created.get("import_id")
            dest_id = created.get("id")
            if import_id and dest_id and import_id in source_by_import:
                source_id = source_by_import[import_id]
                checkpoint.set_mapping("transaction", source_id, dest_id)
                existing_import_map[import_id] = dest_id

        duplicates = response_data.get("duplicate_import_ids", [])
        if isinstance(duplicates, list):
            for import_id in duplicates:
                source_id = source_by_import.get(import_id)
                dest_id = existing_import_map.get(import_id)
                if source_id and dest_id:
                    checkpoint.set_mapping("transaction", source_id, dest_id)

        unresolved = [
            import_id
            for import_id, source_id in source_by_import.items()
            if checkpoint.get_mapping("transaction", source_id) is None
        ]
        if unresolved:
            existing_import_map.update(self._load_destination_transaction_import_map())
            for import_id in unresolved:
                source_id = source_by_import.get(import_id)
                dest_id = existing_import_map.get(import_id)
                if source_id and dest_id:
                    checkpoint.set_mapping("transaction", source_id, dest_id)
                elif source_id:
                    self._record_issue(
                        report,
                        "errors",
                        "transaction",
                        source_id,
                        "could not resolve destination transaction ID after create",
                        details={"import_id": import_id},
                    )

    def _load_destination_transaction_import_map(self) -> Dict[str, str]:
        transaction_map: Dict[str, str] = {}
        destination_transactions = clean_deleted(self.dest_client.get_transactions(self.dest_plan_id).get("transactions", []))
        for transaction in destination_transactions:
            import_id = transaction.get("import_id")
            transaction_id = transaction.get("id")
            if import_id and transaction_id:
                transaction_map[str(import_id)] = str(transaction_id)
        return transaction_map

    def _augment_payee_map_by_name(
        self,
        source_payees: List[Dict[str, Any]],
        existing_payee_map: Dict[str, str],
    ) -> Dict[str, str]:
        merged = dict(existing_payee_map)
        destination_payees = clean_deleted(self.dest_client.get_payees(self.dest_plan_id).get("payees", []))
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for payee in destination_payees:
            if payee.get("transfer_account_id"):
                continue
            destination_id = payee.get("id")
            destination_name = payee.get("name")
            if not destination_id or not isinstance(destination_name, str):
                continue
            by_name.setdefault(destination_name, []).append(payee)

        used_destination_payee_ids: Set[str] = {
            str(dest_id) for dest_id in merged.values() if dest_id
        }

        for source_payee in source_payees:
            source_id = source_payee.get("id")
            if not source_id or source_id in merged:
                continue
            if source_payee.get("transfer_account_id"):
                continue
            source_name = source_payee.get("name")
            if not isinstance(source_name, str):
                continue
            matches = by_name.get(source_name, [])
            if len(matches) == 1:
                matched_dest_id = str(matches[0].get("id"))
                if matched_dest_id in used_destination_payee_ids:
                    continue
                merged[source_id] = matched_dest_id
                used_destination_payee_ids.add(matched_dest_id)
        return merged

    def _create_scheduled_transactions(
        self,
        source_scheduled_transactions: List[Dict[str, Any]],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
    ) -> None:
        cursor_name = "scheduled_transactions_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        scheduled_transactions = sorted(
            source_scheduled_transactions,
            key=lambda item: (item.get("date_next") or item.get("date_first") or "", item.get("id") or ""),
        )

        account_map = checkpoint.get_mapping_dict("account")
        category_map = checkpoint.get_mapping_dict("category")
        payee_map = checkpoint.get_mapping_dict("payee")
        reference_date = date.fromisoformat(checkpoint.get_metadata("apply_reference_date"))

        for idx in range(cursor, len(scheduled_transactions)):
            self._log_progress("apply", "scheduled_transactions", idx + 1, len(scheduled_transactions))
            scheduled = scheduled_transactions[idx]
            if is_deleted(scheduled):
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            source_id = scheduled.get("id")
            if not source_id:
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            if checkpoint.get_mapping("scheduled_transaction", source_id):
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            payload, error = self._build_scheduled_payload(
                scheduled=scheduled,
                source_accounts_by_id=source_accounts_by_id,
                source_payees_by_id=source_payees_by_id,
                account_map=account_map,
                category_map=category_map,
                payee_map=payee_map,
                reference_date=reference_date,
                report=report,
            )
            if payload is None:
                self._record_issue(
                    report,
                    "warnings",
                    "scheduled_transaction",
                    source_id,
                    error or "unsupported scheduled transaction",
                    details=self._scheduled_issue_details(scheduled),
                )
                self._exclude_entity(
                    checkpoint,
                    entity="scheduled_transaction",
                    source_id=source_id,
                    reason=error or "unsupported",
                )
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            try:
                created = self.dest_client.create_scheduled_transaction(self.dest_plan_id, payload).get(
                    "scheduled_transaction", {}
                )
                dest_id = created.get("id")
                if not dest_id:
                    raise RuntimeError("scheduled transaction response missing id")
                checkpoint.set_mapping("scheduled_transaction", source_id, dest_id)
                checkpoint.add_event("INFO", f"scheduled_transaction mapped {source_id} -> {dest_id}")
            except Exception as error_obj:  # noqa: BLE001
                self._record_issue(
                    report,
                    "errors",
                    "scheduled_transaction",
                    source_id,
                    f"failed to create scheduled transaction: {summarize_exception(error_obj)}",
                    details=self._scheduled_issue_details(scheduled),
                )
                checkpoint.add_event(
                    "ERROR",
                    f"scheduled transaction creation failed for {source_id}: {summarize_exception(error_obj)}",
                )
            finally:
                checkpoint.set_cursor(cursor_name, idx + 1)

    def _apply_month_budgets(
        self,
        month_category_budgets: List[Dict[str, Any]],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        source_internal_category_ids: Set[str],
    ) -> None:
        cursor_name = "month_budgets_idx"
        cursor = checkpoint.get_cursor(cursor_name)
        category_map = checkpoint.get_mapping_dict("category")
        entries = sorted(month_category_budgets, key=lambda item: (item["month"], item["category_id"]))
        for idx in range(cursor, len(entries)):
            self._log_progress("apply", "month_budgets", idx + 1, len(entries))
            entry = entries[idx]
            source_category_id = entry.get("category_id")
            month = entry.get("month")
            source_key = f"{month}:{source_category_id}"
            if source_category_id in source_internal_category_ids:
                self._exclude_entity(
                    checkpoint=checkpoint,
                    entity="month_budget",
                    source_id=source_key,
                    reason="internal system category month budget skipped",
                )
                self._increment_system_counter(report, "skipped_internal_month_budget_patches")
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue
            dest_category_id = category_map.get(source_category_id)
            if not dest_category_id:
                self._record_issue(
                    report,
                    "warnings",
                    "month_budget",
                    source_key,
                    "missing category mapping; excluding from parity",
                    details={
                        "month": month,
                        "source_category_id": source_category_id,
                        "budgeted": _safe_int(entry.get("budgeted")),
                    },
                )
                self._exclude_entity(
                    checkpoint,
                    entity="month_budget",
                    source_id=source_key,
                    reason="missing category mapping",
                )
                checkpoint.set_cursor(cursor_name, idx + 1)
                continue

            try:
                self.dest_client.patch_month_category(
                    self.dest_plan_id,
                    month=month,
                    category_id=dest_category_id,
                    budgeted=_safe_int(entry.get("budgeted")),
                )
            except YNABApiError as error:
                if int(getattr(error, "status_code", 0) or 0) == 404:
                    self._record_issue(
                        report,
                        "warnings",
                        "month_budget",
                        source_key,
                        "destination month/category resource not found; excluding from parity",
                        details={
                            "month": month,
                            "source_category_id": source_category_id,
                            "dest_category_id": dest_category_id,
                            "budgeted": _safe_int(entry.get("budgeted")),
                        },
                    )
                    self._exclude_entity(
                        checkpoint,
                        entity="month_budget",
                        source_id=source_key,
                        reason="destination month/category resource not found",
                    )
                    self._increment_system_counter(report, "skipped_missing_month_budget_targets")
                    continue
                self._record_issue(
                    report,
                    "errors",
                    "month_budget",
                    source_key,
                    f"failed to patch month budget: {summarize_exception(error)}",
                    details={
                        "month": month,
                        "source_category_id": source_category_id,
                        "dest_category_id": dest_category_id,
                        "budgeted": _safe_int(entry.get("budgeted")),
                    },
                )
            finally:
                checkpoint.set_cursor(cursor_name, idx + 1)

    # ---------- Payload transforms ----------
    def _build_transaction_payload(
        self,
        tx: Dict[str, Any],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        account_map: Dict[str, str],
        category_map: Dict[str, str],
        payee_map: Dict[str, str],
        include_import_id: bool,
        report: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if is_deleted(tx):
            return None, "deleted transaction"
        source_id = tx.get("id")
        source_account_id = tx.get("account_id")
        if not source_account_id:
            return None, "transaction missing account_id"
        dest_account_id = account_map.get(source_account_id)
        if not dest_account_id:
            return None, f"missing destination account mapping for {source_account_id}"

        payload: Dict[str, Any] = {
            "account_id": dest_account_id,
            "date": tx.get("date"),
            "amount": _safe_int(tx.get("amount")),
            "approved": bool(tx.get("approved", False)),
        }

        if tx.get("memo") is not None:
            payload["memo"] = tx.get("memo")
        if tx.get("cleared") is not None:
            payload["cleared"] = tx.get("cleared")
        raw_flag_color = tx.get("flag_color")
        if raw_flag_color is not None:
            normalized_flag_color = _normalize_flag_color(raw_flag_color)
            if normalized_flag_color is not None:
                payload["flag_color"] = normalized_flag_color
            else:
                self._record_issue(
                    report,
                    "warnings",
                    "transaction",
                    source_id,
                    f"invalid or empty flag_color '{raw_flag_color}' omitted",
                    details={
                        "account_id": source_account_id,
                        "date": tx.get("date"),
                    },
                )

        transfer_account_id = tx.get("transfer_account_id")
        source_payee_id = tx.get("payee_id")
        source_category_id = tx.get("category_id")

        if transfer_account_id:
            transfer_account = source_accounts_by_id.get(transfer_account_id)
            transfer_source_payee = transfer_account.get("transfer_payee_id") if transfer_account else None
            payee_id_candidate = None
            if source_payee_id:
                payee_id_candidate = payee_map.get(source_payee_id)
            if not payee_id_candidate and transfer_source_payee:
                payee_id_candidate = payee_map.get(transfer_source_payee)
            if not payee_id_candidate:
                return None, f"missing transfer payee mapping for transfer_account_id={transfer_account_id}"
            payload["payee_id"] = payee_id_candidate
            if source_category_id:
                mapped_category_id = category_map.get(source_category_id)
                if mapped_category_id:
                    payload["category_id"] = mapped_category_id
                else:
                    self._record_issue(
                        report,
                        "warnings",
                        "transaction",
                        source_id,
                        f"missing category mapping for {source_category_id}; setting null category",
                        details={
                            "account_id": source_account_id,
                            "transfer_account_id": transfer_account_id,
                            "date": tx.get("date"),
                            "source_category_id": source_category_id,
                        },
                    )
                    payload["category_id"] = None
            else:
                payload["category_id"] = None
        else:
            if source_payee_id:
                mapped_payee_id = payee_map.get(source_payee_id)
                if mapped_payee_id:
                    payload["payee_id"] = mapped_payee_id
                else:
                    source_payee = source_payees_by_id.get(source_payee_id)
                    if source_payee and source_payee.get("name"):
                        payload["payee_name"] = source_payee["name"]

            if source_category_id:
                mapped_category_id = category_map.get(source_category_id)
                if mapped_category_id:
                    payload["category_id"] = mapped_category_id
                else:
                    self._record_issue(
                        report,
                        "warnings",
                        "transaction",
                        source_id,
                        f"missing category mapping for {source_category_id}; setting null category",
                        details={
                            "account_id": source_account_id,
                            "date": tx.get("date"),
                            "source_category_id": source_category_id,
                        },
                    )
                    payload["category_id"] = None
            else:
                payload["category_id"] = None

        subtransactions = clean_deleted(tx.get("subtransactions", []))
        if subtransactions:
            payload["category_id"] = None
            transformed_subs: List[Dict[str, Any]] = []
            for sub in subtransactions:
                transformed_sub: Dict[str, Any] = {
                    "amount": _safe_int(sub.get("amount")),
                }
                if sub.get("memo") is not None:
                    transformed_sub["memo"] = sub.get("memo")
                sub_payee_id = sub.get("payee_id")
                if sub_payee_id:
                    mapped_sub_payee_id = payee_map.get(sub_payee_id)
                    if mapped_sub_payee_id:
                        transformed_sub["payee_id"] = mapped_sub_payee_id
                    else:
                        payee = source_payees_by_id.get(sub_payee_id)
                        if payee and payee.get("name"):
                            transformed_sub["payee_name"] = payee["name"]

                sub_category_id = sub.get("category_id")
                if sub_category_id:
                    mapped_sub_category_id = category_map.get(sub_category_id)
                    if mapped_sub_category_id:
                        transformed_sub["category_id"] = mapped_sub_category_id
                    else:
                        transformed_sub["category_id"] = None
                else:
                    transformed_sub["category_id"] = None
                transformed_subs.append(transformed_sub)
            payload["subtransactions"] = transformed_subs

        if include_import_id and source_id and not tx.get("transfer_account_id") and not tx.get(
            "transfer_transaction_id"
        ):
            payload["import_id"] = deterministic_import_id(self.source_plan_id, source_id)
        return payload, None

    def _build_source_system_entities(
        self,
        source_category_groups: List[Dict[str, Any]],
        source_categories: List[Dict[str, Any]],
        report: Dict[str, Any],
    ) -> Dict[str, Any]:
        internal_group_ids = sorted(
            str(group["id"])
            for group in source_category_groups
            if group.get("name") == INTERNAL_MASTER_CATEGORY_GROUP_NAME and group.get("id")
        )
        hidden_group_ids = sorted(
            str(group["id"])
            for group in source_category_groups
            if group.get("name") == HIDDEN_CATEGORIES_GROUP_NAME and group.get("id")
        )
        credit_card_payments_group_ids = sorted(
            str(group["id"])
            for group in source_category_groups
            if group.get("name") == CREDIT_CARD_PAYMENTS_GROUP_NAME and group.get("id")
        )

        internal_group_id_set = set(internal_group_ids)
        credit_card_group_id_set = set(credit_card_payments_group_ids)

        inflow_category_ids = sorted(
            str(category["id"])
            for category in source_categories
            if category.get("name") == INTERNAL_INFLOW_CATEGORY_NAME
            and category.get("id")
            and category.get("category_group_id") in internal_group_id_set
        )
        uncategorized_category_ids = sorted(
            str(category["id"])
            for category in source_categories
            if category.get("name") == INTERNAL_UNCATEGORIZED_CATEGORY_NAME
            and category.get("id")
            and category.get("category_group_id") in internal_group_id_set
        )

        credit_card_payment_categories = []
        for category in source_categories:
            category_id = category.get("id")
            category_name = category.get("name")
            category_group_id = category.get("category_group_id")
            if not category_id or not category_name:
                continue
            if category_group_id not in credit_card_group_id_set:
                continue
            credit_card_payment_categories.append(
                {
                    "id": str(category_id),
                    "name": str(category_name),
                    "category_group_id": str(category_group_id),
                }
            )
        credit_card_payment_categories.sort(key=lambda item: (item["name"], item["id"]))

        if len(internal_group_ids) > 1:
            self._append_warning(
                report,
                "source has multiple 'Internal Master Category' groups; all will map to destination system group."
            )
        if len(hidden_group_ids) > 1:
            self._append_warning(
                report,
                "source has multiple 'Hidden Categories' groups; all will map to destination system group."
            )
        if len(credit_card_payments_group_ids) > 1:
            self._append_warning(
                report,
                "source has multiple 'Credit Card Payments' groups; all will map to destination system group."
            )
        if len(inflow_category_ids) > 1:
            self._append_warning(
                report,
                "source has multiple 'Inflow: Ready to Assign' categories in internal group; all will map to destination inflow category."
            )
        if len(uncategorized_category_ids) > 1:
            self._append_warning(
                report,
                "source has multiple 'Uncategorized' categories in internal group; all will map to destination uncategorized category."
            )

        return {
            "internal_master_category_group_name": INTERNAL_MASTER_CATEGORY_GROUP_NAME,
            "hidden_categories_group_name": HIDDEN_CATEGORIES_GROUP_NAME,
            "credit_card_payments_group_name": CREDIT_CARD_PAYMENTS_GROUP_NAME,
            "internal_inflow_category_name": INTERNAL_INFLOW_CATEGORY_NAME,
            "internal_uncategorized_category_name": INTERNAL_UNCATEGORIZED_CATEGORY_NAME,
            "source_internal_master_category_group_ids": internal_group_ids,
            "source_hidden_category_group_ids": hidden_group_ids,
            "source_credit_card_payments_group_ids": credit_card_payments_group_ids,
            "source_internal_inflow_category_ids": inflow_category_ids,
            "source_internal_uncategorized_category_ids": uncategorized_category_ids,
            "source_credit_card_payment_categories": credit_card_payment_categories,
            "source_credit_card_payment_category_ids": [
                item["id"] for item in credit_card_payment_categories
            ],
        }

    def _resolve_and_map_destination_system_entities(
        self,
        source_system_entities: Dict[str, Any],
        checkpoint: CheckpointStore,
        report: Dict[str, Any],
        map_credit_card_payment_categories: bool = True,
    ) -> None:
        source_internal_group_ids = list(source_system_entities["source_internal_master_category_group_ids"])
        source_hidden_group_ids = list(source_system_entities["source_hidden_category_group_ids"])
        source_cc_group_ids = list(source_system_entities["source_credit_card_payments_group_ids"])
        source_inflow_ids = list(source_system_entities["source_internal_inflow_category_ids"])
        source_uncategorized_ids = list(source_system_entities["source_internal_uncategorized_category_ids"])
        source_cc_category_entries = list(source_system_entities["source_credit_card_payment_categories"])

        if (
            not source_internal_group_ids
            and not source_hidden_group_ids
            and not source_cc_group_ids
            and not source_inflow_ids
            and not source_uncategorized_ids
            and not source_cc_category_entries
        ):
            return

        destination_plan = self.dest_client.get_plan(self.dest_plan_id).get("plan", {})
        destination_groups = clean_deleted(destination_plan.get("category_groups", []))
        destination_categories = clean_deleted(destination_plan.get("categories", []))

        internal_group_name = source_system_entities["internal_master_category_group_name"]
        hidden_group_name = source_system_entities["hidden_categories_group_name"]
        cc_group_name = source_system_entities["credit_card_payments_group_name"]
        inflow_name = source_system_entities["internal_inflow_category_name"]
        uncategorized_name = source_system_entities["internal_uncategorized_category_name"]

        def resolve_destination_group(group_name: str, required: bool) -> Optional[str]:
            matches = [group for group in destination_groups if group.get("name") == group_name]
            if not required and not matches:
                return None
            if len(matches) != 1:
                raise RuntimeError(
                    f"destination must contain exactly one '{group_name}' group; found {len(matches)}"
                )
            return str(matches[0]["id"])

        destination_internal_group_id = resolve_destination_group(
            internal_group_name,
            required=bool(source_internal_group_ids or source_inflow_ids or source_uncategorized_ids),
        )
        destination_hidden_group_id = resolve_destination_group(
            hidden_group_name,
            required=bool(source_hidden_group_ids),
        )
        destination_cc_group_id = resolve_destination_group(
            cc_group_name,
            required=bool(source_cc_group_ids or source_cc_category_entries),
        )

        if destination_internal_group_id:
            for source_group_id in source_internal_group_ids:
                checkpoint.set_mapping("category_group", source_group_id, destination_internal_group_id)
        if destination_hidden_group_id:
            for source_group_id in source_hidden_group_ids:
                checkpoint.set_mapping("category_group", source_group_id, destination_hidden_group_id)
        if destination_cc_group_id:
            for source_group_id in source_cc_group_ids:
                checkpoint.set_mapping("category_group", source_group_id, destination_cc_group_id)

        if destination_internal_group_id:
            matching_inflow_categories = [
                category
                for category in destination_categories
                if category.get("name") == inflow_name
                and category.get("category_group_id") == destination_internal_group_id
            ]
            if source_inflow_ids and len(matching_inflow_categories) != 1:
                raise RuntimeError(
                    "destination must contain exactly one 'Inflow: Ready to Assign' category "
                    f"in '{internal_group_name}'; found {len(matching_inflow_categories)}"
                )
            if matching_inflow_categories:
                destination_inflow_category_id = str(matching_inflow_categories[0]["id"])
                for source_category_id in source_inflow_ids:
                    checkpoint.set_mapping("category", source_category_id, destination_inflow_category_id)

            matching_uncategorized_categories = [
                category
                for category in destination_categories
                if category.get("name") == uncategorized_name
                and category.get("category_group_id") == destination_internal_group_id
            ]
            if source_uncategorized_ids and len(matching_uncategorized_categories) != 1:
                raise RuntimeError(
                    "destination must contain exactly one 'Uncategorized' category "
                    f"in '{internal_group_name}'; found {len(matching_uncategorized_categories)}"
                )
            if matching_uncategorized_categories:
                destination_uncategorized_category_id = str(matching_uncategorized_categories[0]["id"])
                for source_category_id in source_uncategorized_ids:
                    checkpoint.set_mapping("category", source_category_id, destination_uncategorized_category_id)

        if map_credit_card_payment_categories and source_cc_category_entries:
            if not destination_cc_group_id:
                raise RuntimeError(
                    "destination is missing required 'Credit Card Payments' category group for system category mapping"
                )
            destination_cc_categories_by_name: Dict[str, List[Dict[str, Any]]] = {}
            for category in destination_categories:
                if category.get("category_group_id") != destination_cc_group_id:
                    continue
                destination_cc_categories_by_name.setdefault(str(category.get("name")), []).append(category)
            for source_category in source_cc_category_entries:
                source_category_id = source_category.get("id")
                source_category_name = source_category.get("name")
                if not source_category_id or not source_category_name:
                    continue
                matches = destination_cc_categories_by_name.get(str(source_category_name), [])
                if len(matches) != 1:
                    raise RuntimeError(
                        "destination must contain exactly one credit-card payment category named "
                        f"'{source_category_name}' in '{cc_group_name}'; found {len(matches)}"
                    )
                checkpoint.set_mapping("category", str(source_category_id), str(matches[0]["id"]))

        def _mapped_count(entity: str, source_ids: Iterable[str]) -> int:
            count = 0
            for source_id in source_ids:
                if source_id and checkpoint.get_mapping(entity, str(source_id)):
                    count += 1
            return count

        source_cc_category_ids = [
            str(item.get("id"))
            for item in source_cc_category_entries
            if item.get("id")
        ]

        stats = report.setdefault("system_entity_stats", {})
        stats["mapped_internal_category_group_ids"] = _mapped_count(
            "category_group",
            source_internal_group_ids,
        )
        stats["mapped_hidden_category_group_ids"] = _mapped_count(
            "category_group",
            source_hidden_group_ids,
        )
        stats["mapped_credit_card_payments_group_ids"] = _mapped_count(
            "category_group",
            source_cc_group_ids,
        )
        stats["mapped_internal_inflow_category_ids"] = _mapped_count(
            "category",
            source_inflow_ids,
        )
        stats["mapped_internal_uncategorized_category_ids"] = _mapped_count(
            "category",
            source_uncategorized_ids,
        )
        stats["mapped_credit_card_payment_category_ids"] = _mapped_count(
            "category",
            source_cc_category_ids,
        )
        stats["mapped_system_category_group_ids"] = (
            stats["mapped_internal_category_group_ids"]
            + stats["mapped_hidden_category_group_ids"]
            + stats["mapped_credit_card_payments_group_ids"]
        )
        stats["mapped_system_category_ids"] = (
            stats["mapped_internal_inflow_category_ids"]
            + stats["mapped_internal_uncategorized_category_ids"]
            + stats["mapped_credit_card_payment_category_ids"]
        )

    def _increment_system_counter(self, report: Dict[str, Any], key: str) -> None:
        stats = report.setdefault("system_entity_stats", {})
        stats[key] = int(stats.get(key, 0)) + 1

    def _is_starting_balance_transaction(
        self,
        tx: Dict[str, Any],
        payees_by_id: Dict[str, Dict[str, Any]],
    ) -> bool:
        if not isinstance(tx, dict):
            return False
        if tx.get("transfer_account_id") or tx.get("transfer_transaction_id"):
            return False
        payee_name = self._resolve_transaction_payee_name(tx, payees_by_id)
        return normalize_name(payee_name) == normalize_name(STARTING_BALANCE_PAYEE_NAME)

    def _is_auto_starting_balance_candidate(
        self,
        tx: Dict[str, Any],
        destination_payees_by_id: Dict[str, Dict[str, Any]],
    ) -> bool:
        if not isinstance(tx, dict):
            return False
        if tx.get("transfer_account_id") or tx.get("transfer_transaction_id"):
            return False
        if _safe_int(tx.get("amount")) != 0:
            return False
        if tx.get("import_id"):
            return False
        payee_name = self._resolve_transaction_payee_name(tx, destination_payees_by_id)
        return normalize_name(payee_name) == normalize_name(STARTING_BALANCE_PAYEE_NAME)

    def _resolve_transaction_payee_name(
        self,
        tx: Dict[str, Any],
        payees_by_id: Dict[str, Dict[str, Any]],
    ) -> str:
        payee_name = tx.get("payee_name")
        if isinstance(payee_name, str) and payee_name.strip():
            return payee_name.strip()
        payee_id = tx.get("payee_id")
        if not payee_id:
            return ""
        payee = payees_by_id.get(payee_id) or payees_by_id.get(str(payee_id))
        if not isinstance(payee, dict):
            return ""
        raw_name = payee.get("name")
        if not isinstance(raw_name, str):
            return ""
        return raw_name.strip()

    def _build_scheduled_payload(
        self,
        scheduled: Dict[str, Any],
        source_accounts_by_id: Dict[str, Dict[str, Any]],
        source_payees_by_id: Dict[str, Dict[str, Any]],
        account_map: Dict[str, str],
        category_map: Dict[str, str],
        payee_map: Dict[str, str],
        reference_date: date,
        report: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if is_deleted(scheduled):
            return None, "deleted scheduled transaction"
        if clean_deleted(scheduled.get("subtransactions", [])):
            return None, "split scheduled transactions are not writable via API"

        source_account_id = scheduled.get("account_id")
        if not source_account_id:
            return None, "scheduled transaction missing account_id"
        dest_account_id = account_map.get(source_account_id)
        if not dest_account_id:
            return None, f"missing destination account mapping for {source_account_id}"

        raw_date = scheduled.get("date_next") or scheduled.get("date_first")
        if not raw_date:
            return None, "scheduled transaction missing date_next/date_first"
        try:
            parsed_date = date.fromisoformat(raw_date)
        except ValueError:
            return None, f"invalid scheduled transaction date {raw_date}"

        # API create endpoint expects a future date; shift only when required and track warning.
        if parsed_date <= reference_date:
            shifted_date = reference_date + timedelta(days=1)
            self._record_issue(
                report,
                "warnings",
                "scheduled_transaction",
                scheduled.get("id"),
                f"scheduled date shifted from {parsed_date.isoformat()} to {shifted_date.isoformat()}",
                details={
                    "account_id": source_account_id,
                    "date_next": scheduled.get("date_next"),
                    "date_first": scheduled.get("date_first"),
                },
            )
            parsed_date = shifted_date

        payload: Dict[str, Any] = {
            "account_id": dest_account_id,
            "date": parsed_date.isoformat(),
            "amount": _safe_int(scheduled.get("amount")),
            "frequency": scheduled.get("frequency"),
        }

        if scheduled.get("memo") is not None:
            payload["memo"] = scheduled.get("memo")
        raw_flag_color = scheduled.get("flag_color")
        if raw_flag_color is not None:
            normalized_flag_color = _normalize_flag_color(raw_flag_color)
            if normalized_flag_color is not None:
                payload["flag_color"] = normalized_flag_color
            else:
                self._record_issue(
                    report,
                    "warnings",
                    "scheduled_transaction",
                    scheduled.get("id"),
                    f"invalid or empty flag_color '{raw_flag_color}' omitted",
                    details={
                        "account_id": source_account_id,
                        "date_next": scheduled.get("date_next"),
                        "date_first": scheduled.get("date_first"),
                    },
                )

        transfer_account_id = scheduled.get("transfer_account_id")
        source_payee_id = scheduled.get("payee_id")
        source_category_id = scheduled.get("category_id")
        if transfer_account_id:
            transfer_account = source_accounts_by_id.get(transfer_account_id)
            transfer_source_payee = transfer_account.get("transfer_payee_id") if transfer_account else None
            payee_id_candidate = None
            if source_payee_id:
                payee_id_candidate = payee_map.get(source_payee_id)
            if not payee_id_candidate and transfer_source_payee:
                payee_id_candidate = payee_map.get(transfer_source_payee)
            if not payee_id_candidate:
                return None, f"missing transfer payee mapping for transfer_account_id={transfer_account_id}"
            payload["payee_id"] = payee_id_candidate
            payload["category_id"] = None
        else:
            if source_payee_id:
                mapped_payee_id = payee_map.get(source_payee_id)
                if mapped_payee_id:
                    payload["payee_id"] = mapped_payee_id
                else:
                    payee = source_payees_by_id.get(source_payee_id)
                    if payee and payee.get("name"):
                        payload["payee_name"] = payee["name"]
            if source_category_id:
                mapped_category_id = category_map.get(source_category_id)
                if mapped_category_id:
                    payload["category_id"] = mapped_category_id
                else:
                    payload["category_id"] = None
            else:
                payload["category_id"] = None

        return payload, None

    # ---------- Normalize destination entities for verify ----------
    def _normalize_destination_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {
            "account_id": tx.get("account_id"),
            "date": tx.get("date"),
            "amount": _safe_int(tx.get("amount")),
            "approved": bool(tx.get("approved", False)),
            "import_id": tx.get("import_id"),
            "category_id": tx.get("category_id"),
            "payee_id": tx.get("payee_id"),
        }
        if tx.get("memo") is not None:
            normalized["memo"] = tx.get("memo")
        if tx.get("cleared") is not None:
            normalized["cleared"] = tx.get("cleared")
        if tx.get("flag_color") is not None:
            normalized["flag_color"] = tx.get("flag_color")
        if tx.get("payee_name") is not None and tx.get("payee_id") is None:
            normalized["payee_name"] = tx.get("payee_name")

        subtransactions = clean_deleted(tx.get("subtransactions", []))
        if subtransactions:
            normalized["category_id"] = None
            normalized["subtransactions"] = []
            for sub in subtransactions:
                sub_norm = {
                    "amount": _safe_int(sub.get("amount")),
                    "category_id": sub.get("category_id"),
                }
                if sub.get("memo") is not None:
                    sub_norm["memo"] = sub.get("memo")
                if sub.get("payee_id") is not None:
                    sub_norm["payee_id"] = sub.get("payee_id")
                if sub.get("payee_name") is not None and sub.get("payee_id") is None:
                    sub_norm["payee_name"] = sub.get("payee_name")
                normalized["subtransactions"].append(sub_norm)
        return normalized

    def _normalize_destination_scheduled(self, scheduled: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {
            "account_id": scheduled.get("account_id"),
            "date": scheduled.get("date_next") or scheduled.get("date_first"),
            "amount": _safe_int(scheduled.get("amount")),
            "frequency": scheduled.get("frequency"),
            "category_id": scheduled.get("category_id"),
            "payee_id": scheduled.get("payee_id"),
        }
        if scheduled.get("memo") is not None:
            normalized["memo"] = scheduled.get("memo")
        if scheduled.get("flag_color") is not None:
            normalized["flag_color"] = scheduled.get("flag_color")
        if scheduled.get("payee_name") is not None and scheduled.get("payee_id") is None:
            normalized["payee_name"] = scheduled.get("payee_name")
        return normalized

    # ---------- Helpers ----------
    @staticmethod
    def _normalize_issue_details(details: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(details, dict):
            return {}
        normalized: Dict[str, Any] = {}
        for raw_key, raw_value in details.items():
            key = str(raw_key).strip()
            if not key or raw_value is None:
                continue
            if isinstance(raw_value, bool):
                normalized[key] = raw_value
                continue
            if isinstance(raw_value, (int, float)):
                normalized[key] = raw_value
                continue
            if isinstance(raw_value, str):
                value = raw_value.strip()
                if value:
                    normalized[key] = value
                continue
            if isinstance(raw_value, list):
                compact_values = []
                for item in raw_value:
                    if item is None:
                        continue
                    compact = str(item).strip()
                    if compact:
                        compact_values.append(compact)
                if compact_values:
                    normalized[key] = compact_values
                continue
            normalized[key] = str(raw_value)
        return normalized

    @staticmethod
    def _render_issue_details(details: Dict[str, Any]) -> str:
        if not details:
            return ""
        parts = []
        for key in sorted(details.keys()):
            value = details[key]
            if isinstance(value, list):
                rendered = ",".join(str(item) for item in value)
            else:
                rendered = str(value)
            parts.append(f"{key}={rendered}")
        return ", ".join(parts)

    @staticmethod
    def _transaction_issue_details(tx: Dict[str, Any]) -> Dict[str, Any]:
        details: Dict[str, Any] = {}
        for field in ("account_id", "date", "payee_id", "category_id", "transfer_account_id"):
            value = tx.get(field)
            if value:
                details[field] = value
        if tx.get("amount") is not None:
            details["amount"] = _safe_int(tx.get("amount"))
        return details

    @staticmethod
    def _scheduled_issue_details(scheduled: Dict[str, Any]) -> Dict[str, Any]:
        details: Dict[str, Any] = {}
        for field in ("account_id", "payee_id", "category_id", "frequency"):
            value = scheduled.get(field)
            if value:
                details[field] = value
        for field in ("date_next", "date_first"):
            value = scheduled.get(field)
            if value:
                details[field] = value
        if scheduled.get("amount") is not None:
            details["amount"] = _safe_int(scheduled.get("amount"))
        return details

    def _exclude_entity(self, checkpoint: CheckpointStore, entity: str, source_id: str, reason: str) -> None:
        exclusions = checkpoint.get_metadata("exclusions", [])
        candidate = {"entity": entity, "source_id": source_id, "reason": reason}
        if candidate not in exclusions:
            exclusions.append(candidate)
            checkpoint.set_metadata("exclusions", exclusions)

    def _append_warning(self, report: Dict[str, Any], message: str) -> None:
        report.setdefault("warnings", []).append(message)
        self.logger.warning(message)

    def _record_issue(
        self,
        report: Dict[str, Any],
        section: str,
        entity: str,
        source_id: Optional[str],
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        issue = {
            "entity": entity,
            "source_id": source_id,
            "message": message,
        }
        normalized_details = self._normalize_issue_details(details)
        if normalized_details:
            issue["details"] = normalized_details
        report.setdefault(section, []).append(issue)
        source_suffix = f" (source_id={source_id})" if source_id is not None else ""
        rendered_details = self._render_issue_details(normalized_details)
        if rendered_details:
            log_message = f"{entity}{source_suffix}: {message} | {rendered_details}"
        else:
            log_message = f"{entity}{source_suffix}: {message}"
        if section == "errors":
            self.logger.error(log_message)
        elif section == "warnings":
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)

    def _record_mismatch(
        self,
        report: Dict[str, Any],
        entity: str,
        source_id: str,
        message: str,
        expected: Optional[Dict[str, Any]] = None,
        actual: Optional[Dict[str, Any]] = None,
    ) -> None:
        mismatch = {
            "entity": entity,
            "source_id": source_id,
            "message": message,
        }
        if expected is not None:
            mismatch["expected"] = expected
        if actual is not None:
            mismatch["actual"] = actual
        report.setdefault("mismatches", []).append(mismatch)

    def _load_snapshot(self) -> Dict[str, Any]:
        if not self.paths.snapshot_path.exists():
            raise FileNotFoundError(
                f"snapshot file not found at {self.paths.snapshot_path}; run `plan` first"
            )
        snapshot = read_json(self.paths.snapshot_path)
        if snapshot.get("snapshot_schema_version") != SNAPSHOT_SCHEMA_VERSION:
            raise RuntimeError(
                "snapshot schema is incompatible with this migrator version; "
                "rerun `plan` using a clean workdir"
            )
        if snapshot.get("source_plan_id") != self.source_plan_id:
            raise RuntimeError("snapshot source_plan_id does not match provided source_plan_id")
        if snapshot.get("dest_plan_id") != self.dest_plan_id:
            raise RuntimeError("snapshot dest_plan_id does not match provided dest_plan_id")
        system_entities = snapshot.get("system_entities")
        if not isinstance(system_entities, dict):
            raise RuntimeError("snapshot is missing system_entities metadata; rerun `plan`")
        required_system_keys = [
            "source_internal_master_category_group_ids",
            "source_hidden_category_group_ids",
            "source_credit_card_payments_group_ids",
            "source_internal_inflow_category_ids",
            "source_internal_uncategorized_category_ids",
            "source_credit_card_payment_categories",
            "source_credit_card_payment_category_ids",
        ]
        missing_system_keys = [key for key in required_system_keys if key not in system_entities]
        if missing_system_keys:
            raise RuntimeError(
                "snapshot is missing required system_entities keys "
                f"({', '.join(sorted(missing_system_keys))}); rerun `plan`"
            )
        non_migratable_fields = snapshot.get("non_migratable_fields")
        if not isinstance(non_migratable_fields, dict):
            raise RuntimeError("snapshot is missing non_migratable_fields metadata; rerun `plan`")
        for key in ("account_notes", "month_notes"):
            if key not in non_migratable_fields:
                raise RuntimeError(
                    f"snapshot non_migratable_fields is missing '{key}'; rerun `plan`"
                )
        return snapshot
