# ynab-migrator

Note: This codebase is fully written by codex.

Resumable Python CLI to migrate YNAB data from one plan to another via the public API.

## Guarantees and Safety

- Uses a strict extraction filter: any entity with `deleted=true` (or tombstone-like status) is ignored and is never replayed.
- Uses checkpointed execution (`checkpoint.sqlite3`) so runs can resume safely after interruption.
- Uses deterministic transaction `import_id` values to support idempotent replay and duplicate protection.
- Uses automatic throttling for YNAB rate limits (`200 requests/hour` per token by default).
- Auto-maps source system-managed groups (`Internal Master Category`, `Hidden Categories`, `Credit Card Payments`) to destination system groups and does not recreate them.
- Auto-maps source internal categories (`Inflow: Ready to Assign`, `Uncategorized`) and credit-card payment categories by exact name in destination `Credit Card Payments`.
- Creates destination accounts with opening balance `0`; balances are established by replayed transactions (including `Starting Balance`).
- For unsupported source account types, first tries to reuse an existing destination account by exact name + exact type; if no unique eligible match exists, creates as `cash` (warning + report counters emitted).
- Captures and deletes destination auto-generated `Starting Balance` transactions, then recreates source `Starting Balance` transactions with original source date/amount/memo/flags.
- Records account notes and month notes as non-migratable read-only fields and reports them explicitly.
- Emits human-readable runtime logs to console and workdir log files (`plan.log`, `apply.log`, `verify.log`, `resume.log`), overwritten each run.
- Use `--verbose` for detailed technical API telemetry (request/retry/backoff diagnostics).
- Produces JSON reports for planning, apply, and verification phases.

## Install

```bash
pip install -e .
```

## Commands

All commands require both source and destination tokens and plan IDs.
Global options must be placed before the subcommand (`plan`/`apply`/`verify`/`resume`).
Use `--verbose` (before subcommand) to enable detailed technical logs.
For implementation-level behavior details, use `instructions.md` as the source of truth.

### 1) Build snapshot and preflight

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  plan
```

Outputs:

- `.ynab_migrator/snapshot.json`
- `.ynab_migrator/plan_report.json`
- `.ynab_migrator/plan.log`

### 2) Apply migration

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  apply
```

Outputs:

- `.ynab_migrator/checkpoint.sqlite3`
- `.ynab_migrator/apply_report.json`
- `.ynab_migrator/apply.log`

### 3) Verify parity for migrated subset

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  verify
```

Outputs:

- `.ynab_migrator/verify_report.json`
- `.ynab_migrator/verify.log`

### 4) Resume

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  resume
```

`resume` is an alias for `apply`, using existing checkpoint state.
It writes `.ynab_migrator/resume.log`.

## Notes

- Destination plan must already exist (YNAB API does not expose plan creation).
- Some API resources are read-only and cannot be recreated exactly (for example payee locations and money movements). These are reported.
- Split scheduled transactions are not writable via current API write schemas and are excluded with explicit warnings.
- Destination must contain exactly one of each required system-managed group: `Internal Master Category`, `Hidden Categories`, `Credit Card Payments`.
- Destination internal group must contain exactly one `Inflow: Ready to Assign` and one `Uncategorized`.
- Destination `Credit Card Payments` group must contain each source credit-card payment category by exact name.
- If you upgrade from an older migrator snapshot/checkpoint format, rerun from a fresh workdir (`plan` first).

## Troubleshooting

- Required arg error (`--source-token`, `--dest-token`, etc.): place global flags before the subcommand.
- Account create behavior: all source accounts first try exact destination name+type reuse; if no unique match exists, migrator creates a new account. Unsupported source account types still fall back to `cash` and log structured warnings in `apply_report.json`.
- System entity mapping error: destination must have exactly one `Internal Master Category`, `Hidden Categories`, `Credit Card Payments`, `Inflow: Ready to Assign`, and `Uncategorized`.
- Verify account type mismatch: use the same migrator version for `apply` and `verify`, and verify against the same snapshot/checkpoint pair.

## Report Quick Reference

- `plan_report.json`
  - `stats.source_counts_after_deleted_filter`: source entity counts used for migration.
  - `stats.estimated_apply_requests`: rough request volume and duration estimate.
  - `manual_action_items`: explicit pre/post apply operator actions to maximize migration parity.
- `apply_report.json`
  - `mapping_counts`: how many entities were successfully mapped.
  - `warnings` / `errors`: per-entity issues and non-fatal behavior changes.
  - `system_entity_stats.reused_accounts_by_name_type`: accounts mapped to existing destination accounts by exact name+type before creation.
  - `system_entity_stats.ambiguous_account_name_type_matches`: accounts where multiple exact destination name+type matches were found (new account created to avoid incorrect mapping).
  - `system_entity_stats.coerced_account_types`: number of accounts coerced to `cash`.
  - `system_entity_stats.reused_unsupported_accounts_by_name_type`: number of unsupported accounts mapped to existing destination accounts by exact name+type.
  - `system_entity_stats.ambiguous_unsupported_account_name_type_matches`: unsupported accounts with multiple exact destination name+type candidates (fallback to `cash`).
  - `system_entity_stats.skipped_missing_month_budget_targets`: month budget entries excluded when destination month/category targets are not available (`404`).
- `verify_report.json`
  - `passed`: overall parity status.
  - `mismatch_count` + `mismatches`: parity failures with expected/actual payloads.
