# AI Agent Instructions for `ynab-migrator`

This document is the operating manual for AI/code agents working on this repository.
For operator usage and command examples, refer to `README.md`.

## 0) Documentation Ownership

- `README.md` is operator-facing and should contain runnable usage guidance.
- `instructions.md` is implementation-facing and should contain architecture, invariants, and contributor/agent rules.
- Avoid duplicating full command walkthroughs here unless needed to explain engineering behavior.

## 1) Project Goal

`ynab-migrator` is a resumable Python CLI that migrates data from one YNAB plan to another using YNAB API v1 (`/plans/...` endpoints).

Primary goals:

- Replay migratable data with deterministic behavior.
- Survive interruptions and continue safely.
- Provide parity verification for the migrated subset.
- Never replay deleted/tombstoned entities.

## 2) Runtime and Entry Points

- Python: `>=3.9` (see `pyproject.toml`)
- CLI entrypoint: `ynab_migrator/cli.py`
- Installed script: `ynab-migrator = ynab_migrator.cli:main`
- Main commands: `plan`, `apply`, `verify`, `resume`
- CLI parsing detail: global flags (for example `--source-token`, `--dest-token`, `--source-plan-id`, `--dest-plan-id`, `--workdir`) must appear before the subcommand.
- Logging detail: `--verbose` (global flag) enables detailed technical API telemetry; default logging is human-readable progress.
- `apply` UX detail: interactive Up/Down selector before execution chooses scope (`Everything` first). Selected scope auto-expands to include required dependencies.

Quick local run:

```bash
python3 -m ynab_migrator.cli --help
```

Install editable:

```bash
pip install -e .
```

## 3) Repository Map

- `ynab_migrator/cli.py`
  - Parses arguments.
  - Builds `YNABClient`s + `MigrationEngine`.
  - Dispatches command (`plan`/`apply`/`verify`/`resume`).
  - Initializes runtime logging (console + per-command log file under workdir).

- `ynab_migrator/runtime_logging.py`
  - Central runtime logger setup.
  - Redaction filter for sensitive token-like content.
  - Per-command log file path strategy.

- `ynab_migrator/client.py`
  - Thin YNAB API wrapper.
  - Handles retries for `429/5xx`, request backoff, and local rolling rate limiter.
  - Exposes read/write endpoint helpers used by migration logic.

- `ynab_migrator/migration.py`
  - Core orchestration and data transforms.
  - Snapshot planning, resumable apply, parity verification.
  - Entity mapping and exclusion handling.

- `ynab_migrator/checkpoint.py`
  - SQLite state store for resumability (`metadata`, `mappings`, `cursors`, `events`).

- `ynab_migrator/utils.py`
  - Hashing, atomic JSON writes, chunking, deleted filtering, deterministic import IDs.

## 4) Artifacts Produced in `--workdir`

Default workdir: `./.ynab_migrator`

- `snapshot.json`:
  - Plan-phase immutable source extraction (filtered) + integrity hash.

- `plan_report.json`:
  - Counts, estimated request volume/time, unsupported defaults.
  - `manual_action_items` with required/recommended operator steps before/after apply.

- `<command>.log` (`plan.log`, `apply.log`, `verify.log`, `resume.log`):
  - Default mode: human-readable stage boundaries, progress checkpoints, and warnings/errors.
  - `--verbose` mode: adds technical HTTP request/retry/backoff telemetry and stack traces.
  - Overwritten on each run of that command.

- `checkpoint.sqlite3`:
  - Persistent migration state:
    - `metadata` (plan IDs, snapshot hash, reference date, exclusions)
    - `mappings` (`source_id -> dest_id`)
    - `cursors` (stage progress)
    - `events` (execution log)

- `apply_report.json`:
  - Mapping counts, warnings/errors, exclusions, recent events.

- `verify_report.json`:
  - Mismatches and pass/fail for migrated subset parity.

## 5) Command Semantics

### `plan`

- Fetches source plan/settings/month summaries/details.
- Filters deleted/tombstoned records.
- Extracts month-category `budgeted` values for all months.
- Reads destination current counts for preflight context.
- Builds explicit `manual_action_items` for operator prep (for example unsupported account-type exact-match account creation).
- Writes `snapshot.json` and `plan_report.json`.

### `apply`

- Requires `snapshot.json`.
- Initializes/uses checkpoint.
- Enforces snapshot hash continuity with checkpoint metadata.
- Resolves effective apply scope from:
  - interactive CLI selection (for `apply` command), or
  - stored checkpoint `apply_entities` (for resume continuity), or
  - default `Everything` when non-interactive.
- Dependency closure is automatic (for example selecting `transactions` includes `accounts`, `category_groups`, `categories`, `payees`).
- Resolves destination internal system entities and pre-maps source internal IDs.
- Runs stages in order:
  1. accounts
  2. capture destination auto-created starting-balance transaction candidates per account
  3. category groups
  4. categories
  5. payee mapping by name (plus transfer payees captured from account creation)
  6. transactions (batched + idempotent `import_id`; starting-balance entries use delete+recreate flow)
  7. payee mapping refresh by name
  8. scheduled transactions
  9. month category budget patches
- Updates cursor after each processed item to support resume.
- Transaction replay uses large write batches by default and recursive batch splitting on failures (instead of immediate one-by-one fallback), to keep calls close to endpoint batch capacity while isolating problematic payloads safely.
- Creates accounts with opening balance `0`; balance comes from replayed transaction history.
- For unsupported source account types, first attempts to map by exact destination account `name` + `type`; if no unique eligible match exists, creates as `cash` and records structured warnings/counters.
- For source `Starting Balance` transactions, deletes one captured destination auto starting-balance transaction and then creates the source transaction to preserve original source date.
- Month budget patches that return `404` (missing destination month/category resource) are recorded as warnings and excluded from parity instead of treated as hard errors.

### `resume`

- Alias for `apply`.

### `verify`

- Re-reads destination plan/month details.
- Compares migrated subset using mappings + canonicalized payload forms.
- For unsupported source account types, verifies account parity on `name` + `balance` only (skips `type` parity).
- Respects `exclusions` recorded during apply.
- Writes `verify_report.json` with mismatch details and pass/fail.

## 6) Critical Invariants (Do Not Break)

### A) Deleted/tombstoned data must never be replayed

- Use `clean_deleted(...)` and `is_deleted(...)` consistently.
- Applies to source extraction, apply loops, payload building, and verify comparisons.
- Current fallback tombstone statuses in `utils.is_deleted`: `deleted`, `tombstone`, `archived`.

### B) Resumability must remain correct

- Every long-running stage uses a cursor in checkpoint.
- Cursor must advance even on per-item failures (to avoid infinite retry loops without operator action).
- Mappings are source-of-truth for source->destination identity.

### C) Idempotency for transactions

- Use deterministic import IDs (`deterministic_import_id`) for creates.
- Reconcile duplicates through `duplicate_import_ids` and destination import map refresh.

### D) Snapshot/workdir integrity

- `apply` validates checkpoint plan IDs and snapshot hash continuity.
- `apply` enforces snapshot schema version compatibility.
- A changed snapshot with old checkpoint should fail fast.

### E) JSON artifact writes should be atomic

- Use `atomic_write_json` for all report/snapshot writes.

### F) Internal system entities must be mapped, not recreated

- Source system entities are detected in `plan` by exact names:
  - `Internal Master Category`
  - `Hidden Categories`
  - `Credit Card Payments`
  - `Inflow: Ready to Assign`
  - `Uncategorized`
- `apply` maps source system IDs to destination system IDs and skips creating those entities.
- Credit-card payment categories are mapped by exact name inside destination `Credit Card Payments`.
- If destination required system entities are missing or ambiguous, `apply` fails early.

### G) Non-migratable note fields are report-only

- Account notes and month notes are read-only in current YNAB API.
- `plan` writes them into `snapshot.json.non_migratable_fields`.
- `plan`/`apply`/`verify` reports include explicit warnings for these fields.

## 7) YNAB-Specific Behavior Already Encoded

- Rate limit assumption: `200 req/hour` (client-side rolling limiter).
- Split scheduled transactions are not created (excluded with warning).
- Scheduled transaction date is shifted forward if not future-dated at apply time.
- Month category patches only update `budgeted`.
- Month-category patches for internal system categories are skipped and excluded from parity.
- Some entities/fields are read-only in YNAB API and are not reproduced exactly.
- All accounts attempt exact destination name+type reuse first; unsupported account types fall back to `cash` only when reuse is not uniquely possible.

## 8) When Modifying Code

If you add a new migration stage:

- Add a dedicated checkpoint cursor (`<stage>_idx`).
- Add mapping/exclusion behavior if the stage produces identities or skip conditions.
- Filter deleted records before processing.
- Include stage outcomes in apply report.
- Add verify logic for that stage if parity is expected.

If you add new YNAB writable fields:

- Update payload builders in `migration.py`.
- Update normalization logic used by verify.
- Ensure parity compares only fields intended to be controlled by migrator.

If you change CLI args:

- Update `cli.py`.
- Update `README.md`.
- Keep defaults backward-compatible when possible.

## 9) Validation Checklist Before Finishing a Change

Run at minimum:

```bash
python3 -m compileall -q ynab_migrator
python3 -m ynab_migrator.cli --help
```

If behavior changes, also sanity-check with a temp workdir and mocked/safe plan IDs (or documented dry-run strategy).

## 10) Security and Secrets

- Never print tokens in logs/reports/errors.
- Tokens are passed via CLI args today; prefer secure env handling if extending.
- Treat artifacts as sensitive financial metadata.

## 11) Known Gaps / Future Improvements

- No formal unit/integration test suite yet.
- No encrypted-at-rest artifact storage.
- No explicit “dry-run without API writes” mode.
- No automatic plan creation (YNAB API limitation).
- No advanced logging tuning flags beyond `--verbose`.

## 12) Useful Mental Model

Think of the migrator as:

1. Deterministic source snapshot (with deleted filtering),
2. Stateful replay with checkpoint cursors + mappings,
3. Canonicalized parity validator over migrated subset.

Any change should preserve that 3-part contract.
