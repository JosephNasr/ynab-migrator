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

- Python: `>=3.11` (see `pyproject.toml`); Python 3.12 is the recommended local runtime.
- TOML parsing uses standard-library `tomllib`; do not add a third-party TOML reader
  unless support for Python 3.10 or older is deliberately restored.
- CLI entrypoint: `ynab_migrator/cli.py`
- Installed script: `ynab-migrator = ynab_migrator.cli:main`
- Main commands: `plan`, `apply`, `verify`, `resume`, `doctor`
- Default local config: `./ynab-migrator.toml`; copy it from the tracked
  `ynab-migrator.example.toml`. The real file is Git-ignored because it contains tokens.
- TOML contains exactly two required settings: `source_token` and `dest_token`.
  Unknown, missing, empty, or non-string values fail before budget discovery.
- The CLI accepts only a command. Do not add configuration flags or alternate
  config-path selection without an explicit product decision to change this invariant.
- Budget IDs are selected interactively by `plan` and are never configured in TOML.
  A successful plan stores the selected names/IDs in
  `.ynab_migrator/budget_selection.json` for later commands.
- `apply`, `resume`, `doctor`, and `verify` load the saved pair without displaying
  selectors. Every command still requires explicit confirmation showing both names
  and IDs. Non-interactive execution fails.
- The saved selection is bound to one-way fingerprints of both configured tokens. A
  token change requires a successful new `plan`; raw tokens must never be written to
  the selection artifact.
- Budget selectors must display only `name` and `id`, never other API metadata.
- Logging is fixed to human-readable progress without verbose HTTP telemetry.
- `apply` UX detail: an interactive Up/Down selector chooses scope. Selected scope
  auto-expands to include required dependencies.

Quick local run:

```bash
python3 -m ynab_migrator.cli --help
```

Install editable:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## 3) Repository Map

- `ynab_migrator/cli.py`
  - Parses the command and loads both tokens from `./ynab-migrator.toml`.
  - `plan` discovers budgets and runs both selectors; later commands load the saved,
    token-bound selection. Every command confirms the names/IDs.
  - Builds `YNABClient`s + `MigrationEngine`.
  - Dispatches command (`plan`/`apply`/`verify`/`resume`/`doctor`).
  - Initializes runtime logging (console + per-command log file under workdir).

- `ynab_migrator/config.py`
  - Loads and validates the local TOML file.
  - Accepts only `source_token` and `dest_token`.
  - Contains no interactive prompting or operating-system credential-store integration.

- `ynab_migrator/runtime_logging.py`
  - Central runtime logger setup.
  - Redaction filter for sensitive token-like content.
  - Per-command log file path strategy.

- `ynab_migrator/client.py`
  - Thin YNAB API wrapper.
  - Handles retries for `429/5xx`, request backoff, and local rolling rate limiter.
  - Supports `last_knowledge_of_server` on delta-capable read helpers.
  - Exposes read/write endpoint helpers used by migration logic.

- `ynab_migrator/migration.py`
  - Core orchestration and data transforms.
  - Snapshot planning, resumable apply, parity verification.
  - Entity mapping and exclusion handling.

- `ynab_migrator/checkpoint.py`
  - SQLite state store for resumability (`metadata`, `mappings`, `cursors`, `events`).

- `ynab_migrator/utils.py`
  - Hashing, atomic JSON writes, chunking, deleted filtering, deterministic import IDs.

## 4) Artifacts Produced in `workdir`

Default workdir: `./.ynab_migrator`

- `snapshot.json`:
  - Plan-phase immutable source extraction (filtered) + integrity hash.

- `budget_selection.json`:
  - Source/destination names and IDs saved after a successful `plan`.
  - Includes token fingerprints for stale-selection detection, never raw tokens.

- `plan_report.json`:
  - Counts, estimated request volume/time, unsupported defaults.
  - `manual_action_items` with required/recommended operator steps before/after apply.

- `<command>.log` (`plan.log`, `apply.log`, `verify.log`, `resume.log`, `doctor.log`):
  - Default mode: human-readable stage boundaries, progress checkpoints, and warnings/errors.
  - Overwritten on each run of that command.

- `checkpoint.sqlite3`:
  - Persistent migration state:
    - `metadata` (plan IDs, snapshot hash, reference date, exclusions)
    - `mappings` (`source_id -> dest_id`)
    - `cursors` (stage progress)
  - `events` (execution log)
  - durable entity statuses (`pending`, `in_progress`, `succeeded`,
    `retryable_failed`, `ambiguous_commit`, `permanently_failed`, `excluded`)

- `apply_report.json`:
  - Mapping counts, warnings/errors, exclusions, recent events.

- `verify_report.json`:
  - Mismatches and pass/fail for migrated subset parity.

- `doctor_report.json`:
  - Checkpoint coverage, recoverable mappings, collisions, and resume safety.

- `checkpoint.pre-v2.sqlite3`:
  - One-time backup created before upgrading an older checkpoint schema.

- `rate-limit-<token-fingerprint>.json`:
  - Persisted rolling request timestamps; the filename contains only a one-way token fingerprint.

- `apply.lock`:
  - Advisory file lock preventing concurrent mutation from the same workdir.

## 5) Command Semantics

### `plan`

- Fetches one full source plan export. The export already contains plan settings,
  detailed months/categories, transactions, and other related entities; do not add
  separate settings, month-list, or per-month source reads.
- Filters deleted/tombstoned records.
- Extracts month-category `budgeted` values for all months.
- Reads destination current counts for preflight context.
- Builds explicit `manual_action_items` for operator prep (for example unsupported account-type exact-match account creation).
- Writes `snapshot.json` and `plan_report.json`.
- After both artifacts are written successfully, the CLI writes
  `budget_selection.json`; a failed or cancelled plan does not replace the saved pair.

### `apply`

- Requires `snapshot.json`.
- Initializes/uses checkpoint.
- Enforces snapshot hash continuity with checkpoint metadata.
- Resolves effective apply scope from:
  - mandatory interactive CLI selection (for `apply` command), or
  - stored checkpoint `apply_entities` for resume continuity.
- Dependency closure is automatic (for example selecting `transactions` includes `accounts`, `category_groups`, `categories`, `payees`).
- Resolves destination internal system entities and pre-maps source internal IDs.
- Runs stages in order:
  1. accounts
  2. refresh the shared destination working set and capture auto-created
     starting-balance candidates from its combined transaction collection
  3. category groups
  4. categories
  5. payee mapping by name (plus transfer payees captured from account creation)
  6. transactions (batched + idempotent `import_id`; starting-balance entries use delete+recreate flow)
  7. payee mapping refresh by name
  8. scheduled transactions
  9. month category budget patches
- Updates cursor after each processed item to support resume.
- Transaction replay uses large write batches by default and recursive batch splitting on failures (instead of immediate one-by-one fallback), to keep calls close to endpoint batch capacity while isolating problematic payloads safely.
- A single full destination export initializes an in-memory working set. Entity arrays
  are updated from successful write responses and merged by stable ID (`month` for
  months) from delta plan responses. Delta tombstones remain cached and are filtered
  with `clean_deleted(...)` when consumed.
- Persisting `server_knowledge` without its matching full working set is unsafe. Server
  knowledge is therefore used only within a command; every new command begins with a
  fresh full destination export.
- Ordinary create responses that cannot be correlated immediately are accumulated and
  reconciled together by deterministic `import_id`, with bounded delta refreshes.
- On a batch `409`, all import IDs are reconciled once and only unresolved entries are
  retried before recursive splitting is used.
- Transfer creates remain one POST per pair for deterministic checkpoint semantics,
  but counterparts are validated together from the shared transaction cache after a
  grouped delta refresh. Do not reintroduce per-transfer transaction GETs.
- Creates accounts with opening balance `0`; balance comes from replayed transaction history.
- For unsupported source account types, first attempts to map by exact destination account `name` + `type`; if no unique eligible match exists, creates as `cash` and records structured warnings/counters.
- For source `Starting Balance` transactions, deletes one captured destination auto starting-balance transaction and then creates the source transaction to preserve original source date.
- Month budget patches that return `404` (missing destination month/category resource) are recorded as warnings and excluded from parity instead of treated as hard errors.

### `resume`

- Alias for `apply`.

### `doctor`

- Performs API-read-only checkpoint diagnostics.
- Reports stale/colliding mappings and deterministic transaction imports that can
  be recovered on resume.
- Writes `doctor_report.json`.

### `verify`

- Re-reads one full destination plan and uses its embedded detailed months; it does not
  issue per-month or redundant payee reads.
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
- A cursor is a scan optimization, not proof of completion.
- Cursor may advance on per-item failure, but the entity must retain a durable
  retryable, ambiguous, permanent-failure, or excluded outcome.
- Mappings plus durable statuses are the source of truth for replay completion.
- A mapping and its succeeded status must be committed atomically.
- `KeyboardInterrupt` must be allowed to unwind through migration `finally` blocks so
  reports and locks are finalized, then be handled by the CLI without a traceback.
  The CLI returns status 130 and directs the operator to `ynab-migrator resume`.

### C) Idempotency for transactions

- Use deterministic import IDs (`deterministic_import_id`) for creates.
- Reconcile duplicates through `duplicate_import_ids` and destination import map refresh.

### D) Snapshot/workdir integrity

- Source and destination plan IDs must differ; reject equal IDs before any API call.
- `apply` validates checkpoint plan IDs and snapshot hash continuity.
- `apply` enforces snapshot schema version compatibility.
- A changed snapshot with old checkpoint should fail fast.
- The migration engine owns the workdir lock so direct library callers cannot bypass
  concurrent-apply protection.

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
- Ambiguous exact-name reuse for ordinary accounts, category groups, and categories
  also fails closed; never resolve ambiguity by creating another entity.

### G) Non-migratable note fields are report-only

- Account notes and month notes are read-only in current YNAB API.
- `plan` writes them into `snapshot.json.non_migratable_fields`.
- `plan`/`apply`/`verify` reports include explicit warnings for these fields.

## 7) YNAB-Specific Behavior Already Encoded

- `plan` starts with one `GET /plans` request per unique token for interactive source
  and destination discovery. When both configured tokens are equal, the returned list
  is reused by both selectors. Later commands load `budget_selection.json` and make no
  discovery requests.
- Server rate limit assumption: `200 req/hour`; the fixed client limit is 190 for headroom.
- Clients sharing the same access token share one in-process rolling limiter.
- Rolling request timestamps are persisted by a one-way token fingerprint so a
  restarted process does not forget requests made during the preceding hour.
- Transfer cleared-state updates use the bulk transaction PATCH endpoint.
- A transfer pair is mapped only after validating both destination sides, including
  reciprocal IDs, mapped accounts, dates, and opposite source amounts.
- Month budgets normally reuse detailed months in the full destination working set;
  a specific month read is only a fallback. Equal values are not patched.
- Only transaction write endpoints support array request bodies in current API (`POST /plans/{plan_id}/transactions`, `PATCH /plans/{plan_id}/transactions`).
- Full plan reads are intentional because migration consumes nearly every collection.
  Later reads within the command use `last_knowledge_of_server` and merge the delta.
- Account, category-group, category, scheduled-transaction, and month/category writes
  remain one request per entity because the public API has no bulk operation for them.
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

If you change the command/config interface:

- Update `cli.py`.
- Update `README.md`.
- Update `ynab-migrator.example.toml`, `ynab_migrator/config.py`, and `.gitignore`
  when configuration keys or file names change.
- Keep TOML as the only token source and keep budget IDs out of configuration
  unless the user explicitly changes that product requirement.

## 9) Validation Checklist Before Finishing a Change

Run at minimum:

```bash
python3 -m compileall -q ynab_migrator
python3 -m ynab_migrator.cli --help
```

If behavior changes, also sanity-check with a temp workdir and mocked/safe plan IDs (or documented dry-run strategy).

## 10) Security and Secrets

- Never print tokens in logs/reports/errors.
- `ynab-migrator.toml` contains plaintext API tokens. It must remain Git-ignored;
  recommend owner-only permissions (`chmod 600 ynab-migrator.toml`).
- `ynab-migrator.example.toml` must contain placeholders only—never real credentials.
- Never add token flags or budget IDs to the command line. TOML is the exclusive
  token source; budget IDs are mandatory interactive selections.
- Treat artifacts as sensitive financial metadata.

## 11) Known Gaps / Future Improvements

- No live API integration test suite yet; local regression tests use fakes/mocks.
- No encrypted-at-rest artifact storage.
- No explicit “dry-run without API writes” mode.
- No automatic plan creation (YNAB API limitation).
- No configurable logging verbosity.

## 12) Useful Mental Model

Think of the migrator as:

1. Deterministic source snapshot (with deleted filtering),
2. Stateful replay with checkpoint cursors + mappings,
3. Canonicalized parity validator over migrated subset.

Any change should preserve that 3-part contract.
