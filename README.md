# ynab-migrator

Resumable CLI to migrate YNAB data from one plan to another using the public API.

## Documentation Split

- `README.md`: operator usage (install, run, artifacts, troubleshooting)
- `instructions.md`: engineering/agent source of truth (architecture, invariants, implementation behavior)

## Core Safety Guarantees

- Deleted/tombstoned entities are filtered and never replayed.
- Migration is resumable via `checkpoint.sqlite3`.
- Transaction replay is idempotent with deterministic `import_id` values.
- Apply tracks durable per-entity outcomes; a cursor cannot hide unresolved work.
- Source and destination plan IDs must differ.
- Concurrent `apply`/`resume` processes in the same workdir are rejected, including
  migration-engine calls made outside the CLI.
- Ambiguous account/category name matches fail closed instead of creating duplicates.
- Transfer mappings are persisted only after both destination sides pass reciprocal
  account, date, amount, and linkage validation.
- Account balances are rebuilt from transaction history (accounts created with `balance=0`).
- Runtime logs are always written (`plan.log`, `apply.log`, `verify.log`, `resume.log`, `doctor.log`).
- `Ctrl+C` exits cleanly without a traceback; completed apply work remains checkpointed.

## Install

Python 3.11 or newer is required because configuration loading uses the standard
library's `tomllib`. Python 3.12 is recommended for this repository.

Create an isolated environment using the Python 3.12 installation already
available on this machine:

```bash
cd /path/to/ynab_migrate
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
python --version
```

The final command should report Python 3.11 or newer. On later runs, activate the
environment again with `source .venv/bin/activate` before using `ynab-migrator`.

## Quickstart

Create your local configuration file from the tracked example:

```bash
cp ynab-migrator.example.toml ynab-migrator.toml
chmod 600 ynab-migrator.toml
```

Edit `ynab-migrator.toml` and replace the two token placeholders.
This exact filename is ignored by Git because it contains API tokens. The checked-in
`ynab-migrator.example.toml` contains safe placeholders only.

```bash
ynab-migrator plan
ynab-migrator apply
ynab-migrator doctor
ynab-migrator resume
ynab-migrator verify
```

`ynab-migrator.toml` is the only source of runtime configuration. The CLI accepts
only a command; it has no flags for alternate config paths, credentials, budget IDs,
workdir, output mode, verbosity, batching, rate limits, or retries.

`plan` establishes the migration's saved budget pair:

1. Fetch budgets accessible with the source token and select one by name and ID.
2. Fetch budgets accessible with the destination token and select one by name and ID.
   When both tokens are identical, the budget list is fetched once and reused.
3. Review the selected source and destination budget names/IDs and answer `y` or
   `yes` to begin. Any other answer cancels without starting the command.
4. After `plan` completes successfully, the selected names and IDs are saved in
   `.ynab_migrator/budget_selection.json`.

Following commands load that saved pair without showing the budget selectors. They
still display both names and IDs and require `y` or `yes` before starting. `apply`
also asks for the migration scope before its confirmation. Run `plan` again whenever
you want to choose a different source or destination.

The budget lists intentionally display only budget names and IDs. Budget IDs are
never stored in TOML. `plan` requires an interactive terminal with curses support;
all commands require interactive confirmation and never choose a budget automatically.

## Configuration

The TOML file requires exactly these top-level keys:

| Key | Type | Purpose |
|---|---:|---|
| `source_token` | string | API token used to read the source plan |
| `dest_token` | string | API token used to write the destination plan |

Operational behavior is fixed: `workdir` is `./.ynab_migrator`, transaction batches
target 200 entries, transient requests retry up to eight times with 1–120 second
exponential backoff, console output is human-readable, and detailed HTTP telemetry
is disabled. JSON report files are still written in `workdir`.

The client-side request cap is fixed at 190 per unique token per rolling hour. This
leaves ten requests (5%) of headroom below YNAB's nominal 200-request limit. `plan`
uses one budget-list request per unique token, and those requests count toward the
limits. Following commands reuse the saved pair and make no budget-list requests.

## API Request Efficiency

The migrator deliberately keeps its small built-in HTTP client; it does not depend on
the generated YNAB Python SDK. It uses the same public API capabilities directly:

- `plan` obtains source settings, months, month categories, transactions, and other
  entities from one full source-plan export instead of issuing per-month reads.
- `apply` maintains one in-memory destination working set. Successful write responses
  update it locally, while YNAB-generated side effects are merged through delta plan
  requests using `last_knowledge_of_server`.
- Starting Balance candidates are found from one shared transaction collection rather
  than one request per newly created account.
- Ordinary transaction IDs and transfer counterparts are reconciled in grouped
  refreshes. A transaction batch conflict is reconciled by import ID before the
  remaining entries are split and retried.
- Month-budget apply and verification normally reuse month/category data already
  present in a full plan response. A specific month read is only a fallback if a
  destination month is absent from the working set.

Accounts, category groups, categories, scheduled transactions, and individual
month/category budget updates have only singular write endpoints, so large migrations
can still require rate-limit waits. The request estimate in `plan_report.json` includes
the shared working-set read and conservative grouped-refresh allowances.

Unknown keys, missing keys, and non-string token values are rejected before any API
client is created.

On the first apply with the upgraded checkpoint format, the existing database is
backed up atomically as `checkpoint.pre-v2.sqlite3`. Legacy cursors are retained
for diagnostics, while missing transaction mappings are automatically revisited.

## Artifacts (`workdir`)

- `snapshot.json`
- `budget_selection.json` (selected names/IDs and token fingerprints; never tokens)
- `plan_report.json`
- `apply_report.json`
- `verify_report.json`
- `doctor_report.json`
- `checkpoint.sqlite3`
- `checkpoint.pre-v2.sqlite3` (one-time backup when upgrading an older checkpoint)
- `rate-limit-<token-fingerprint>.json`
- `apply.lock` (advisory lock file)
- `plan.log`, `apply.log`, `verify.log`, `resume.log`, `doctor.log`

## Important Limitations

- Destination plan must already exist.
- The `plan` phase may report required or recommended manual actions that should be reviewed before running `apply`.
- Split scheduled transactions are not writable via current API write schema.

## Troubleshooting

- Run commands from the directory containing `ynab-migrator.toml`. There is no
  alternate config-path flag.
- Run `plan` in an interactive terminal to select the budgets. Later commands reuse
  `.ynab_migrator/budget_selection.json`; every command still requires confirmation.
- If a configured token changes, rerun `plan` to replace the token-bound saved
  selection before using `apply`, `resume`, `doctor`, or `verify`.
- If snapshot/checkpoint mismatch errors appear, rerun from a clean workdir.
- Pressing `Ctrl+C` during apply or a rate-limit wait exits safely with status 130.
  Run `ynab-migrator resume` later; already completed work will not be replayed blindly.
- Run `doctor` after an interrupted or older migration. Resolve stale mappings or
  ambiguous transfer commits before allowing more writes.
- A long rate-limit wait is expected when the destination token reaches its
  rolling request allowance. The default client limit leaves a small safety margin,
  and recent request timestamps are retained per token fingerprint across resumes.

## Advanced / Internal Behavior

For implementation details, invariants, and change guidelines, use `instructions.md`.
