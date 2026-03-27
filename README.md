# ynab-migrator

Resumable CLI to migrate YNAB data from one plan to another using the public API.

## Documentation Split

- `README.md`: operator usage (install, run, artifacts, troubleshooting)
- `instructions.md`: engineering/agent source of truth (architecture, invariants, implementation behavior)

## Core Safety Guarantees

- Deleted/tombstoned entities are filtered and never replayed.
- Migration is resumable via `checkpoint.sqlite3`.
- Transaction replay is idempotent with deterministic `import_id` values.
- Account balances are rebuilt from transaction history (accounts created with `balance=0`).
- Runtime logs are always written (`plan.log`, `apply.log`, `verify.log`, `resume.log`).

## Install

```bash
pip install -e .
```

## Quickstart

Set your credentials/IDs first:

```bash
export YNAB_SOURCE_TOKEN="..."
export YNAB_DEST_TOKEN="..."
export YNAB_SOURCE_PLAN_ID="..."
export YNAB_DEST_PLAN_ID="..."
```

Run `plan`:

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  plan
```

Run `apply`:

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  apply
```

Run `verify`:

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  verify
```

Resume interrupted apply:

```bash
ynab-migrator \
  --source-token "$YNAB_SOURCE_TOKEN" \
  --dest-token "$YNAB_DEST_TOKEN" \
  --source-plan-id "$YNAB_SOURCE_PLAN_ID" \
  --dest-plan-id "$YNAB_DEST_PLAN_ID" \
  --workdir ./.ynab_migrator \
  resume
```

## Artifacts (`--workdir`)

- `snapshot.json`
- `plan_report.json`
- `apply_report.json`
- `verify_report.json`
- `checkpoint.sqlite3`
- `plan.log`, `apply.log`, `verify.log`, `resume.log`

## Important Limitations

- Destination plan must already exist.
- The `plan` phase may report required or recommended manual actions that should be reviewed before running `apply`.
- Split scheduled transactions are not writable via current API write schema.

## Troubleshooting

- Put global flags before subcommand (`plan`/`apply`/`verify`/`resume`).
- If snapshot/checkpoint mismatch errors appear, rerun from a clean workdir.

## Advanced / Internal Behavior

For implementation details, invariants, and change guidelines, use `instructions.md`.
