# bizcredit

A small offline data-processing service with a local HTTP fixture and SQLite storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/bizcredit`. The fixture service supports the source formats used by the package, and the command-line entry point writes results to the local SQLite database. Re-running commands is safe for an existing database.

## Verification submission recovery

Saving a material locally never implies the external portal accepted it. Each
material gets a durable row in `submissions` with one of four distinguishable
states:

| State      | Meaning                                                        |
|------------|----------------------------------------------------------------|
| `pending`  | registered locally, not yet dispatched                         |
| `unknown`  | dispatched, but the portal outcome could not be confirmed      |
| `accepted` | the portal confirmed the record (newly created or deduplicated)|
| `rejected` | the portal gave a definitive refusal                           |

Every dispatch carries the same content-derived idempotency token. On an
ambiguous result (timeout, connection drop, 5xx) the client retries with
bounded backoff and, if still unconfirmed, queries the portal's
`/submissions/{token}` reconciliation endpoint instead of resending blindly.
A later run processes only `pending`/`unknown` rows: `unknown` rows are
reconciled first, and a new POST happens solely when the portal confirms it
holds no record. Terminal (`accepted`/`rejected`) conclusions are never
resent, so repeated clicks and restarts cannot create a second verification
record.

Each run result (and `python -m bizcredit.cli --status`, which is read-only)
reports per-state counts plus an `unconfirmed` list identifying exactly which
materials still lack a portal conclusion, so partially completed runs show
precisely what remains. A process-wide lock makes a concurrent second run
return the current state with `skipped: true` rather than dispatch again.

The local fixture exposes test hooks for failure injection:
`POST /test/fail_submit` (`reject`, `timeout_before`, `timeout_after`,
`drop_after`) and `POST /test/reset`.
