# AGENTS.md

This file adds path-specific guidance for Dumpling integration tests under
`dumpling/tests/**`. The repository root `AGENTS.md` still applies.

## Test Placement

- Prefer extending the nearest existing case in `dumpling/tests/<case>/run.sh`
  before adding a new case directory.
- Keep new cases focused on one Dumpling behavior or workflow. Avoid broad
  setup churn in shared cases unless the behavior under test needs it.
- Use deterministic table data and exact assertions against generated files in
  `$DUMPLING_OUTPUT_DIR`.
- Set up and clean the database/table state needed by the test block itself.
  Do not rely on state left by an earlier block in the same script.

## Harness Helpers

- Use `run_sql` for SQL setup and assertions against the source database.
- Use `run_dumpling` for Dumpling invocations instead of calling `bin/dumpling`
  directly, unless the test is specifically about the helper or command wrapper.
- Use `file_should_exist` and `file_not_exist` for simple file presence checks.
- Inspect dumped SQL or CSV output under `$DUMPLING_OUTPUT_DIR`; keep greps and
  cuts narrow enough that the assertion proves the intended behavior.

## Ports and Services

- `DUMPLING_TEST_PORT=4000` targets the TiDB server started by the Dumpling test
  harness. Use this for TiDB-specific features and TiDB version-gated behavior.
- `DUMPLING_TEST_PORT=3306` targets the external MySQL service expected by the
  harness. Use this only for MySQL compatibility coverage or existing cases that
  intentionally compare MySQL behavior.
- When a script switches ports, set `DUMPLING_TEST_PORT` explicitly before each
  block that depends on a specific server. Do not assume the previous block left
  the desired value.
- If a test depends on TiDB version parsing, verify the local or CI
  `bin/tidb-server` was built from a checkout with enough Git tag/history context
  for `git describe --tags` to produce a semver-shaped release string. A
  shallow or tagless checkout can make Dumpling detect TiDB as version `0.0.0`.

## Running Tests

Run commands from the repository root.

```bash
# Build the TiDB server used by the harness when needed.
make server
```

```bash
# Run every Dumpling integration case.
make dumpling_integration_test
```

```bash
# Run one case.
CASE=basic make dumpling_integration_test
```

```bash
# Run one case with shell tracing.
VERBOSE=true CASE=basic make dumpling_integration_test
```

The `dumpling_integration_test` target checks these binaries before running:
`bin/tidb-server`, `bin/minio`, `bin/mc`, `bin/tidb-lightning`, and
`bin/sync_diff_inspector`. The required sync-diff binary path uses an underscore:
`bin/sync_diff_inspector`.

The full harness also expects the `mysql` client and a local MySQL-compatible
server on `127.0.0.1:3306` for cases that target MySQL.

## Validation Notes

- For a small shell-only test change, run the changed case with
  `CASE=<case> make dumpling_integration_test` when local prerequisites are
  available.
- If unrelated blocks in the same case require unavailable local services, a
  focused TiDB-only reproduction may be useful while iterating, but report that
  the official case target was not fully run.
- `make bazel_prepare` is not required for changes limited to Dumpling shell
  integration tests or this file. Re-check root `AGENTS.md` if Go files, Bazel
  metadata, or module files are also changed.
