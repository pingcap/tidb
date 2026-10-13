# RestoreRegion manual fixtures and evidence

These tools exercise an explicitly configured Classic source, CSE target, and
S3-compatible backup store. They require a compatible NextGen BR, Store, and
Worker with the experimental RestoreRegion path enabled. They are run manually;
`run-manual.sh` is intentionally outside the `*/run.sh` integration-test discovery.

Create a fixture once, then restore the frozen fixture into a fresh target:

```bash
# From the TiDB repository root. Configure the endpoints and binaries first.
br/tests/br_restore_region/run-manual.sh prepare --kind full \
  --database br_restore_region_full --fixture /path/to/new-fixture
br/tests/br_restore_region/run-manual.sh restore \
  --fixture /path/to/frozen-fixture --run /path/to/new-run
```

Preparation requires `SOURCE_BR`, `SOURCE_PD`, `RESTORE_REGION_STORAGE`, and
optionally `SOURCE_SQL_PORT` (default `24000`). Restore requires `RESTORE_BR`,
`TARGET_PD`, `TARGET_KEYSPACE`, and optionally `TARGET_SQL_PORT` (default `4001`).
`RESTORE_REGION_STORAGE` may override the fixture's endpoint and access settings,
but must retain the same bucket and prefix. AWS CLI object capture also requires
the appropriate AWS credentials. Source BR and restore BR are separate binaries.

The target must not already contain the fixture database. Automatic retry and
checkpoint resume are unsupported; preserve failed runs and use a new target.

Capture component identities with explicit repository and binary paths:

```bash
python3 br/tests/br_restore_region/capture.py /path/to/new-evidence --snapshots \
  --repository br=/path/to/tidb \
  --repository cse=/path/to/cse \
  --binary source-br=/path/to/classic-br \
  --binary restore-br=/path/to/nextgen-br
```

Repeat `--repository` and `--binary` for all relevant local components. Repository
paths must be actual Git worktrees at the revisions being tested. No historical
kvproto checkout is required; preserve the consumer dependency files with the
source snapshots. `capture.py` and `audit.py` still use the original Compose
container names and monitoring ports; inspect those assumptions before using a
different cluster layout. Missing container captures must be investigated before
claiming complete runtime provenance.

Evidence is private: raw command arguments, component configurations, outputs,
and fixture storage URLs can contain credentials. The recorder omits
`AWS_SECRET_ACCESS_KEY` from its environment metadata, but does not sanitize the
raw evidence bundle. Publish only a separately reviewed, sanitized summary.

Run the local tool regressions without starting a cluster:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s br/tests/br_restore_region -p test_tools.py
```
