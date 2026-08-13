# Next-Gen TiDB Testing Scripts

This directory contains scripts for testing the next-generation TiDB architecture with a local test cluster. These scripts help set up and run tests against a real TiKV cluster with next-generation components.

## Overview

The scripts in this directory facilitate testing the next-generation architecture of TiDB by:

1. Setting up a local test cluster with PD, TiKV, and TiKV-Worker components
2. Configuring MinIO as an S3-compatible storage for DFS (Distributed File System) features
3. Running test suites against this cluster

## Scripts

### bootstrap-test-with-cluster.sh

This script sets up a complete local test cluster with:
- 3 PD nodes
- 3 TiKV servers
- 1 TiKV-Worker node
- MinIO server for S3-compatible storage

It creates temporary data directories, configures the components using the configuration files in `tests/realtikvtest/configs/next-gen/`, and starts all services required for testing.

Usage:
```bash
./bootstrap-test-with-cluster.sh <command>
```

The script will execute `<command>` with the `NEXT_GEN=1` environment variable set.

### run-tests-with-gotest.sh

This script runs a specific Go test suite against the next-gen cluster.

Usage:
```bash
./run-tests-with-gotest.sh <test_suite> [<timeout>]
```

Arguments:
- `<test_suite>`: The test suite directory under `./tests/realtikvtest/`
- `<timeout>`: Optional timeout for the test suite (default: 40m)

Example:
```bash
./run-tests-with-gotest.sh addindextest 60m
```

### run-tests.sh

This script runs a make test task against the next-gen cluster.

Usage:
```bash
./run-tests.sh <make_test_task>
```

Example:
```bash
./run-tests.sh bazel_addindextest
```

### run-starter-tests-with-server.sh

This script runs external starter-mode tests against a real `tidb-server`
process. It first reuses `bootstrap-test-with-cluster.sh` to start PD, TiKV,
TiKV-Worker, and MinIO. Then it starts `bin/tidb-server` with
`deploy-mode = "starter"` and runs Go tests through the MySQL protocol and
status HTTP APIs. The standard `startertest` Makefile target runs against a
non-`SYSTEM` keyspace. For a non-`SYSTEM` target keyspace, the script first
bootstraps the shared `SYSTEM` keyspace with a no-op starter server, then
creates the target keyspace through the PD keyspace API. It then starts TiDB in
standby mode, activates the configured keyspace through `/tidb-pool/activate`,
and runs the tests against the activated external server. For the default
`startertest` run, the script also runs a final destructive phase that verifies
graceful exit waits for held client connections before shutting down the
external `tidb-server`.

This script is an independent direct entry point and is not part of generic
`run-tests.sh` suite discovery. For the standard next-gen RealTiKV CI flow, use
the `startertest` Makefile target through `run-tests.sh`; that target
intentionally calls this shell runner because starter coverage needs an external
`tidb-server` process:

```bash
tests/realtikvtest/scripts/next-gen/run-tests.sh startertest
```

When `TIDB_SERVER_BIN` is not set, the script builds `bin/tidb-server` from the
current checkout before starting the external server. The script exports
`NEXT_GEN=1` internally, so the built binary and the Go tests both use the
next-gen code paths.

Because the script reuses `bootstrap-test-with-cluster.sh`, `bin/pd-server`,
`bin/tikv-server`, and `bin/tikv-worker` must also be available.

Usage:

```bash
tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh [<test_suite>] [<timeout>] [<go test args>...]
```

Arguments:
- `<test_suite>`: The test suite directory under `./tests/realtikvtest/`
  (default: `startertest`)
- `<timeout>`: Optional timeout for the test suite (default: `40m`)
- `<go test args>`: Optional extra arguments passed to `go test`

Example:

```bash
tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh startertest 40m -run TestExternalStarterSysVarContracts
```

Useful environment variables:
- `TIDB_SERVER_BIN`: Path to the next-gen `tidb-server` binary
  (default: `bin/tidb-server`, built from the current checkout by the script)
- `STARTER_MAX_ALLOWED_PACKET`: Starter `max-allowed-packet` config used by
  the external server (default: `65536`)
- `STARTER_TIKV_WORKER_URL`: Starter `tikv-worker-url` config used by the
  external server (default: `localhost:19000`)
- `STARTER_KEYSPACE_NAME`: Starter keyspace activated by the script
  (default: `SYSTEM`)
- `STARTER_PREPARE_KEYSPACE`: Whether to bootstrap the shared `SYSTEM` keyspace
  and create a non-`SYSTEM` `STARTER_KEYSPACE_NAME` before starting the tested
  starter server (default: `1`)
- `STARTER_KEYSPACE_CREATE_BODY`: Optional custom request body for creating
  a non-`SYSTEM` `STARTER_KEYSPACE_NAME` through the PD keyspace API
- `STARTER_SYSTEM_BOOTSTRAP_TIMEOUT`: Timeout for the no-op `SYSTEM` bootstrap
  phase before creating a non-`SYSTEM` keyspace (default: `2m`)
- `STARTER_STANDBY_MODE`: Whether to start in standby mode and activate before
  running tests (default: `1`; set to `0` to start directly with
  `-keyspace-name`)
- `STARTER_ACTIVATE_EXPORT_ID`: `export_id` sent in the standby activation
  request (default: `starter-external-export`)
- `STARTER_ACTIVATE_MAX_IDLE_SECONDS`: `max_idle_seconds` sent in the standby
  activation request (default: `60`)
- `STARTER_KEYSPACE_OBSERVABILITY`: Whether to configure starter keyspace
  observability mappings and send activation metadata for those mappings
  (default: follows `STARTER_STANDBY_MODE`; requires standby activation)
- `STARTER_KEYSPACE_META_TENANT` / `STARTER_KEYSPACE_META_PROJECT`: Metadata
  values sent in the standby activation request and expected in starter
  observability labels (defaults: `starter_tenant` / `starter_project`)
- `STARTER_ACTIVATION_TIMEOUT`: Starter standby activation timeout passed to
  `tidb-server` (default: `120`)
- `STARTER_RUN_EXIT_WAIT_TEST`: Whether to run the final destructive
  graceful-exit wait test. By default it runs only for `startertest` when no
  extra `go test` arguments are passed. The `startertest` Makefile target sets
  this to `1` explicitly because it passes `-count=1`.
- `STARTER_EXIT_WAIT_TEST_TIMEOUT`: Timeout for the final destructive
  graceful-exit wait test (default: `2m`)
- `STARTER_TIDB_PORT` / `STARTER_TIDB_STATUS_PORT`: Preferred starting ports
  for the SQL and status listeners (defaults: `4000` / `10080`; the script
  advances to the next available port if needed)

## Configuration Files

The test cluster uses configuration files from `tests/realtikvtest/configs/next-gen/`:

- `pd.toml`: Configuration for PD servers with keyspace settings
- `tikv.toml`: Configuration for TiKV servers with storage, DFS and IA settings
- `tikv-worker.toml`: Configuration for TiKV-Worker with DFS and IA settings

## Required Ports

The test cluster requires the following TCP ports to be available:

- PD: 2379, 2380, 2381, 2383, 2384
- TiKV: 20160, 20161, 20162, 20180, 20181, 20182
- TiKV-Worker: 19000
- MinIO: 9000 (configurable via MINIO_PORT environment variable)

## Environment Variables

You can customize the MinIO setup using these environment variables:

- `MINIO_BIN_PATH`: Path to the MinIO binary (defaults to searching in PATH)
- `MINIO_PORT`: Port for the MinIO server (default: 9000)
- `MINIO_ACCESS_KEY`: MinIO access key (default: minioadmin)
- `MINIO_SECRET_KEY`: MinIO secret key (default: minioadmin)

## Cleanup

The scripts automatically clean up processes on exit. However, temporary data directories created during test runs are not automatically removed.

## Requirements

- Go development environment
- TiDB development environment with compiled binaries in `bin/` directory
- Available ports as listed above
- Sufficient disk space for temporary data directories

## Next-Gen Testing

The next-gen architecture includes:
- Distributed File System (DFS) integration with S3-compatible storage
- TiKV-Worker for offloading coprocessor tasks
- Enhanced Intelligent Architecture (IA) capabilities

The tests are run with the `NEXT_GEN=1` environment variable to enable next-gen specific test paths.
