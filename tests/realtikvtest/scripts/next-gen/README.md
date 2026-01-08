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
