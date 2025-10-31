# Classic TiDB Testing Scripts

This directory contains scripts for testing the classic TiDB architecture with a local test cluster. These scripts help set up and run tests against a real TiKV cluster with traditional components.

## Overview

The scripts in this directory facilitate testing the classic architecture of TiDB by:

1. Setting up a local test cluster with PD and TiKV components
2. Running test suites against this cluster

## Scripts

### bootstrap-test-with-cluster.sh

This script sets up a complete local test cluster with:
- 3 PD nodes
- 3 TiKV servers

It creates temporary data directories, configures the components using the configuration files in `tests/realtikvtest/configs/classic/`, and starts all services required for testing.

Usage:
```bash
./bootstrap-test-with-cluster.sh <command>
```

The script will execute `<command>` after the cluster is set up.

### run-tests-with-gotest.sh

This script runs a specific Go test suite against the classic cluster.

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

This script runs a make test task against the classic cluster.

Usage:
```bash
./run-tests.sh <make_test_task>
```

Example:
```bash
./run-tests.sh bazel_addindextest
```

## Configuration Files

The test cluster uses configuration files from `tests/realtikvtest/configs/classic/`:

- `tikv.toml`: Configuration for TiKV servers with transaction and storage settings

## Required Ports

The test cluster requires the following TCP ports to be available:

- PD: 2379, 2380, 2381, 2383, 2384
- TiKV: 20160, 20161, 20162, 20180, 20181, 20182

## Cleanup

The scripts automatically clean up processes on exit. Different cleanup approaches are used depending on the operating system:
- On macOS: `killall -9 -q <process>`
- On Linux: `killall -9 -r -q <process>` (with regex support)

Temporary data directories created during test runs are not automatically removed.

## Requirements

- Go development environment
- TiDB development environment with compiled binaries in `bin/` directory
- Available ports as listed above
- Sufficient disk space for temporary data directories

## Classic vs Next-Gen Testing

The classic architecture is the traditional TiDB architecture, which differs from the next-gen architecture in several ways:

- No distributed file system (DFS) integration
- No TiKV-Worker component
- Simplified configuration without S3 storage requirements

To test the next-gen architecture instead, see the scripts in the `next-gen` directory.
