# IntegrationTest

This directory contains the integration test suite for TiDB, including tools and test cases for validating TiDB's execution plan logic and end-to-end behavior. You can run these tests using either the classic `run-tests.sh` script or the next-generation `run-tests-next-gen.sh` script, which runs tests against a real TiKV cluster.

---

## Quick Start

### 1. Classic Integration Test Runner

Run all integration tests with the default configuration:

```sh
./run-tests.sh
```

### 2. Next-Generation Runner (with Real TiKV Cluster)

To run integration tests against a real TiKV cluster, use:

```sh
./run-tests-next-gen.sh [run-tests.sh options]
```

This script sets up a real cluster environment and then invokes `run-tests.sh` with your provided options.

---

## Script Options

### `run-tests.sh` Options

```
Usage: ./run-tests.sh [options]

    -h: Print this help message.

    -d <y|Y|n|N|b|B>: Control new collation feature:
        "y" or "Y": Only enable new collation during test.
        "n" or "N": Only disable new collation during test.
        "b" or "B": For tests prefixed with `collation`, run both enable/disable; for others, only enable [default].

    -s <tidb-server-path>: Use the specified tidb-server binary for testing.
        Example: ./run-tests.sh -s ./integrationtest_tidb-server

    -b <y|Y|n|N>: Build test binaries:
        "y" or "Y": Build binaries [default].
        "n" or "N": Do not build.
        (Building is skipped if -s is provided.)

    -r <test-name>|all: Run and record results for the specified test case(s).
        Example: ./run-tests.sh -r select
        Use "all" to record results for all tests.

    -t <test-name>: Run the specified test case(s) (ignored if -r is provided).
        Example: ./run-tests.sh -t select

    -v <vendor-path>: Add <vendor-path> to $GOPATH.

    -p <portgenerator-path>: Use the specified port generator for port allocation.
```

### `run-tests-next-gen.sh` Notes

- This script bootstraps a real TiKV cluster and then runs `run-tests.sh`.
- It requires several TCP ports to be available:
    - pd: 2379, 2380, 2381, 2383, 2384
    - tikv: 20160, 20161, 20162, 20180, 20181, 20182
    - tikv-worker: 19000
- All options supported by `run-tests.sh` can be passed to `run-tests-next-gen.sh`.

---

## How It Works

- Test cases are defined in `t/*.test`.
- Each test is executed against a TiDB server (with or without a real TiKV backend).
- Results are compared with expected outputs in `r/*.result`.
- Statistics are loaded from `s/*.json`.
- To generate new `.result` and `.json` files from execution, use the `-r` parameter.

---

## Typical Workflows

### Regression Testing After Code Changes

After modifying TiDB code, run:

```sh
make dev
```
or
```sh
make integrationtest
```

This will run the integration tests and help you identify any changes in execution plans.

### Adding or Updating Test Cases

1. Add a new `.test` file to the `t/` directory, or append queries to an existing file.
2. Generate new expected results by running:

    ```sh
    cd tests/integrationtest
    ./run-tests.sh -r [casename]
    ```

---

## Debugging Integration Tests

### Visual Studio Code

1. Add a configuration to `.vscode/launch.json` (example below). You can adjust the config file or backend as needed.
> If you want to change some configurations, such as using `TiKV` to run integration tests or others, you can modify `pkg/config/config.toml.example`.
    ```json
    {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "Debug TiDB With Default Config",
                "type": "go",
                "request": "launch",
                "mode": "auto",
                "program": "${fileWorkspaceFolder}/cmd/tidb-server",
                "args": ["--config=${fileWorkspaceFolder}/pkg/config/config.toml.example"]
            }
        ]
    }
    ```

2. Start TiDB-Server using the **Run and Debug** view or press **F5**.
3. Connect with any MySQL client (default: port 4000, user `root`, no password):

    ```sh
    mysql --comments --host 127.0.0.1 --port 4000 -u root
    ```

### Goland

See the [TiDB Dev Guide](https://pingcap.github.io/tidb-dev-guide/get-started/setup-an-ide.html#run-or-debug) for instructions on running or debugging TiDB-Server with or without TiKV. Use any MySQL client to run SQLs.

---

## Notes

- Use `run-tests-next-gen.sh` for testing with a real TiKV cluster.
- Use `run-tests.sh` for classic or local integration testing.
- For more details on test case structure and contributing new tests, see the comments and examples in the `t/` directory.

---
