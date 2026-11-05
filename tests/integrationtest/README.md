# IntegrationTest

<<<<<<< HEAD
IntegrationTest is a integration test command tool, also with some useful test cases for TiDB execute plan logic, we can run case via `run-tests.sh`.
=======
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

### 3. Diagnostic Mode Test

Run the diagnostic-mode DDL integration test through the standard runner:

```sh
./run-tests.sh -t ddl/diagnostic_mode
```

The standard runner bootstraps a dedicated persistent UniStore with a normal
TiDB, stops that server, and then starts TiDB with `--diagnostic-mode` against
the same store. A full classic test run executes this case separately after the
regular cases.

The diagnostic test also verifies that the diagnostic TiDB log does not contain
`start DDL`. Missing, unreadable or empty logs fail the test. This case does not
support `-P`, because the runner must capture the diagnostic startup log.

### 4. External Servers and Reused TiKV Stores

Golden results require `tidb_redact_log = OFF` to preserve literal values in
errors and EXPLAIN output. Before using `-P <port>` with an existing test server,
connect to that server and run:

```sql
SET GLOBAL tidb_redact_log = OFF;
```

This changes the persisted global setting and disables log redaction on the
server. Use a dedicated test server; restore its previous setting afterward if
needed. The `-P` option skips server startup and does not apply `init.sql`.

For servers started by the runner, `-initialize-sql-file init.sql` applies this
setting only on first bootstrap. A reused TiKV store bootstrapped with this
`init.sql` already has the setting, unless it was subsequently changed. If the
store was bootstrapped without it, or redaction has since been enabled, run the
same statement against the test server before running golden cases.

---

## Script Options

### `run-tests.sh` Options
>>>>>>> 47dc1214658 (session, lightning: enable log redaction by default (#72666))

```
Usage: ./run-tests.sh [options]

    -h: Print this help message.

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           eg. "./run-tests.sh -s ./integrationtest_tidb-server"

    -b <y|Y|n|N>: "y" or "Y" for building test binaries [default "y" if this option is not specified].
                  "n" or "N" for not to build.
                  The building of tidb-server will be skiped if "-s <tidb-server-path>" is provided.

    -r <test-name>|all: Run tests in file "t/<test-name>.test" and record result to file "r/<test-name>.result".
                        "all" for running all tests and record their results.

    -t <test-name>: Run tests in file "t/<test-name>.test".
                    This option will be ignored if "-r <test-name>" is provided.
                    Run all tests if this option is not provided.

    -P <port>: Use an existing tidb-server; first set its global tidb_redact_log to OFF.
        See "External Servers and Reused TiKV Stores" above for persistence details.

    -v <vendor-path>: Add <vendor-path> to $GOPATH.

    -p <portgenerator-path>: Use port generator in <portgenerator-path> for generating port numbers.
```

## How it works

IntegrationTest will read test case in `t/*.test`, and execute them in TiDB server with `s/*.json` stat, and compare integration result in `r/*.result`.

For convenience, we can generate new `*.result` and `*.json` from execute by use `-r` parameter for `run-tests.sh`

## Usage

### Regression Execute Plan Modification

After modify code and before commit, please run this command under TiDB root folder.

```sh
make dev
```

or

```sh
make integrationtest
```

It will identify execute plan change.

### Generate New Result from Execute

Add a new test file in `t/` folder, file name should ended with `.test`. Or add new test queries at the end of some files. Then run shell below, it will generate results based on last execution.

```sh
cd tests/integrationtest
./run-tests.sh -r [casename]
```

## How to debug integration test

### Visual Studio Code

1. Add or create a configuration to `.vscode/launch.json`, an example is shown below. If you want to change some configurations, such as using `TiKV` to run integration tests or others, you can modify `pkg/config/config.toml.example`.

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

2. To run `TiDB-Server`, you could bring up **Run and Debug** view, select the **Run and Debug** icon in the **Activity Bar** on the side of VS Code. You can also use shortcut **F5**.

3. Use any mysql client application you like to run SQLs in integration test, the default port is `4000` and default user is `root` without password. Taking `mysql` as an example, you can use `mysql --comments --host 127.0.0.1 --port 4000 -u root` command to do this.

### Goland

You could follow <https://pingcap.github.io/tidb-dev-guide/get-started/setup-an-ide.html#run-or-debug> to run a `TiDB-Server` with or without `TiKV`. Then use any mysql client application you like to run SQLs.
