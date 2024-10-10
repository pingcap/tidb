# IntegrationTest

IntegrationTest is a integration test command tool, also with some useful test cases for TiDB execute plan logic, we can run case via `run-tests.sh`.

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
