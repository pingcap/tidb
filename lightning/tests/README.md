# Integration tests

This folder contains all tests which relies on external processes such as TiDB.

## Preparations

1. The following executables must be copied or linked into these locations:

    * `bin/tidb-server`
    * `bin/tikv-server`
    * `bin/pd-server`
    * `bin/pd-ctl`
    * `bin/minio`
    * `bin/mc`
    * `bin/tiflash`

    The versions must be ≥2.1.0.

    What's more, there must be dynamic link library for TiFlash, see make target `bin` to learn more.
    You can install most of dependencies by running `download_tools.sh`.

2. `make build_for_lightning_integration_test`

3. The following programs must be installed:

    * `mysql` (the CLI client)
    * `curl`
    * `openssl`
    * `wget`
    * `lsof`

4. The user executing the tests must have permission to create the folder
    `/tmp/lightning_test`. All test artifacts will be written into this folder.

## Running

Run `make lightning_integration_test` to execute all the integration tests.
- Logs will be written into `/tmp/lightning_test` directory.

Run `tests/run.sh --debug` to pause immediately after all servers are started.

If you only want to run some tests, you can use:
```shell
TEST_NAME="lightning_gcs lightning_view" lightning/tests/run.sh
```

Case names are separated by spaces.

## Writing new tests

1. New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
    - `TEST_NAME` should start with `lightning_`.
    - The script should exit with a nonzero error code on failure.
2. Add TEST_NAME to existing group in [run_group_lightning_tests.sh](./run_group_lightning_tests.sh)(Recommended), or add a new group for it.
3. If you add a new group, the name of the new group must be added to CI  [lightning-integration-test](https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_lightning_integration_test.groovy).

Several convenient commands are provided in [utils](../../tests/_utils/):

* `run_sql <SQL>` — Executes an SQL query on the TiDB database
* `run_lightning [CONFIG]` — Starts `tidb-lightning` using `tests/TEST_NAME/CONFIG.toml`
* `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
    (in `-E` format)
* `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
    text (in `-E` format)
