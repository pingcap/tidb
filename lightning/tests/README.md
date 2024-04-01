# Integration tests

This folder contains all tests which relies on external processes such as TiDB.

## Preparations

1. The following 9 executables must be copied or linked into these locations:

    * `bin/tidb-server`
    * `bin/tikv-server`
    * `bin/pd-server`
    * `bin/pd-ctl`
    * `bin/go-ycsb`
    * `bin/minio`
    * `bin/mc`
    * `bin/tiflash`
    * `bin/cdc`

    The versions must be ≥2.1.0.

    What's more, there must be dynamic link library for TiFlash, see make target `bin` to learn more.
    You can install most of dependencies by running `download_tools.sh`.

2. The following programs must be installed:

    * `mysql` (the CLI client)
    * `curl`
    * `openssl`
    * `wget`
    * `lsof`

3. The user executing the tests must have permission to create the folder
    `/tmp/lightning_test`. All test artifacts will be written into this folder.

## Running

Link `bin` directory by `cd br && ln -s ../bin bin` and run `make br_integration_test` to execute the integration tests.
This command will

1. Build `br`
2. Check that all 9 required executables and `br` executable exist
3. Execute `tests/run.sh`
4. To start cluster with tiflash, please run `TIFLASH=1 tests/run.sh`

If the first two steps are done before, you could also run `tests/run.sh` directly.
This script will

1. Start PD, TiKV and TiDB in background with local storage
2. Find out all `tests/*/run.sh` and run it

Run `tests/run.sh --debug` to pause immediately after all servers are started.

## Writing new tests

1. New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
The script should exit with a nonzero error code on failure.
2. Add TEST_NAME to existing group in [run_group_lightning_tests.sh](./run_group_lightning_tests.sh)(Recommended), or add a new group for it.
3. If you add a new group, the name of the new group must be added to CI [br-integration-test](https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_br_integration_test.groovy) or [lightning-integration-test](https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_lightning_integration_test.groovy).

Several convenient commands are provided:

* `run_sql <SQL>` — Executes an SQL query on the TiDB database
* `run_lightning [CONFIG]` — Starts `tidb-lightning` using `tests/TEST_NAME/CONFIG.toml`
* `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
    (in `-E` format)
* `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
    text (in `-E` format)
