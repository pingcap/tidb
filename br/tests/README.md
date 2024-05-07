# Unit tests

Unit tests (the `*_test.go` files inside the source directory) should *never* rely on external
programs.

Run `make br_unit_test` to execute all unit tests for br.

To run a specific test, pass `ARGS` into `make test` like

```sh
make br_unit_test ARGS='github.com/pingcap/tidb/br/pkg/cdclog --test.v --check.v --check.f TestColumn'
#                      ^~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ^~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                       which package to test                  more extra test flags
```

You can also run unit tests directly via `go test` like:

```sh
make failpoint-enable

go test github.com/pingcap/tidb/br/pkg/cdclog --test.v --check.v --check.f TestColumn

make failpoint-disable
```

but note that:

* failpoints must be toggled manually

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
    You can install most of dependencies by running following commands:

    ```sh
    cd ${WORKSPACE}/tidb
    rm -rf third_bin/
    rm -rf bin/
    br/tests/download_integration_test_binaries.sh
    mkdir -p bin && mv third_bin/* bin/
    rm -rf third_bin/
    make failpoint-enable && make && make failpoint-disable #make tidb
    ```

2. The following programs must be installed:

    * `mysql` (the CLI client)
    * `curl`
    * `openssl`
    * `wget`
    * `lsof`
    * `psmisc`

3. The user executing the tests must have permission to create the folder
    `/tmp/backup_restore_test`. All test artifacts will be written into this folder.

If you have docker installed, you can skip step 1 and step 2 by running
`br/tests/up.sh --pull-images` (in `tidb` directory) to build and run a testing Docker container.

## Running

1. Build `br.test` using `make build_for_br_integration_test`
2. Check that all 9 required executables and `br` executable exist
3. Select the tests to run using `export TEST_NAME="<test_name1> <test_name2> ..."` 
3. Execute `tests/run.sh`
<!-- 4. To start cluster with tiflash, please run `TIFLASH=1 tests/run.sh` -->

If the first two steps are done before, you could also run `tests/run.sh` directly.
This script will

1. Start PD, TiKV and TiDB in background with local storage
2. Find out all `tests/*/run.sh` and run it

Run `tests/run.sh --debug` to pause immediately after all servers are started.

After executing the tests, run `make br_coverage` to get a coverage report at
`/tmp/backup_restore_test/all_cov.html`.

## Writing new tests

1. New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
The script should exit with a nonzero error code on failure.
2. Add TEST_NAME to existing group in [run_group_br_tests.sh](./run_group_br_tests.sh)(Recommended), or add a new group for it.
3. If you add a new group, the name of the new group must be added to CI [br-integration-test](https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_br_integration_test.groovy).

Several convenient commands are provided:

* `run_sql <SQL>` — Executes an SQL query on the TiDB database
* `run_br` - Executes `br.test` with necessary settings
* `run_lightning [CONFIG]` — Starts `tidb-lightning` using `tests/TEST_NAME/CONFIG.toml`
* `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
    (in `-E` format)
* `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
    text (in `-E` format)
