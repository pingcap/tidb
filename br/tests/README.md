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
    You can install most of dependencies by running `download_tools.sh`.

2. The following programs must be installed:

    * `mysql` (the CLI client)
    * `curl`
    * `openssl`
    * `wget`
    * `lsof`

3. The user executing the tests must have permission to create the folder
    `/tmp/backup_restore_test`. All test artifacts will be written into this folder.

If you have docker installed, you can skip step 1 and step 2 by running
`tests/up.sh --pull-images` to build and run a testing Docker container.

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

After executing the tests, run `make br_coverage` to get a coverage report at
`/tmp/backup_restore_test/all_cov.html`.

## Writing new tests

New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
The script should exit with a nonzero error code on failure.

Several convenient commands are provided:

* `run_sql <SQL>` — Executes an SQL query on the TiDB database
* `run_lightning [CONFIG]` — Starts `tidb-lightning` using `tests/TEST_NAME/CONFIG.toml`
* `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
    (in `-E` format)
* `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
    text (in `-E` format)
