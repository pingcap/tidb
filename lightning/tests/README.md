# Integration tests

This folder contains all tests which relies on external processes such as TiDB.

## Preparations

1. The following executables must be copied or linked into these locations:

    * `bin/tidb-server`
    * `bin/tikv-server`
    * `bin/pd-server`
    * `bin/tiflash`
    * `bin/minio`
    * `bin/mc`

    The versions must be ≥2.1.0.

    **Only some tests requires `minio`/`mc`** which can be downloaded from the official site, you can skip them if you don't need to run those cases.

    You can use `tiup` to download binaries related to TiDB cluster, and then link them to the `bin` directory:
    ```shell
    cluster_version=v8.1.0 # change to the version you need
    tiup install tidb:$cluster_version tikv:$cluster_version pd:$cluster_version tiflash:$cluster_version
    ln -s ~/.tiup/components/tidb/$cluster_version/tidb-server bin/tidb-server
    ln -s ~/.tiup/components/tikv/$cluster_version/tikv-server bin/tikv-server
    ln -s ~/.tiup/components/pd/$cluster_version/pd-server bin/pd-server
    ln -s ~/.tiup/components/tiflash/$cluster_version/tiflash/tiflash bin/tiflash
    ```

2. `make build_for_lightning_integration_test`
   
    `make server` to build the latest TiDB server if your test requires it.

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
