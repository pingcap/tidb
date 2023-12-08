# Commands to trigger ci pipeline

## Guide

The CI pipeline will automatically trigger when you submit a PR or push new commits. There are also some CI pipelines that support manual triggering, usually for integration testing, which will give you more confidence in the quality of PR. You can obtain the trigger command for all supported CI pipelines by commenting `/test ?` in a PR.

## Commands

| ci pipeline                     | Commands                                | Tests                                                        |
| ------------------------------- | --------------------------------------- | ------------------------------------------------------------ |
| pull-br-integration-test        | `/test pull-br-integration-test`        | All br integration test in `br/tests`                        |
| pull-lightning-integration-test | `/test pull-lightning-integration-test` | All lightning integration tests in `br/tests`                |
| pull-common-test                | `/test pull-common-test`                | Some ORM tests performed through the unistore.               |
| pull-e2e-test                   | `/test pull-e2e-test`                   | E2e tests in `tests/globalkilltest` and `tests/graceshutdown` |
| pull-integration-common-test    | `/test pull-integration-common-test`    | Some ORM tests performed through the tikv                    |
| pull-integration-copr-test      | `/test pull-integration-copr-test`      | Coprocessor tests in [tikv/copr-test](https://github.com/tikv/copr-test) |
| pull-integration-ddl-test       | `/test pull-integration-ddl-test`       | All ddl tests in PingCAP-QE/tidb-test `ddl_test`             |
| pull-integration-jdbc-test      | `/test pull-integration-jdbc-test`      | All JDBC tests in PingCAP-QE/tidb-test                       |
| pull-integration-mysql-test     | `/test pull-integration-mysql-test`     | All mysql tests in PingCAP-QE/tidb-test performed through the tikv |
| pull-integration-nodejs-test    | `/test pull-integration-nodejs-test`    | Node.js ORM tests in PingCAP-QE/tidb-test                    |
| pull-mysql-client-test          | `/test pull-mysql-client-test`          | MySQL client tests in PingCAP-QE/tidb-test                   |
| pull-sqllogic-test              | `/test pull-sqllogic-test`              | SQL logic tests in PingCAP-QE/tidb-test                      |
| pull-tiflash-test               | `/test pull-tiflash-test`               | TiFlash tests in pingcap/tiflash `tests/docker/`             |

