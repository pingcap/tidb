# Commands to trigger ci pipeline

## Guide

The CI pipeline will automatically trigger when you submit a PR or push new commits. There are also some CI pipelines that support manual triggering, usually for integration testing, which will give you more confidence in the quality of PR. You can obtain the trigger command for all supported CI pipelines by commenting `/test ?` in a PR.

## Commands

| ci pipeline                     | Commands                                | Tests                                                        | Auto Trigger |
| ------------------------------- | --------------------------------------- | ------------------------------------------------------------ | ------------ |
| build                           | `/test build`                           | Build binaries.                                              | Yes          |
| check-dev                       | `/test check-dev`                       | Some common check tasks including `lint`, `tidy` etc         | Yes          |
| check-dev2                      | `/test check-dev2`                      | All realtikv tests in `tests/realtikvtest`                   | Yes          |
| mysql-test                      | `/test mysql-test`                      | All mysql tests in PingCAP-QE/tidb-test `mysql_test`         | Yes          |
| unit-test                       | `/test unit-test`                       | All unit tests                                               | Yes          |
| pull-integration-ddl-test       | `/test pull-integration-ddl-test`       | All ddl tests in PingCAP-QE/tidb-test `ddl_test`             | Yes          |
| pull-mysql-client-test          | `/test pull-mysql-client-test`          | MySQL client tests in PingCAP-QE/tidb-test                   | Yes          |
| pull-br-integration-test        | `/test pull-br-integration-test`        | All br integration test in `br/tests`                        | No           |
| pull-lightning-integration-test | `/test pull-lightning-integration-test` | All lightning integration tests in `br/tests`                | No           |
| pull-common-test                | `/test pull-common-test`                | Some ORM tests performed through the unistore.               | No           |
| pull-e2e-test                   | `/test pull-e2e-test`                   | E2e tests in `tests/globalkilltest` and `tests/graceshutdown` | No           |
| pull-integration-common-test    | `/test pull-integration-common-test`    | Some ORM tests performed through the tikv                    | No           |
| pull-integration-copr-test      | `/test pull-integration-copr-test`      | Coprocessor tests in [tikv/copr-test](https://github.com/tikv/copr-test) | No           |
| pull-integration-nodejs-test    | `/test pull-integration-nodejs-test`    | Node.js ORM tests in PingCAP-QE/tidb-test                    | No           |
| pull-integration-jdbc-test      | `/test pull-integration-jdbc-test`      | All JDBC tests in PingCAP-QE/tidb-test                       | No           |
| pull-integration-mysql-test     | `/test pull-integration-mysql-test`     | All mysql tests in PingCAP-QE/tidb-test performed through the tikv | No           |
| pull-sqllogic-test              | `/test pull-sqllogic-test`              | SQL logic tests in PingCAP-QE/tidb-test                      | No           |
| pull-tiflash-test               | `/test pull-tiflash-test`               | TiFlash tests in pingcap/tiflash `tests/docker/`             | No           |

