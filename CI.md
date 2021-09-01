# Commands to trigger ci pipeline

## Guide

1. ci pipeline will be triggered when your comment on pull request matched command.
2. "**Only triggered by command**". What does that mean?
   * Yes, this ci will be triggered only when your comment on pr matched command.
   * No, this ci will be triggered by every new commit on current pr, comment matched command also trigger ci pipeline.

## Commands

| ci pipeline                              | Commands                                                     | Only triggered by command |
| ---------------------------------------- | ------------------------------------------------------------ | ------------------------- |
| tidb_ghpr_build                          | /run-build<br />/run-all-tests<br />/merge                   | No                        |
| tidb_ghpr_check                          | /run-check_dev<br />/run-all-tests<br />/merge               | No                        |
| tidb_ghpr_check_2                        | /run-check_dev_2<br />/run-all-tests<br />/merge             | No                        |
| tidb_ghpr_coverage                       | /run-coverage                                                | Yes                       |
| tidb_ghpr_build_arm64                    | /run-build-arm64                                             | Yes                       |
| tidb_ghpr_common_test                    | /run-common-test<br />/run-integration-tests                 | Yes                       |
| tidb_ghpr_integration_br_test            | /run-integration-br-test<br />/run-integration-tests         | Yes                       |
| tidb_ghpr_integration_campatibility_test | /run-integration-compatibility-test<br />/run-integration-tests | Yes                       |
| tidb_ghpr_integration_common_test        | /run-integration-common-test<br />/run-integration-tests     | Yes                       |
| tidb_ghpr_integration_copr_test          | /run-integration-copr-test<br />/run-integration-tests       | Yes                       |
| tidb_ghpr_integration_ddl_test           | /run-integration-ddl-test<br />/run-integration-tests        | Yes                       |
| tidb_ghpr_monitor_test                   | /run-monitor-test                                            | Yes                       |
| tidb_ghpr_mybatis                        | /run-mybatis-test<br />/run-integration-tests                | Yes                       |
| tidb_ghpr_sqllogic_test_1                | /run-sqllogic-test<br />/run-integration-tests               | Yes                       |
| tidb_ghpr_sqllogic_test_2                | /run-sqllogic-test<br />/run-integration-tests               | Yes                       |
| tidb_ghpr_tics_test                      | /run-tics-test<br />/run-integration-tests                   | Yes                       |
| tidb_ghpr_unit_test                      | /run-unit-test<br />/run-all-tests<br />/merge               | Yes                       |

