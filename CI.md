# Commands to trigger ci pipeline

## Guide

1. ci pipeline will be triggered when your comment on pull request matched trigger phrase.

2. Some ci pipeline only response to people belong to specify organization like pingcap & tikv.

3. "**Only use trigger phrase for build triggering**". What does that mean?

   * Yes，this ci will be triggered only when your comment on pr matched trigger phrase.

   * No，this ci will be triggered by every new commit on current pr，comment matched trigger phrase also trigger ci pipeline.

## Commands

| ci pipeline                              | Command                                                      | Only use trigger phrase for build triggering |
| ---------------------------------------- | ------------------------------------------------------------ | -------------------------------------------- |
| tidb_ghpr_build                          | .*\/(merge\|run(-all-tests\|-build).*)                       | No                                           |
| tidb_ghpr_check                          | .*\/(merge\|run(-all-tests\|-check_dev).*)                   | No                                           |
| tidb_ghpr_check_2                        | .*\/(merge\|run(-all-tests\|-check_dev_2).*)                 | No                                           |
| tidb_ghpr_release_note                   | /(merge\|run-check-release-note\|run-check_release_note)     | No                                           |
| tidb_ghpr_unit_test                      | .*\/(merge\|run(-all-tests\|-unit-test).*)                   | Yes                                          |
| tidb_ghpr_coverage                       | .\/run-coverage.                                             | Yes                                          |
| tidb_ghpr_build_arm64                    | .*\/(run-build-arm64.*)                                      | Yes                                          |
| tidb_ghpr_common_test                    | .*\/(run(-integration-tests\|-common-test).*)                | Yes                                          |
| tidb_ghpr_integration_br_test            | .*\/(run(-integration-tests\|-integration-br-test).*)        | Yes                                          |
| tidb_ghpr_integration_campatibility_test | .*\/(run(-integration-tests\|-integration-compatibility-test).*) | Yes                                          |
| tidb_ghpr_integration_common_test        | .*\/(run(-integration-tests\|-integration-common-test).*)    | Yes                                          |
| tidb_ghpr_integration_copr_test          | .*\/(run-integration-tests\|run-integration-copr-test).*     | Yes                                          |
| tidb_ghpr_integration_ddl_test           | .*\/(run(-integration-tests\|-integration-ddl-test).*)       | Yes                                          |
| tidb_ghpr_monitor_test                   | .*\/run-monitor-test.*                                       | Yes                                          |
| tidb_ghpr_mybatis                        | .*\/(run(-integration-tests\|-mybatis-test).*)               | Yes                                          |
| tidb_ghpr_sqllogic_test_1                | .*\/(run(-integration-tests\|-sqllogic-test).*)              | Yes                                          |
| tidb_ghpr_sqllogic_test_2                | .*\/(run(-integration-tests\|-sqllogic-test).*)              | Yes                                          |
| tidb_ghpr_tics_test                      | .*\/(run(-integration-tests\|-tics-test).*)                  | Yes                                          |
| tidb_ghpr_unit_test                      | .*\/(merge\|run(-all-tests\|-unit-test).*)                   | Yes                                          |

