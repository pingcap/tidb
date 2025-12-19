# Unit Tests in pkg/planner that use format='plan_tree' and contain Column# references

These unit tests need to be regenerated/updated after removing column number suffixes from plan_tree format output.

## Test Functions (20 total)

### pkg/planner/core/casetest/physicalplantest/physical_plan_test.go
1. **TestAlwaysTruePredicateWithSubquery**
2. **TestExplainExpand**
3. **TestMPPHintsScope**

### pkg/planner/core/casetest/hint/hint_test.go
4. **TestAllViewHintType**
5. **TestIsolationReadTiFlashUseIndexHint**
6. **TestJoinHintCompatibility**
7. **TestReadFromStorageHint**
8. **TestReadFromStorageHintAndIsolationRead**

### pkg/planner/core/casetest/integration_test.go
9. **TestIssue31240**
10. **TestIssue32632**
11. **TestMergeContinuousSelections**
12. **TestPushDownGroupConcatToTiFlash** (test suite name in JSON, may be loaded by another test function)
13. **TestTiFlashExtraColumnPrune**
14. **TestTiFlashFineGrainedShuffle**
15. **TestTiFlashPartitionTableScan**

### pkg/planner/core/casetest/pushdown/push_down_test.go
16. **TestPushDownProjectionForTiFlash**
17. **TestPushDownProjectionForTiFlashCoprocessor**
18. **TestPushDownToTiFlashWithKeepOrder**
19. **TestPushDownToTiFlashWithKeepOrderInFastMode**
20. **TestSelPushDownTiFlash**

## Test Data Files Affected

The following JSON testdata files contain plan_tree format test cases with Column# references:
- `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_out.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_xut.json`
- `pkg/planner/core/casetest/hint/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/hint/testdata/integration_suite_xut.json`
- `pkg/planner/core/casetest/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/testdata/integration_suite_xut.json`
- `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_out.json`
- `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_xut.json`
- `pkg/planner/core/casetest/pushdown/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/pushdown/testdata/integration_suite_xut.json`
- `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_out.json`
- `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_xut.json`

## How to Regenerate

Run the test functions listed above. The test framework will automatically regenerate the JSON testdata files when tests are run with the `-record` flag or when `testdata.OnRecord()` is called.

Example:
```bash
cd pkg/planner/core/casetest/physicalplantest
go test -v -run TestExplainExpand -record
```
