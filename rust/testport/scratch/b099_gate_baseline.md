# b099 gate baseline (recorded before any edits)

Command:
    cargo nextest run --locked -p tidb-planner -E 'not test(/bench/)' --no-fail-fast

Result (clean tree @ e849003bce):
    Summary [   3.583s] 1047 tests run: 1047 passed, 686 skipped

Failure set: EMPTY. (grep for FAIL/FLAKY/ABORT/TIMEOUT lines returned nothing.)

Scope (pkg/planner.part22 = items 1261-1278 of 1278 on origin/master):
  1261 pkg/planner/memo/group_test.go:196::TestGetInsertGroupImpl
  1262 pkg/planner/memo/group_test.go:214::TestFirstElemAfterDelete
  1263 pkg/planner/memo/group_test.go:233::TestBuildKeyInfo
  1264 pkg/planner/memo/group_test.go:287::TestExploreMark
  1265 pkg/planner/memo/main_test.go:24::TestMain
  1266 pkg/planner/planctx/context_test.go:26::TestContextDetach
  1267 pkg/planner/property/physical_property_test.go:26::TestNeedEnforceExchangerWithHashByEquivalence
  1268 pkg/planner/util/column_test.go:29::TestIndexInfo2Cols
  1269 pkg/planner/util/fixcontrol/fixcontrol_test.go:46::TestFixControl
  1270 pkg/planner/util/fixcontrol/fixcontrol_test.go:92::TestParseToMapEmptyValue
  1271 pkg/planner/util/fixcontrol/main_test.go:29::TestMain
  1272 pkg/planner/util/main_test.go:24::TestMain
  1273 pkg/planner/util/null_misc_test.go:36::TestNullRejectBuiltinRegistrySnapshot
  1274 pkg/planner/util/null_misc_test.go:57::TestIsNullRejectedProofModes
  1275 pkg/planner/util/path_test.go:30::TestCompareCol2Len
  1276 pkg/planner/util/path_test.go:81::TestOnlyPointRange
  1277 pkg/planner/util/slice_recursive_flatten_iter_test.go:37::TestSliceRecursiveFlattenIter
  1278 pkg/planner/util/utilfuncp/func_pointer_misc_test.go:26::TestCloneConstantsForPlanCacheWithNilEntry

Pre-existing complete Rust coverage found while re-deriving (to be cited,
not duplicated):
  - src/access_path.rs tests::test_only_point_range        -> item 1276 (in-gate)
  - src/fix_control.rs tests::empty_value_is_a_present_empty_string -> item 1270 (in-gate)
  - src/lib.rs recursive_flatten::tests::test_slice_recursive_flatten_iter -> item 1277 (in-gate)
  - crates/tidb-expr/src/expression.rs tests::{test_null_reject_builtin_registry_snapshot,
    test_is_null_rejected_proof_modes}                      -> items 1273+1274 (out-of-gate;
    verify separately with -p tidb-expr)
