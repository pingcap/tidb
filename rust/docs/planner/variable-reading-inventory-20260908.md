# Session variable package reading inventory

Go authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`.
This is a prerequisite for the planner range-limit transmission repair. Enumerating a file does not mean reading it. Production edits remain pending complete package reading.

| Artifact | Lines | Blob | Read status |
| --- | ---: | --- | --- |
| pkg/sessionctx/variable/BUILD.bazel | 134 | 5a0c79884f29404be6521624f903b6b3f11280e4 | complete |
| pkg/sessionctx/variable/OWNERS | 11 | bc24856b5d5093da82c8698e437c062b00dc3bcd | complete |
| pkg/sessionctx/variable/embedding_vars.go | 83 | e55a3e262859a509729ab6e2645a10f8a37ea05d | complete |
| pkg/sessionctx/variable/embedding_vars_test.go | 141 | b25ed580afde13fba058a94cbb93933b457863e4 | complete |
| pkg/sessionctx/variable/error.go | 52 | ec5a92cb4b9ee0e81083bdacaf495580fb8c487f | complete |
| pkg/sessionctx/variable/main_test.go | 34 | 90b6abe8a546ebaaecd6847065a51e7619ab4564 | complete |
| pkg/sessionctx/variable/mock_globalaccessor.go | 131 | 78449d3f4ee3c9f62e7b8e72c95fd743f149d10f | complete |
| pkg/sessionctx/variable/mock_globalaccessor_test.go | 57 | d0f4970f5227289671dd1a58bfc48b7505983d9a | complete |
| pkg/sessionctx/variable/nextgen_test.go | 84 | 7b5986b3293e257d0b276e8f957d6805343f5873 | complete |
| pkg/sessionctx/variable/noop.go | 649 | 9466e014911fdac5f8f570310fb6340eb6784ae8 | pending |
| pkg/sessionctx/variable/removed.go | 68 | f540f3894abe0e471186051ed20dfc8500482603 | complete |
| pkg/sessionctx/variable/removed_test.go | 29 | 5490a54250bd51b1956600e9024a7c665805d3e3 | complete |
| pkg/sessionctx/variable/sequence_state.go | 69 | a78daf52684176b9179a6cdd420b08a438c3796c | complete |
| pkg/sessionctx/variable/session.go | 4013 | 6ee9f24a21b915c42a1e10dd7378da90d5780474 | pending |
| pkg/sessionctx/variable/setvar_affect.go | 158 | be61d7f3c5b2cdba5565c88a9c7adddbfd5d63d3 | complete |
| pkg/sessionctx/variable/slow_log.go | 1216 | 79f7c7c7289b79620ed7a5edd809984c61b38054 | pending |
| pkg/sessionctx/variable/statusvar.go | 178 | 762693e4af842c17e1ee2377791abab3e631a72d | complete |
| pkg/sessionctx/variable/statusvar_test.go | 66 | 7336d821a228f19f4224bcc288791e6d2a68f068 | complete |
| pkg/sessionctx/variable/sysvar.go | 4404 | dc386aa826a9e35b860ace3577f8859cda667be8 | pending |
| pkg/sessionctx/variable/sysvar_test.go | 2422 | dacf87345db71e486ed229a15077b2c644494848 | pending |
| pkg/sessionctx/variable/tests/BUILD.bazel | 43 | 820f01709636ba61e6524d3b7b2f816fa39e3bf9 | pending |
| pkg/sessionctx/variable/tests/main_test.go | 35 | eaa52297b5d03dad004edfe4fa3754110bba36e3 | pending |
| pkg/sessionctx/variable/tests/session_test.go | 1083 | 25e5edf470bf49a5e2a71768b9309e0375985904 | pending |
| pkg/sessionctx/variable/tests/slowlog/BUILD.bazel | 25 | 2b31b0864bd6ff058784990530efc691f4bd46b6 | pending |
| pkg/sessionctx/variable/tests/slowlog/main_test.go | 34 | 09a52e707f12bd8c84d08eb4b49905518ec22dba | pending |
| pkg/sessionctx/variable/tests/slowlog/slow_log_test.go | 707 | 79afe57e8ef2442c4d71af175e772a6eed600895 | pending |
| pkg/sessionctx/variable/tests/variable_test.go | 743 | 74b2f17fc5b4d7a402473aa5b55d33fa9bee989a | pending |
| pkg/sessionctx/variable/tidb_vars.go | 69 | 30576c2c4373b63330c4de8446feb0c90c7abe72 | complete |
| pkg/sessionctx/variable/variable.go | 844 | b586c0965319f351b2f46bd2cbe292e1e1ebbb11 | complete |
| pkg/sessionctx/variable/varsutil.go | 557 | 0322d2fd948b13c93b73e320cd3a9d2ba9a0caac | complete |
| pkg/sessionctx/variable/varsutil_test.go | 728 | 413fbc73c3e46107ee9fe48fcb272289b9b3ef61 | complete |

The nested tests and tests/slowlog directories are separate packages, recorded here to avoid losing cross-package test coverage. The direct BUILD lists fourteen production sources and seven ordinary test sources; nextgen_test.go is a separate nextgen build-tag variant and is not listed in that ordinary test target. No doc.go or fixture/generated source was found in this tracked tree.

## Reading checkpoint

Completed BUILD, OWNERS, embedding configuration production/tests, error definitions, TestMain, nextgen tests and sequence state. Embedding normalization and configuration-version changes were read as package prerequisites, not targets for unrelated changes. Nextgen tests reject fair locking ON, bulk DML and nonleader replica reads. Sequence SetAllStates merges entries rather than replacing the existing map; preserve this source behavior in any later comparison. No Rust implementation or tests were changed in this checkpoint.

Next: mock-global-accessor production/tests, removed-variable production/tests, status-variable production/tests, then remaining large files in bounded segments. Complete function inventory as each remaining file is read. The range-size value, validation/setter, statement snapshot and fallback-warning/cache paths remain the intended repair scope.


## Second reading checkpoint

Seven additional direct artifacts read completely: mock_globalaccessor.go and its test, removed.go and its test, statusvar.go and its test, tidb_vars.go. Fifteen direct artifacts are now fully read. No production edits were made.

Mock global setters validate before invoking hooks, while SetGlobalSysVarOnly bypasses hooks/validation after checking registration. The test-mode accessor returns an unknown-variable error where the lightweight accessor returns an empty value. Removed-variable lookup is exact-name and returns the source-specific reason. Status collection stops on the first provider error; later providers overwrite duplicate names. Unregistration removes the last matching provider by swapping the final entry. Default statistics expose live connect-attribute counters and session keys examined. TiDB hook declarations include statistics cache capacity and statistics owner controls and must not be mistaken for implementations.

Remaining direct files: noop.go, session.go, setvar_affect.go, slow_log.go, sysvar.go, sysvar_test.go, variable.go, varsutil.go, varsutil_test.go. Nested test packages remain pending. The intended range-limit repair is still open.

## Generic validation reading checkpoint

variable.go is now read completely in bounded segments, bringing direct coverage to sixteen files. Its functions cover MV execution-variable capture/apply/restore, SysVar hook getters/setters, scope checks, type validation, relaxed validation, time/duration/integer/enum/float/bool checks, native result types, initialization/cache exclusions, registration and dependency ordering.

For range-limit inputs, checkInt64SystemVar parses the entire value first: parsing overflow is ErrWrongTypeForVar, while parsed values outside the configured bounds append ErrTruncatedWrongValue and return the boundary. AllowAutoValue accepts only the exact special string -1 before parsing. Valid integers retain their original spelling. Scope validation precedes type and custom validation. Relaxed validation restores the prior warning slice even when a validator emits warnings. Session hooks execute before systems updates; aliases skip validation and alias recursion. Global custom hooks return before generic alias processing. These must be preserved when wiring the range-limit session value.

Remaining direct files: noop.go, session.go, setvar_affect.go, slow_log.go, sysvar.go, sysvar_test.go, varsutil.go and varsutil_test.go; nested test packages also remain pending. This is reading evidence only, not a Rust fix or passing test claim.

## Hint and conversion helper reading checkpoint


At parent 9f99bd62f1, read all 158 lines of setvar_affect.go, all 557 lines of varsutil.go, and all 728 lines of varsutil_test.go (three bounded segments). Nineteen direct artifacts are now complete. Remaining direct files are noop.go, session.go, slow_log.go, sysvar.go and sysvar_test.go; all nested test packages remain pending.

setHintUpdatable marks only exact names in isHintUpdatableVerified and includes tidb_opt_range_max_size. The pending transmission regression must cover statement SET_VAR overrides as well as normal session values. The helper TidbOptInt64 returns its supplied default on parsing errors; that helper behavior must not replace SysVar validation before SET.

All 34 varsutil.go functions were read: BoolToOnOff, int32ToBoolStr, checkCollation, checkDefaultCollationForUTF8MB4, checkCharacterSet, checkReadOnly, checkIsolationLevel, getTiDBTableValue, setTiDBTableValue, trueFalseToOnOff, OnOffToTrueFalse, appendDeprecationWarning, TiDBOptOn, TiDBOptOnOffWarn, tidbOptAssertionLevel, tidbOptPositiveInt32, TidbOptInt, TidbOptInt64, TidbOptUint64, tidbOptFloat64, parseMemoryLimit, parsePercentage, parseByteSize, setSnapshotTS, parseTSFromNumberOrTime, setTxnReadTS, setReadStaleness, switchDDL, switchStats, collectAllowFuncName4ExpressionIndex, updatePasswordValidationLength, ValidAnalyzeSkipColumnTypes, ParseAnalyzeSkipColumnTypes and parseSchemaCacheSize. The expression-index and analyze-skip allowlists and memory bounds were also read.

Conversion details: TiDBOptOn accepts case-insensitive ON or exact 1; OnOffWarn switches on canonical uppercase values. Memory limits accept integer byte/unit and percentage forms with a nonzero lower-bound warning; schema cache additionally caps above MaxInt64. Snapshot setting clears TxnReadTS after parsing even on a parse error, while setTxnReadTS mutates only after successful timestamp conversion. Analyze-skip validation trims names and retains duplicates; the map parser does not trim each item. These are source observations, not new repair scope.

The ten tests read are TestTiDBOptOn, TestNewSessionVars, TestVarsutil, TestValidate, TestValidateStmtSummary, TestConcurrencyVariables, TestHelperFuncs, TestSessionStatesSystemVar, TestOnOffHelpers and TestAssertionLevel, plus assertFieldsGreaterThanZero. Tests cover session defaults, optimizer variable assignment, nextgen replica restrictions, warning clamping, timezone limits, scope errors, concurrency inheritance and state retention. No Rust edits or test execution are claimed for this reading checkpoint.
