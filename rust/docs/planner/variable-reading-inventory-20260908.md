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
| pkg/sessionctx/variable/noop.go | 649 | 9466e014911fdac5f8f570310fb6340eb6784ae8 | complete |
| pkg/sessionctx/variable/removed.go | 68 | f540f3894abe0e471186051ed20dfc8500482603 | complete |
| pkg/sessionctx/variable/removed_test.go | 29 | 5490a54250bd51b1956600e9024a7c665805d3e3 | complete |
| pkg/sessionctx/variable/sequence_state.go | 69 | a78daf52684176b9179a6cdd420b08a438c3796c | complete |
| pkg/sessionctx/variable/session.go | 4013 | 6ee9f24a21b915c42a1e10dd7378da90d5780474 | complete |
| pkg/sessionctx/variable/setvar_affect.go | 158 | be61d7f3c5b2cdba5565c88a9c7adddbfd5d63d3 | complete |
| pkg/sessionctx/variable/slow_log.go | 1216 | 79f7c7c7289b79620ed7a5edd809984c61b38054 | complete |
| pkg/sessionctx/variable/statusvar.go | 178 | 762693e4af842c17e1ee2377791abab3e631a72d | complete |
| pkg/sessionctx/variable/statusvar_test.go | 66 | 7336d821a228f19f4224bcc288791e6d2a68f068 | complete |
| pkg/sessionctx/variable/sysvar.go | 4404 | dc386aa826a9e35b860ace3577f8859cda667be8 | complete |
| pkg/sessionctx/variable/sysvar_test.go | 2422 | dacf87345db71e486ed229a15077b2c644494848 | complete |
| pkg/sessionctx/variable/tests/BUILD.bazel | 43 | 820f01709636ba61e6524d3b7b2f816fa39e3bf9 | complete |
| pkg/sessionctx/variable/tests/main_test.go | 35 | eaa52297b5d03dad004edfe4fa3754110bba36e3 | complete |
| pkg/sessionctx/variable/tests/session_test.go | 1083 | 25e5edf470bf49a5e2a71768b9309e0375985904 | complete |
| pkg/sessionctx/variable/tests/slowlog/BUILD.bazel | 25 | 2b31b0864bd6ff058784990530efc691f4bd46b6 | complete |
| pkg/sessionctx/variable/tests/slowlog/main_test.go | 34 | 09a52e707f12bd8c84d08eb4b49905518ec22dba | complete |
| pkg/sessionctx/variable/tests/slowlog/slow_log_test.go | 707 | 79afe57e8ef2442c4d71af175e772a6eed600895 | complete |
| pkg/sessionctx/variable/tests/variable_test.go | 743 | 74b2f17fc5b4d7a402473aa5b55d33fa9bee989a | complete |
| pkg/sessionctx/variable/tidb_vars.go | 69 | 30576c2c4373b63330c4de8446feb0c90c7abe72 | complete |
| pkg/sessionctx/variable/variable.go | 844 | b586c0965319f351b2f46bd2cbe292e1e1ebbb11 | complete |
| pkg/sessionctx/variable/varsutil.go | 557 | 0322d2fd948b13c93b73e320cd3a9d2ba9a0caac | complete |
| pkg/sessionctx/variable/varsutil_test.go | 728 | 413fbc73c3e46107ee9fe48fcb272289b9b3ef61 | complete |

The nested tests and tests/slowlog directories are separate packages, recorded here to avoid losing cross-package test coverage. The direct BUILD lists fourteen production sources and seven ordinary test sources; nextgen_test.go is a separate nextgen build-tag variant and is not listed in that ordinary test target. No doc.go or fixture/generated source was found in this tracked tree.

## Reading checkpoint

Current coverage: all 31 inventoried artifacts are completely read, including
the final variable_test.go (743 lines) and slowlog/slow_log_test.go (707 lines)
in two contiguous segments each. Earlier partial checkpoints are historical.
This completes source reading, not package parity or runtime validation.

variable_test.go covers catalog defaults, fixed index-join-build-v2 behavior,
error mappings, registration/hook failures, every generic validation type,
native types/flags, deprecation, naming/default/hook invariants, relaxed and
internal validation, instance scope/config mapping, global hooks, cache skips,
timezone handling and dependency ordering. Ordinary enums accept zero-based
numeric indexes; the TopN-specific rejection recorded above is custom behavior.
Relaxed validation preserves invalid values such as analyze version 1, while
fixed index-join-build-v2 returns ON. Dependency ordering retains unknown names.

slowlog tests inspect accessor coverage, AND conditions/OR rules, matching and
parsing all threshold kinds, KV detail snapshot ownership, absent detail matching
zero only, MVCC ratio zero-denominator behavior, signed-counter rejection,
session/global rule formats and duplicate last-value wins. Database/resource
group matching ignores case; digest/session alias matching does not. Global
empty rules produce a nonnil empty map and zero checksum. The checkRuleByField
helper does not assert that it finds a field, so calls with non-normalized names
cannot establish presence. Both test helpers and all subtests were read.
No Rust change, runtime test, Ready claim, commit or push in this checkpoint.

Newest checkpoint: tests/session_test.go is fully read in three contiguous
segments (1–400, 401–780, 781–1083). Only tests/variable_test.go and
tests/slowlog/slow_log_test.go remain pending. All direct artifacts stay complete.

Tests cover SET scope errors, retry counter reset, slow-log exact output and
field population, TiFlash RU totals/default weights, isolation engines from
configuration, table-delta cloning, savepoint replacement/rollback with lock
cache restoration, nonprepared cache entries, hook store context, chunk reuse,
concurrent user-variable migration, status bits, shard step resets, user-variable
removal, partial-ordered-TopN global/session inheritance, connection-attribute
bounds and cloud-storage URI validation/redaction. Partial-ordered-TopN accepts
only DISABLE/COST case-insensitively and rejects numeric enum indexes. Its SQL
test verifies new-session inheritance while existing sessions retain overrides.
The slow-log comparison helper deliberately skips UsedStats, warnings, timing,
RU metrics and other fields; its success alone cannot prove those fields.
TestHookContext ignores the invalid boolean SET error, so the hook assertion
is not evidence that the callback executes. These are inspection findings,
not runtime tests or Rust parity claims. Documentation stays local.

Latest checkpoint: all 24 direct artifacts are completely read, including
sysvar_test.go through EOF (2422). Both nested BUILD/TestMain pairs are also
read. Only tests/session_test.go, tests/variable_test.go and
tests/slowlog/slow_log_test.go remain pending. Re-enumeration confirms the
same 31 tracked artifacts. Earlier checkpoints below are chronological.

The final sysvar tests cover nextgen/classic lock gates and relaxed loading,
transaction retry normalization with deprecation warnings, schema-cache
byte-size bounds, hash-join version case handling, analyze concurrency
dependencies, bucket/TopN boundary clamping, analyze batch zero, isolation
aliases, scope masks and the skipInit allowlist. TestTiDBOptSelectivityFactor
compares a string result with floating-point values using NotEqual; those
assertions do not prove the numeric setting is correct. TestSkipInitIsUsed
explicitly protects global-to-session initialization and excludes RangeMaxSize
from its grandfathered allowlist. Nested BUILD targets use 47 and 10 shards;
their TestMain functions install common setup and distinct goleak exclusions.
No Rust behavior, runtime validation or publication is claimed.

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

## Compatibility catalog and slow-log reading checkpoint


At parent 620d61522f, read noop.go in three bounded segments (649 lines) and slow_log.go in four (1216 lines). Twenty-one direct artifacts are complete. The remaining direct files are session.go, sysvar.go and sysvar_test.go; nested tests, including slowlog tests, remain pending.

noop.go contains the complete noopSysVars catalog and inline validators, with no named functions. Scope, default, type, bounds, aliases, hint flags and validators were inspected entry by entry. Read-only transaction aliases, offline mode and server read-only settings delegate to checkReadOnly. SQLAutoIsNull separately enforces the same-scope noop setting, warning in WARN mode and failing in OFF mode. SecureAuth rejects OFF. CharacterSetFilesystem validates charset names. OptimizerSwitch has ScopeNone despite its hint flag; optimizer_search_depth, optimizer_prune_level and eq_range_index_dive_limit are compatibility catalog entries. A hint flag alone is not evidence of optimizer execution behavior.

slow_log.go declarations read: JSONSQLWarnForSlowLog, extractMsgFromSQLWarn, CollectWarningsForSlowLog, SlowQueryLogItems, kvExecDetailFormat, SessionVars.SlowLogFormat, writeSlowLogItem, SlowLogFieldAccessor, makeExecDetailAccessor, makeKVExecDetailAccessor, numericComparable, MatchEqual, matchGE, uint64FromNonNegative, matchZero, ParseString, parseInt64, parseUint64, parseFloat64, parseBool, SlowLogRuleFieldAccessors (all inline setters/matchers), ParseSlowLogFieldValue, parseSlowLogRuleEntry, parseSlowLogRuleSet, ParseSessionSlowLogRules, encodeRules and ParseGlobalSlowLogRules. All constants, the rule regex, sentinel and CRC64 table were read.

SlowLogFormat sorts used-statistics IDs and backoff names; statistics formatting delegates to UsedStatsInfoForTable.WriteToSlowLog. Ordinary warnings precede extra warnings and only the latter carry IsExtra. Warning and connection-attribute JSON disable HTML escaping. Optimizer phase timings include logical, physical, binding-match, stats-sync-wait and stats-derive. CurrentDBChanged is cleared after emitting use; SQL receives a semicolon only when absent. Nil KV detail emits zero fields. These are source observations pending Rust comparison.

Rule parsing rejects negative signed thresholds and nonfinite or negative floats. Missing execution detail matches only numeric zero; signed scan/write counters must be nonnegative before unsigned comparison. Database/resource-group equality is case-insensitive. Duplicate condition names overwrite earlier values; condition/map encoding order is not sorted. The ten-rule limit counts semicolon-separated pieces before empty rules are skipped. Session rules reject explicit Conn_ID; global rules allow it and checksum the encoded result. No production edit, regression execution or Ready validation is claimed for this prerequisite checkpoint.

## Session lifecycle reading checkpoint

Further reading covers sysvar_test.go 1201–1600. Resume at 1601 inside
TestTiDBAutoAnalyzeRatio. Inspected memory size normalization and percentage
failure, GC trigger bounds, global aggregation/TopN toggles, TTL schedule
timezone preservation, resource-control hooks and invalid auto-analyze ratio
preserving the previous value. No test execution or publication.

Local test-reading continuation: sysvar_test.go lines 1–1200 read completely;
resume at 1201 inside TestTiDBServerMemoryLimit2. The file is still partial.
Tests inspected include TiFlash signed overflow versus bounds clamping,
unsigned LastInsertID above MaxInt64, session-specific isolation validation,
skip-init behavior, noop-mode dependencies, timestamp bounds, DDL concurrency
and disk-quota bounds, and memory-limit minimum/disable/percentage behavior.
No runtime validation was performed. Documentation remains local pending a
verified Rust behavior batch.


Completed sysvar.go with contiguous final segments 3371–3590, 3591–3900 and 3901–4404. All 4404 lines, including every catalog entry and inline hook, are now read. Twenty-three direct artifacts are complete; sysvar_test.go and all nested test packages remain pending. The final named helpers GlobalSystemVariableInitialValue, setTiFlashComputeDispatchPolicy and setPipelinedDmlResourcePolicy were inspected completely. No Rust edits or tests in this local checkpoint.

Fix-control parsing occurs in the global/session setters rather than Validation to avoid duplicate checks; errors return before warnings or map replacement, while successful session parsing appends warnings and replaces the map. Runtime-filter mode uses the exact-string conversion helper, unlike the case-folding type helper. Plan-cache invalidation on fresh statistics and skipping stats invalidation for bindings assign separate flags. GlobalSystemVariableInitialValue overrides selected defaults for store type, test mode and kernel variant, but has no range-limit override. This preserves the distinction between catalog defaults, new-install defaults and raw session construction.

Final catalog observations: MPP exchange compression warns for explicit compression with version zero. Schema-cache resizing invokes its optional change hook before storing the new size/text. Session aliases truncate by runes to 64 and then shorten invalid identifiers, emitting at most one warning. DivPrecisionIncrement uses the positive-integer helper, including its zero fallback. Slow-log rule assignment marks effective fields dirty only after successful parsing. Pipelined DML custom settings build a temporary config and publish only after every parameter passes; duplicate keys overwrite earlier settings. LDAP, embedding, TTL, resource-control and transaction-file entries were also read as package prerequisites, not additional repair scope. Documentation remains local until a validated Rust batch.


Latest sysvar continuation read 2351–2700, 2701–3050 and 3051–3370 completely, including every inline hook. Resume at 3371 inside the partial-ordered-index-for-TopN setter. The range-limit declaration is now directly inspected in this contiguous pass: global/session TypeInt, minimum zero, maximum MaxInt64, catalog default DefTiDBOptRangeMaxSize, session setter assigning RangeMaxSize through TidbOptInt64 with that default. It has no skipInit or custom validation; the separately read hint allowlist enables SET_VAR. This completes the registration evidence, not the package prerequisite or transmission repair.

All cost-factor, correlation/risk, join-reorder, concurrency, partition and statistics registrations in this interval were read. Join-reorder threshold permits zero in validation but its setter uses the positive-integer helper, so zero selects its fallback default. Analyze version validation rejects the exact normalized string 1. Fast-analyze enabling warns that the feature was removed while still assigning the field. Index-join-build-v2 cannot be disabled and both getters always return ON. Partition-prune setters have different global/session warning behavior, including two warnings on a session static-to-dynamic transition. StatsLoadSyncWait has distinct global and session atomics.

MergePartitionStatsConcurrency always reads 1 through both getters, validates to 1 and warns for other normalized values; its setter is a no-op. This deliberately covers stale persisted values that bypass custom validation on read. Async global-statistics merge still assigns the requested boolean and warns. New-cost-interface, exchange-partition and TiFlash-read-for-write switches normalize to ON; these must not be conflated with switches that merely warn while preserving values. No Rust edits, regression runs or pushes in this local checkpoint. Twenty-two direct artifacts remain complete; sysvar.go is partial.


Sysvar continuation after the 1300-line checkpoint: read 1301–1650, 1651–2000 and 2001–2350 completely, including every inline validation/get/set hook. Resume at 2351 inside the broadcast-join threshold-size setter. Complete direct artifacts remain twenty-two. sysvar.go, sysvar_test.go and nested test packages still require completion before production edits.

This interval establishes that disabling the deprecated auto-analyze priority queue is rejected, while the deprecated column-tracking variable always reads ON and warns on validation. StatsCacheMemQuota invokes its capacity hook only on a changed value, after storing that value. AutoAnalyzeConcurrency requires both auto-analyze and its priority queue. Continuous plan-replayer capture checks historical-stats enablement both in validation and the session setter. Prepared cache size and session cache size are reciprocal aliases; the non-prepared size is separately deprecated without that alias. No parity is inferred from these declarations alone.

SQL-mode assignment updates the no-backslash server status bit. Charset/collation setters update their paired systems entry. ForeignKeyChecks mutates session state during validation. GroupConcatMaxLen has an additional 32-bit platform cap and truncation warning. MaxAllowedPacket rounds down to a 1024-byte multiple with a warning and has SQL SET restrictions that depend on scope and deployment mode. MPPStoreFailTTL validation returns the default with a deprecation warning. Memory-arbitrator and global memory controls were also read, preserving their exact parsing and hook ordering as source observations. No Rust edit, test execution, commit or push in this local reading continuation.


Latest local continuation: sysvar.go lines 1–1300 were read contiguously in segments 1–250, 251–600, 601–950 and 951–1300. Resume at 1301, inside the TiDBAnalyzeDefaultNumTopN setter. This file is not complete and does not increase the twenty-two fully read direct-artifact count. No Rust change or test execution in this continuation.

Named helpers read in this portion: normalizeIsolationReadEnginesValue, defaultIsolationReadEnginesValue, withAllowAutoValue, withMinValue, newEmbeddingAPIKeySysVar, maskEmbeddingAPIKey, newExecConcurrencySysVar, allowSetForeignKeyCheckInSharedLock and getForeignKeyCheckInSharedLockSession, plus every catalog entry and inline hook in the interval. The concurrency factory installs deprecation warnings and an auto-value fallback; foreign-key shared-lock reads prefer systems, then the global accessor, then the default.

Optimizer/statistics observations: projection, derived-TopN, aggregation and distinct-aggregation options assign separate fields. Isolation-engine validation normalizes names but its setter excludes TiFlash when the statement context says strict SQL mode already removed it. FoundInPlanCache/FoundInBinding getters expose previous statement flags. Auto-analyze ratio has a custom minimum 0.00001 with tolerance 1e-9; auto-analyze partition batch size warns that it is deprecated. Analyze-column options validate case-insensitively and the global setter stores uppercase. Statistics-owner switching stores the new state only after the switching hook succeeds. These remain source facts pending Rust comparison; the range-limit catalog entry has not yet been reached in the contiguous reading pass.


At parent fc53443c27, read all 4013 lines of session.go in ten contiguous bounded segments: 1–380, 381–780, 781–1180, 1181–1580, 1581–1980, 1981–2380, 2381–2780, 2781–3180, 3181–3590 and 3591–EOF. Twenty-two direct artifacts are now complete. sysvar.go, sysvar_test.go and nested test packages remain pending. All structs, fields, constants, interfaces and functions in those segments were read, including transaction/savepoint, user-variable, session, cache, concurrency and runtime-filter declarations.

RangeMaxSize is an int64 field with zero meaning unlimited, but NewSessionVars does not initialize it from the system-variable default. The eventual Rust repair must distinguish raw construction from the later variable-loading path instead of assuming the catalog default is assigned by this constructor. GetSessionOrGlobalSystemVar uses session hooks first, lazily loads global/session defaults into systems only where needed, and returns fixed values for ScopeNone. SetSystemVar validates before its hook. SetSystemVarWithOldStateAsRet validates first, obtains old state through GetStateValue or a full getter (not a raw potentially empty map lookup), and then applies the hook. Migration state getters avoid introducing uncached defaults.

InitStatementContext selects the other cached context, freezes its reference counter and attempts Reset; failure allocates a new context. MPP enforcement warnings are ordinary warnings in EXPLAIN and extra warnings otherwise. Plan-cache parameter rendering hides non-prepared parameters; LazyStmtText.Update copies the parameter slice for later formatting. PrepareDedupCacheKey incorporates SQL, charset, collation, database and little-endian SQL mode. String selectivity zero enables TopN estimation with defaults 0.1/0.9, while the legacy 0.8 setting remains 0.8 for both positive and negative forms. Temporary-table scan, network and seek cost factors return zero.

Transaction savepoints clone delta/cache maps and preserve specified fields; TableDelta.MergeFrom retains the earliest nonzero InitTime. Replica-read access prioritizes read-only/RC restrictions, deduplicates ignored-hint warnings and handles unavailable adaptive reads. Runtime-filter parsing deduplicates types without sorting, does not trim each comma element, and accepts only OFF/LOCAL modes despite a GLOBAL enum member. These are source observations pending Rust comparison, not new parity claims.

Per user instruction, documentation-only updates from this point remain local; receipt and ExecPlan will accompany a validated Rust fix when one is ready. No production edit or test execution is claimed for this reading checkpoint.

### session.go function declaration inventory


```text
97:func SetEnableAdaptiveReplicaRead(enabled bool) bool {
106:func IsAdaptiveReplicaReadEnabled() bool {
120:func (r *RetryInfo) Clean() {
130:func (r *RetryInfo) ResetOffset() {
136:func (r *RetryInfo) AddAutoIncrementID(id int64) {
141:func (r *RetryInfo) GetCurrAutoIncrementID() (int64, bool) {
146:func (r *RetryInfo) AddAutoRandomID(id int64) {
151:func (r *RetryInfo) GetCurrAutoRandomID() (int64, bool) {
160:func (r *retryInfoAutoIDs) resetOffset() {
164:func (r *retryInfoAutoIDs) clean() {
171:func (r *retryInfoAutoIDs) getCurrent() (int64, bool) {
267:func (s *SessionVars) RUV2Weights() execdetails.RUV2Weights {
274:func ruv2WeightsFromConfig(cfg config.RUV2Config) execdetails.RUV2Weights {
313:func NewRowIDShardGenerator(shardRand *rand.Rand, step int) *RowIDShardGenerator {
322:func (s *RowIDShardGenerator) SetShardStep(step int) {
328:func (s *RowIDShardGenerator) GetShardStep() int {
333:func (s *RowIDShardGenerator) GetCurrentShard(count int) int64 {
342:func (s *RowIDShardGenerator) updateShard(shardRand *rand.Rand) {
349:func (s *SessionVars) GetRowIDShardGenerator() *RowIDShardGenerator {
361:func (tc *TransactionContext) AddUnchangedKeyForLock(key []byte, shared bool) {
371:func (tc *TransactionContext) CollectUnchangedKeysForXLock(buf []kv.Key) []kv.Key {
381:func (tc *TransactionContext) CollectUnchangedKeysForSLock(buf []kv.Key) []kv.Key {
391:func (tc *TransactionContext) ResetUnchangedKeysForLock() {
398:func (tc *TransactionContext) UpdateDeltaForTable(
415:func (tc *TransactionContext) GetKeyInPessimisticLockCache(key kv.Key) (val []byte, ok bool) {
437:func (tc *TransactionContext) SetPessimisticLockCache(key kv.Key, val []byte) {
445:func (tc *TransactionContext) Cleanup() {
461:func (tc *TransactionContext) ClearDelta() {
468:func (tc *TransactionContext) GetForUpdateTS() uint64 {
476:func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
483:func (tc *TransactionContext) GetCurrentSavepoint() TxnCtxNeedToRestore {
497:func (tc *TransactionContext) RestoreBySavepoint(savepoint TxnCtxNeedToRestore) {
505:func (tc *TransactionContext) AddSavepoint(name string, memdbCheckpoint *tikv.MemDBCheckpoint) {
518:func (tc *TransactionContext) DeleteSavepoint(name string) bool {
530:func (tc *TransactionContext) ReleaseSavepoint(name string) bool {
542:func (tc *TransactionContext) RollbackToSavepoint(name string) *SavepointRecord {
556:func (tc *TransactionContext) FlushStmtPessimisticLockCache() {
581:func (ib *WriteStmtBufs) clean() {
609:func (r *RewritePhaseInfo) Reset() {
642:func NewTemporaryTableData(memBuffer kv.MemBuffer) TemporaryTableData {
650:func (d *temporaryTableData) GetTableSize(tblID int64) int64 {
658:func (d *temporaryTableData) DeleteTableKey(tblID int64, k kv.Key) error {
666:func (d *temporaryTableData) SetTableKey(tblID int64, k kv.Key, val []byte) error {
673:func (d *temporaryTableData) updateTblSize(tblID int64, beforeSize int) {
698:func (r ReadConsistencyLevel) IsWeak() bool {
702:func validateReadConsistencyLevel(val string) error {
735:func NewUserVars() *UserVars {
743:func (s *UserVars) Clone() UserVarsReader {
757:func (s *UserVars) SetUserVarVal(name string, dt types.Datum) {
764:func (s *UserVars) UnsetUserVar(varName string) {
773:func (s *UserVars) GetUserVarVal(name string) (types.Datum, bool) {
781:func (s *UserVars) SetUserVarType(name string, ft *types.FieldType) {
788:func (s *UserVars) GetUserVarType(name string) (*types.FieldType, bool) {
1943:func (s *SessionVars) ResetRelevantOptVarsAndFixes(record bool) {
1950:func (s *SessionVars) RecordRelevantOptVar(varName string) {
1961:func (s *SessionVars) RecordRelevantOptFix(fixID uint64) {
1972:func (s *SessionVars) GetSessionVars() *SessionVars {
1977:func (s *SessionVars) GetOptimizerFixControlMap() map[uint64]string {
1986:func (s *SessionVars) AddPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) {
1993:func (s *SessionVars) initializePlanReplayerFinishedTaskKey() {
1998:func (s *SessionVars) CheckPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) bool {
2008:func (s *SessionVars) IsPlanReplayerCaptureEnabled() bool {
2013:func (s *SessionVars) GetChunkAllocator() chunk.Allocator {
2022:func (s *SessionVars) ExchangeChunkStatus() {
2027:func (s *SessionVars) GetUseChunkAlloc() bool {
2032:func (s *SessionVars) SetAlloc(alloc chunk.Allocator) {
2050:func (s *SessionVars) IsAllocValid() bool {
2058:func (s *SessionVars) ClearAlloc(alloc *chunk.Allocator, hasErr bool) {
2069:func (s *SessionVars) GetPreparedStmtByName(stmtName string) (any, error) {
2078:func (s *SessionVars) GetPreparedStmtByID(stmtID uint32) (any, error) {
2087:func (s *SessionVars) InitStatementContext() *stmtctx.StatementContext {
2105:func (s *SessionVars) IsMPPAllowed() bool {
2110:func (s *SessionVars) IsTiFlashCopBanned() bool {
2115:func (s *SessionVars) IsMPPEnforced() bool {
2120:func (s *SessionVars) ChooseMppVersion() kv.MppVersion {
2128:func (s *SessionVars) ChooseMppExchangeCompressionMode() vardef.ExchangeCompressionMode {
2139:func (s *SessionVars) RaiseWarningWhenMPPEnforced(warning string) {
2151:func (s *SessionVars) CheckAndGetTxnScope() string {
2164:func (s *SessionVars) IsDynamicPartitionPruneEnabled() bool {
2170:func (s *SessionVars) IsRowLevelChecksumEnabled() bool {
2175:func (s *SessionVars) BuildParserConfig() parser.ParserConfig {
2184:func (s *SessionVars) AllocNewPlanID() int {
2189:func (s *SessionVars) GetTotalCostDuration() time.Duration {
2194:func (s *SessionVars) GetExecuteDuration() time.Duration {
2200:func (s *SessionVars) IsPartialOrderedIndexForTopNEnabled() bool {
2224:func (p PartitionPruneMode) Valid() bool {
2234:func (p PartitionPruneMode) Update() PartitionPruneMode {
2253:func NewPlanCacheParamList() *PlanCacheParamList {
2260:func (p *PlanCacheParamList) Reset() {
2266:func (p *PlanCacheParamList) String() string {
2275:func (p *PlanCacheParamList) Append(vs ...types.Datum) {
2280:func (p *PlanCacheParamList) SetForNonPrepCache(flag bool) {
2285:func (p *PlanCacheParamList) GetParamValue(idx int) types.Datum {
2290:func (p *PlanCacheParamList) AllParamValues() []types.Datum {
2304:func (s *LazyStmtText) SetText(text string) {
2310:func (s *LazyStmtText) Update(redact string, sql string, params *PlanCacheParamList) {
2322:func (s *LazyStmtText) String() string {
2369:func (connInfo *ConnectionInfo) IsSecureTransport() bool {
2378:func NewSessionVars(hctx HookContext) *SessionVars {
2608:func (s *SessionVars) GetAllowInSubqToJoinAndAgg() bool {
2616:func (s *SessionVars) SetAllowInSubqToJoinAndAgg(val bool) {
2621:func (s *SessionVars) GetAllowPreferRangeScan() bool {
2627:func (s *SessionVars) SetAllowPreferRangeScan(val bool) {
2632:func (s *SessionVars) GetEnableCascadesPlanner() bool {
2640:func (s *SessionVars) SetEnableCascadesPlanner(val bool) {
2645:func (s *SessionVars) GetEnableIndexMerge() bool {
2650:func (s *SessionVars) SetEnableIndexMerge(val bool) {
2655:func (s *SessionVars) GetEnablePseudoForOutdatedStats() bool {
2660:func (s *SessionVars) SetEnablePseudoForOutdatedStats(val bool) {
2665:func (s *SessionVars) GetReplicaRead() kv.ReplicaReadType {
2702:func (s *SessionVars) SetReplicaRead(val kv.ReplicaReadType) {
2707:func (s *SessionVars) IsReplicaReadClosestAdaptive() bool {
2712:func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
2717:func (s *SessionVars) GetSplitRegionTimeout() time.Duration {
2722:func (s *SessionVars) GetIsolationReadEngines() map[kv.StoreType]struct{} {
2727:func (s *SessionVars) CleanBuffers() {
2732:func (s *SessionVars) AllocPlanColumnID() int64 {
2737:func (s *SessionVars) RegisterScalarSubQ(scalarSubQ any) {
2750:func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
2757:func (s *SessionVars) GetParseParams() []parser.ParseParam {
2771:func (s *SessionVars) SetStringUserVar(name string, strVal string, collation string) {
2783:func (s *SessionVars) SetLastInsertID(insertID uint64) {
2791:func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
2816:func (s *SessionVars) HasStatusFlag(flag uint16) bool {
2821:func (s *SessionVars) Status() uint16 {
2827:func (s *SessionVars) SetInTxn(val bool) {
2835:func (s *SessionVars) InTxn() bool {
2840:func (s *SessionVars) IsAutocommit() bool {
2845:func (s *SessionVars) IsIsolation(isolation string) bool {
2859:func (s *SessionVars) IsolationLevelForNewTxn() (isolation string) {
2878:func (s *SessionVars) SetTxnIsolationLevelOneShotStateForNextTxn() {
2891:func (s *SessionVars) IsPessimisticReadConsistency() bool {
2896:func (s *SessionVars) GetNextPreparedStmtID() uint32 {
2902:func (s *SessionVars) SetNextPreparedStmtID(preparedStmtID uint32) {
2907:func (s *SessionVars) Location() *time.Location {
2916:func (s *SessionVars) GetSystemVar(name string) (string, bool) {
2926:func (s *SessionVars) setDDLReorgPriority(val string) {
2942:func (k planCacheStmtKey) Hash() []byte {
2947:func (s *SessionVars) AddNonPreparedPlanCacheStmt(sql string, stmt any) {
2955:func (s *SessionVars) GetNonPreparedPlanCacheStmt(sql string) any {
2969:func PrepareDedupCacheKey(sql, charset, collation, currentDB string, sqlMode mysql.SQLMode) string {
2977:func (s *SessionVars) GetPrepareStmtDedupCache(key string) any {
2987:func (s *SessionVars) SetPrepareStmtDedupCache(key string, val any) {
2995:func (s *SessionVars) AddPreparedStmt(stmtID uint32, stmt any) error {
3010:func (s *SessionVars) RemovePreparedStmt(stmtID uint32) {
3021:func (s *SessionVars) WithdrawAllPreparedStmt() {
3033:func (s *SessionVars) GetSessionOrGlobalSystemVar(ctx context.Context, name string) (string, error) {
3065:func (s *SessionVars) GetSessionStatesSystemVar(name string) (string, bool, error) {
3086:func (s *SessionVars) GetGlobalSystemVar(ctx context.Context, name string) (string, error) {
3098:func (s *SessionVars) SetSystemVar(name string, val string) error {
3111:func (s *SessionVars) SetSystemVarWithOldStateAsRet(name string, val string) (string, error) {
3143:func (s *SessionVars) SetSystemVarWithoutValidation(name string, val string) error {
3154:func (s *SessionVars) SetSystemVarWithRelaxedValidation(name string, val string) error {
3164:func (s *SessionVars) GetReadableTxnMode() string {
3173:func (s *SessionVars) SetPrevStmtDigest(prevStmtDigest string) {
3178:func (s *SessionVars) GetPrevStmtDigest() string {
3185:func (s *SessionVars) GetDivPrecisionIncrement() int {
3190:func (s *SessionVars) GetTemporaryTable(tblInfo *model.TableInfo) tableutil.TempTable {
3210:func (s *SessionVars) EncodeSessionStates(_ context.Context, sessionStates *sessionstates.SessionStates) (err error) {
3249:func (s *SessionVars) DecodeSessionStates(_ context.Context, sessionStates *sessionstates.SessionStates) (err error) {
3284:func (s *SessionVars) SetResourceGroupName(groupName string) {
3300:func (td *TableDelta) MergeFrom(incoming TableDelta) {
3311:func (td TableDelta) Clone() TableDelta {
3377:func (c *Concurrency) SetIndexLookupConcurrency(n int) {
3382:func (c *Concurrency) SetIndexLookupJoinConcurrency(n int) {
3387:func (c *Concurrency) SetDistSQLScanConcurrency(n int) {
3392:func (c *Concurrency) SetAnalyzeDistSQLScanConcurrency(n int) {
3397:func (c *Concurrency) SetHashJoinConcurrency(n int) {
3402:func (c *Concurrency) SetProjectionConcurrency(n int) {
3407:func (c *Concurrency) SetHashAggPartialConcurrency(n int) {
3412:func (c *Concurrency) SetHashAggFinalConcurrency(n int) {
3417:func (c *Concurrency) SetWindowConcurrency(n int) {
3422:func (c *Concurrency) SetMergeJoinConcurrency(n int) {
3427:func (c *Concurrency) SetStreamAggConcurrency(n int) {
3432:func (c *Concurrency) SetIndexMergeIntersectionConcurrency(n int) {
3437:func (c *Concurrency) IndexLookupConcurrency() int {
3445:func (c *Concurrency) IndexLookupJoinConcurrency() int {
3453:func (c *Concurrency) DistSQLScanConcurrency() int {
3458:func (c *Concurrency) AnalyzeDistSQLScanConcurrency() int {
3463:func (c *Concurrency) HashJoinConcurrency() int {
3471:func (c *Concurrency) ProjectionConcurrency() int {
3479:func (c *Concurrency) HashAggPartialConcurrency() int {
3487:func (c *Concurrency) HashAggFinalConcurrency() int {
3495:func (c *Concurrency) WindowConcurrency() int {
3503:func (c *Concurrency) MergeJoinConcurrency() int {
3511:func (c *Concurrency) StreamAggConcurrency() int {
3519:func (c *Concurrency) IndexMergeIntersectionConcurrency() int {
3527:func (c *Concurrency) UnionConcurrency() int {
3587:func NewTxnReadTS(ts uint64) *TxnReadTS {
3595:func (t *TxnReadTS) UseTxnReadTS() uint64 {
3604:func (t *TxnReadTS) SetTxnReadTS(ts uint64) {
3613:func (t *TxnReadTS) PeakTxnReadTS() uint64 {
3621:func (s *SessionVars) CleanupTxnReadTSIfUsed() {
3632:func (s *SessionVars) GetCPUFactor() float64 {
3637:func (s *SessionVars) GetCopCPUFactor() float64 {
3642:func (s *SessionVars) GetMemoryFactor() float64 {
3647:func (s *SessionVars) GetDiskFactor() float64 {
3652:func (s *SessionVars) GetConcurrencyFactor() float64 {
3658:func (s *SessionVars) GetNetworkFactor(tbl *model.TableInfo) float64 {
3669:func (s *SessionVars) GetScanFactor(tbl *model.TableInfo) float64 {
3680:func (s *SessionVars) GetDescScanFactor(tbl *model.TableInfo) float64 {
3691:func (s *SessionVars) GetSeekFactor(tbl *model.TableInfo) float64 {
3702:func (s *SessionVars) EnableEvalTopNEstimationForStrMatch() bool {
3708:func (s *SessionVars) GetStrMatchDefaultSelectivity() float64 {
3721:func (s *SessionVars) GetNegateStrMatchDefaultSelectivity() float64 {
3729:func (s *SessionVars) GetRelatedTableForMDL() *sync.Map {
3743:func (s *SessionVars) ClearRelatedTableForMDL() {
3750:func (s *SessionVars) EnableForceInlineCTE() bool {
3755:func (s *SessionVars) IsRuntimeFilterEnabled() bool {
3760:func (s *SessionVars) GetRuntimeFilterTypes() []RuntimeFilterType {
3765:func (s *SessionVars) GetRuntimeFilterMode() RuntimeFilterMode {
3771:func (s *SessionVars) GetMaxExecutionTime() uint64 {
3783:func (s *SessionVars) GetTiKVClientReadTimeout() uint64 {
3789:func (s *SessionVars) GetMaxKeysRead() uint64 {
3797:func (s *SessionVars) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
3802:func (s *SessionVars) GetDiskFullOpt() kvrpcpb.DiskFullOpt {
3807:func (s *SessionVars) ClearDiskFullOpt() {
3823:func (rfType RuntimeFilterType) String() string {
3839:func RuntimeFilterTypeStringToType(name string) (RuntimeFilterType, bool) {
3854:func ToRuntimeFilterType(sessionVarValue string) ([]RuntimeFilterType, bool) {
3884:func (rfMode RuntimeFilterMode) String() string {
3902:func RuntimeFilterModeStringToMode(name string) (RuntimeFilterMode, bool) {
3915:func (s *SessionVars) GetOptObjective() string {
3920:func ValidTiFlashPreAggMode() string {
3925:func ToTiPBTiFlashPreAggMode(mode string) (tipb.TiFlashPreAggMode, bool) {
3948:func (s *SessionVars) UseLowResolutionTSO() bool {
3955:func (s *SessionVars) PessimisticLockEligible() bool {
3968:func RemoveLockDDLJobs(sv *SessionVars, jobs map[int64]*mdldef.JobMDL, printLog bool) {
```
