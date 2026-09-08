# Statement context Go reading inventory

Authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`.
All four tracked artifacts (2455 lines) were read in full before any production edit in this audit. No nested package, fixture, generated/platform variant, doc.go, or other tracked build artifact exists in this directory.

| Artifact | Lines | Blob | Read |
| --- | ---: | --- | --- |
| pkg/sessionctx/stmtctx/BUILD.bazel | 67 | 58c2bbd7a4110509889d5e4c2480e029cf094d8e | complete |
| pkg/sessionctx/stmtctx/main_test.go | 34 | f34da9ae0cc07f73d02b9fd6fa0a92c301eeaa64 | complete |
| pkg/sessionctx/stmtctx/stmtctx.go | 1713 | 09486ba0d32e1fef88b64f952195cf8fdcee4b3e | complete |
| pkg/sessionctx/stmtctx/stmtctx_test.go | 641 | e6115eaa339c5e0e507346dbf376467d291a72a2 | complete |

## Declaration inventory

### pkg/sessionctx/stmtctx/BUILD.bazel


### pkg/sessionctx/stmtctx/main_test.go

- `func TestMain(m *testing.M)`

### pkg/sessionctx/stmtctx/stmtctx.go

- `func AllocateTaskID() uint64`
- `func (rf *ReferenceCount) TryIncrease() bool`
- `func (rf *ReferenceCount) Decrease()`
- `func (rf *ReferenceCount) TryFreeze() bool`
- `func (rf *ReferenceCount) UnFreeze()`
- `func (r *ReservedRowIDAlloc) Reset(base int64, maxv int64)`
- `func (r *ReservedRowIDAlloc) Consume() (int64, bool)`
- `func (r *ReservedRowIDAlloc) Exhausted() bool`
- `func (mu *stmtCtxMu) reset() *stmtCtxMu`
- `func (s *staleTSOProvider) reset() *staleTSOProvider`
- `func (s *stmtCache) reset() *stmtCache`
- `func NewStmtCtx() *StatementContext`
- `func NewStmtCtxWithTimeZone(tz *time.Location) *StatementContext`
- `func (sc *StatementContext) Reset() bool`
- `func (sc *StatementContext) SaveLogicalPlanBuildState() LogicalPlanBuildState`
- `func (sc *StatementContext) RestoreLogicalPlanBuildState(state LogicalPlanBuildState)`
- `func (sc *StatementContext) ResetAlternativeLogicalPlanSignals()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanDecorrelatedApply()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanSameOrderIndexJoin()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanOrderAwareJoinReorder()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanPreferCorrelate()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanSemiJoinRewrite()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanMixedStorageEngines()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanMissingTiFlashPath()`
- `func (sc *StatementContext) MarkAlternativeLogicalPlanHasStoreTypeHint()`
- `func (sc *StatementContext) CtxID() uint64`
- `func (sc *StatementContext) TimeZone() *time.Location`
- `func (sc *StatementContext) SetTimeZone(tz *time.Location)`
- `func (sc *StatementContext) TypeCtx() types.Context`
- `func (sc *StatementContext) ErrCtx() errctx.Context`
- `func (sc *StatementContext) SetErrLevels(otherLevels errctx.LevelMap)`
- `func (sc *StatementContext) ErrLevels() errctx.LevelMap`
- `func (sc *StatementContext) ErrGroupLevel(group errctx.ErrGroup) errctx.Level`
- `func (sc *StatementContext) TypeFlags() types.Flags`
- `func (sc *StatementContext) SetTypeFlags(flags types.Flags)`
- `func (sc *StatementContext) HandleTruncate(err error) error`
- `func (sc *StatementContext) HandleError(err error) error`
- `func (sc *StatementContext) HandleErrorWithAlias(internalErr, err, warnErr error) error`
- `func (sc *StatementContext) GetOrStoreStmtCache(key StmtCacheKey, value any) any`
- `func (sc *StatementContext) GetOrEvaluateStmtCache(key StmtCacheKey, valueEvaluator func() (any, error)) (any, error)`
- `func (sc *StatementContext) ResetInStmtCache(key StmtCacheKey)`
- `func (sc *StatementContext) ResetStmtCache()`
- `func (sc *StatementContext) SQLDigest() (normalized string, sqlDigest *parser.Digest)`
- `func (sc *StatementContext) InitSQLDigest(normalized string, digest *parser.Digest)`
- `func (sc *StatementContext) ResetSQLDigest(s string)`
- `func (sc *StatementContext) GetPlanDigest() (normalized string, planDigest *parser.Digest)`
- `func (sc *StatementContext) GetPlan() any`
- `func (sc *StatementContext) SetPlan(plan any)`
- `func (sc *StatementContext) GetFlatPlan() any`
- `func (sc *StatementContext) SetFlatPlan(flat any)`
- `func (sc *StatementContext) GetBinaryPlan() string`
- `func (sc *StatementContext) SetBinaryPlan(binaryPlan string)`
- `func (sc *StatementContext) GetResourceGroupTagger() *kv.ResourceGroupTagBuilder`
- `func (sc *StatementContext) SetUseChunkAlloc()`
- `func (sc *StatementContext) ClearUseChunkAlloc()`
- `func (sc *StatementContext) GetUseChunkAllocStatus() bool`
- `func (sc *StatementContext) SetPlanDigest(normalized string, planDigest *parser.Digest)`
- `func (sc *StatementContext) GetEncodedPlan() string`
- `func (sc *StatementContext) SetEncodedPlan(encodedPlan string)`
- `func (sc *StatementContext) GetPlanHint() (string, bool)`
- `func (sc *StatementContext) GetIndexForce() bool`
- `func (sc *StatementContext) InitDiskTracker(label int, bytesLimit int64)`
- `func (sc *StatementContext) InitMemTracker(label int, bytesLimit int64)`
- `func (sc *StatementContext) SetPlanHint(hint string)`
- `func (sc *StatementContext) SetIndexForce()`
- `func (sc *StatementContext) SetHintWarning(reason string)`
- `func (sc *StatementContext) SetHintWarningFromError(reason error)`
- `func (sc *StatementContext) AddAffectedRows(rows uint64)`
- `func (sc *StatementContext) SetAffectedRows(rows uint64)`
- `func (sc *StatementContext) AffectedRows() uint64`
- `func (sc *StatementContext) FoundRows() uint64`
- `func (sc *StatementContext) AddFoundRows(rows uint64)`
- `func (sc *StatementContext) RecordRows() uint64`
- `func (sc *StatementContext) AddRecordRows(rows uint64)`
- `func (sc *StatementContext) DeletedRows() uint64`
- `func (sc *StatementContext) AddDeletedRows(rows uint64)`
- `func (sc *StatementContext) UpdatedRows() uint64`
- `func (sc *StatementContext) AddUpdatedRows(rows uint64)`
- `func (sc *StatementContext) CopiedRows() uint64`
- `func (sc *StatementContext) AddCopiedRows(rows uint64)`
- `func (sc *StatementContext) TouchedRows() uint64`
- `func (sc *StatementContext) AddTouchedRows(rows uint64)`
- `func (sc *StatementContext) GetMessage() string`
- `func (sc *StatementContext) SetMessage(msg string)`
- `func (sc *StatementContext) GetWarnings() []SQLWarn`
- `func (sc *StatementContext) CopyWarnings(dst []SQLWarn) []SQLWarn`
- `func (sc *StatementContext) TruncateWarnings(start int) []SQLWarn`
- `func (sc *StatementContext) WarningCount() uint16`
- `func (sc *StatementContext) NumErrorWarnings() (ec uint16, wc int)`
- `func (sc *StatementContext) SetWarnings(warns []SQLWarn)`
- `func (sc *StatementContext) AppendWarning(warn error)`
- `func (sc *StatementContext) AppendWarnings(warns []SQLWarn)`
- `func (sc *StatementContext) AppendNote(warn error)`
- `func (sc *StatementContext) AppendError(warn error)`
- `func (sc *StatementContext) GetExtraWarnings() []SQLWarn`
- `func (sc *StatementContext) SetExtraWarnings(warns []SQLWarn)`
- `func (sc *StatementContext) AppendExtraWarning(warn error)`
- `func (sc *StatementContext) AppendExtraNote(warn error)`
- `func (sc *StatementContext) AppendExtraError(warn error)`
- `func (sc *StatementContext) resetMuForRetry()`
- `func (sc *StatementContext) ResetForRetry()`
- `func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails`
- `func (sc *StatementContext) PushDownFlags() uint64`
- `func PushDownFlagsWithTypeFlagsAndErrLevels(tcFlags types.Flags, errLevels errctx.LevelMap) uint64`
- `func (sc *StatementContext) InitFromPBFlagAndTz(flags uint64, tz *time.Location)`
- `func (sc *StatementContext) PessimisticLockStarted() bool`
- `func (sc *StatementContext) GetLockWaitStartTime() time.Time`
- `func (sc *StatementContext) UseDynamicPartitionPrune() bool`
- `func (sc *StatementContext) DetachMemDiskTracker()`
- `func (sc *StatementContext) SetStaleTSOProviderIfNotExist(eval func() (uint64, error))`
- `func (sc *StatementContext) GetStaleTSO() (uint64, error)`
- `func (sc *StatementContext) AddSetVarHintRestore(name, val string)`
- `func (sc *StatementContext) GetUsedStatsInfo(initIfNil bool) *UsedStatsInfo`
- `func (sc *StatementContext) RecordedStatsLoadStatusCnt() (cnt int)`
- `func (sc *StatementContext) TypeCtxOrDefault() types.Context`
- `func (sc *StatementContext) GetOrInitDistSQLFromCache(create func() *distsqlctx.DistSQLContext) *distsqlctx.DistSQLContext`
- `func (sc *StatementContext) GetOrInitRangerCtxFromCache(create func() any) any`
- `func (sc *StatementContext) GetOrInitBuildPBCtxFromCache(create func() any) any`
- `func (sc *StatementContext) GetResultRowsCount() (resultRows int64)`
- `func newErrCtx(tc types.Context, otherLevels errctx.LevelMap, handler contextutil.WarnAppender) errctx.Context`
- `func (s *UsedStatsInfoForTable) FormatForExplain() string`
- `func (s *UsedStatsInfoForTable) WriteToSlowLog(w io.Writer)`
- `func (s *UsedStatsInfoForTable) collectFromColOrIdxStatus(`
- `func (s *UsedStatsInfoForTable) recordedColIdxCount() int`
- `func (u *UsedStatsInfo) GetUsedInfo(tableID int64) *UsedStatsInfoForTable`
- `func (u *UsedStatsInfo) RecordUsedInfo(tableID int64, info *UsedStatsInfoForTable)`
- `func (u *UsedStatsInfo) Keys() []int64`
- `func (u *UsedStatsInfo) Values() []*UsedStatsInfoForTable`
- `func (r StatsLoadResult) HasError() bool`
- `func (r StatsLoadResult) ErrorMsg() string`
- `func WithStmtLabel(ctx context.Context, label string) context.Context`
- `func GetStmtLabel(ctx context.Context, node ast.StmtNode) string`

### pkg/sessionctx/stmtctx/stmtctx_test.go

- `func TestCopTasksDetails(t *testing.T)`
- `func TestStatementContextPushDownFLags(t *testing.T)`
- `func TestWeakConsistencyRead(t *testing.T)`
- `func TestMarshalSQLWarn(t *testing.T)`
- `func TestLogicalPlanBuildStateRestore(t *testing.T)`
- `func TestQBHintHandlerBuildState(t *testing.T)`
- `func TestApproxRuntimeInfo(t *testing.T)`
- `func TestStmtHintsClone(t *testing.T)`
- `func TestNewStmtCtx(t *testing.T)`
- `func TestSetStmtCtxTimeZone(t *testing.T)`
- `func TestSetStmtCtxTypeFlags(t *testing.T)`
- `func TestResetStmtCtx(t *testing.T)`
- `func TestStmtCtxID(t *testing.T)`
- `func TestIssue58600(t *testing.T)`
- `func TestErrCtx(t *testing.T)`
- `func TestReservedRowIDAlloc(t *testing.T)`
- `func TestUsedStatsInfoForTableWriteToSlowLog(t *testing.T)`
- `func BenchmarkErrCtx(b *testing.B)`

## Findings and next work

The Go context embeds RangeFallbackHandler and PlanCacheTracker. Construction and Reset bind fresh handlers to this statement; RestoreLogicalPlanBuildState recreates the fallback handler after restoring warnings and plan-cache state. ResetForRetry clears row counters, reserved IDs, table/index lists and warnings and refreshes TaskID/distSQL initialization; it is not equivalent to full Reset. Type flags control truncation even when other error levels change. Used-statistics formatting outputs indexes before columns, sorts IDs, and caps EXPLAIN details at three entries.

All seventeen Test functions, the ErrCtx benchmark and TestMain were read. Tests cover execution details, pushdown flags, weak consistency, warning serialization, logical build-state restoration, query-block hints, context construction/reset/locking, error levels, reserved IDs and statistics slow-log formatting. Reading is not execution or Rust parity evidence.

The range-limit value belongs to SessionVars and must also be inventoried through the variable/session sources before implementing the missing Rust transmission. This document establishes only complete Go source reading for stmtctx, not a completed port. No production change or Ready claim accompanies this checkpoint.

