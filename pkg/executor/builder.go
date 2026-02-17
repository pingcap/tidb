// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/executor/internal/calibrateresource"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/querywatch"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/unionexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/partitionpruning"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/cteutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	clientkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	err     error // err is set when there is error happened during Executor building process.
	hasLock bool
	Ti      *TelemetryInfo
	// isStaleness means whether this statement use stale read.
	isStaleness      bool
	txnScope         string
	readReplicaScope string
	inUpdateStmt     bool
	inDeleteStmt     bool
	inInsertStmt     bool
	inSelectLockStmt bool

	// forDataReaderBuilder indicates whether the builder is used by a dataReaderBuilder.
	// When forDataReader is true, the builder should use the dataReaderTS as the executor read ts. This is because
	// dataReaderBuilder can be used in concurrent goroutines, so we must ensure that getting the ts should be thread safe and
	// can return a correct value even if the session context has already been destroyed
	forDataReaderBuilder bool
	dataReaderTS         uint64

	// Used when building MPPGather.
	encounterUnionScan bool
}

// CTEStorages stores resTbl and iterInTbl for CTEExec.
// There will be a map[CTEStorageID]*CTEStorages in StmtCtx,
// which will store all CTEStorages to make all shared CTEs use same the CTEStorages.
type CTEStorages struct {
	ResTbl    cteutil.Storage
	IterInTbl cteutil.Storage
	Producer  *cteProducer
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema, ti *TelemetryInfo) *executorBuilder {
	txnManager := sessiontxn.GetTxnManager(ctx)
	return &executorBuilder{
		ctx:              ctx,
		is:               is,
		Ti:               ti,
		isStaleness:      staleread.IsStmtStaleness(ctx),
		txnScope:         txnManager.GetTxnScope(),
		readReplicaScope: txnManager.GetReadReplicaScope(),
	}
}

// MockExecutorBuilder is a wrapper for executorBuilder.
// ONLY used in test.
type MockExecutorBuilder struct {
	*executorBuilder
}

// NewMockExecutorBuilderForTest is ONLY used in test.
func NewMockExecutorBuilderForTest(ctx sessionctx.Context, is infoschema.InfoSchema, ti *TelemetryInfo) *MockExecutorBuilder {
	return &MockExecutorBuilder{
		executorBuilder: newExecutorBuilder(ctx, is, ti),
	}
}

// Build builds an executor tree according to `p`.
func (b *MockExecutorBuilder) Build(p base.Plan) exec.Executor {
	return b.build(p)
}

func (b *executorBuilder) build(p base.Plan) exec.Executor {
	if phyWrapper, ok := p.(*plannercore.PhysicalPlanWrapper); ok {
		p = phyWrapper.Inner
	}

	switch v := p.(type) {
	case nil:
		return nil
	case *plannercore.CheckTable:
		return b.buildCheckTable(v)
	case *plannercore.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *plannercore.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *plannercore.CheckIndexRange:
		return b.buildCheckIndexRange(v)
	case *plannercore.ChecksumTable:
		return b.buildChecksumTable(v)
	case *plannercore.ReloadExprPushdownBlacklist:
		return b.buildReloadExprPushdownBlacklist(v)
	case *plannercore.ReloadOptRuleBlacklist:
		return b.buildReloadOptRuleBlacklist(v)
	case *plannercore.AdminPlugins:
		return b.buildAdminPlugins(v)
	case *plannercore.DDL:
		return b.buildDDL(v)
	case *plannercore.Deallocate:
		return b.buildDeallocate(v)
	case *physicalop.Delete:
		return b.buildDelete(v)
	case *plannercore.Execute:
		return b.buildExecute(v)
	case *plannercore.Trace:
		return b.buildTrace(v)
	case *plannercore.Explain:
		return b.buildExplain(v)
	case *physicalop.PointGetPlan:
		return b.buildPointGet(v)
	case *physicalop.BatchPointGetPlan:
		return b.buildBatchPointGet(v)
	case *physicalop.Insert:
		return b.buildInsert(v)
	case *plannercore.ImportInto:
		return b.buildImportInto(v)
	case *plannercore.LoadData:
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		return b.buildLoadStats(v)
	case *plannercore.LockStats:
		return b.buildLockStats(v)
	case *plannercore.UnlockStats:
		return b.buildUnlockStats(v)
	case *plannercore.PlanReplayer:
		return b.buildPlanReplayer(v)
	case *plannercore.Traffic:
		return b.buildTraffic(v)
	case *physicalop.PhysicalLimit:
		return b.buildLimit(v)
	case *plannercore.Prepare:
		return b.buildPrepare(v)
	case *physicalop.PhysicalLock:
		return b.buildSelectLock(v)
	case *plannercore.CancelDDLJobs:
		return b.buildCancelDDLJobs(v)
	case *plannercore.PauseDDLJobs:
		return b.buildPauseDDLJobs(v)
	case *plannercore.ResumeDDLJobs:
		return b.buildResumeDDLJobs(v)
	case *plannercore.AlterDDLJob:
		return b.buildAlterDDLJob(v)
	case *plannercore.ShowNextRowID:
		return b.buildShowNextRowID(v)
	case *plannercore.ShowDDL:
		return b.buildShowDDL(v)
	case *physicalop.PhysicalShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plannercore.ShowDDLJobQueries:
		return b.buildShowDDLJobQueries(v)
	case *plannercore.ShowDDLJobQueriesWithRange:
		return b.buildShowDDLJobQueriesWithRange(v)
	case *plannercore.ShowSlow:
		return b.buildShowSlow(v)
	case *physicalop.PhysicalShow:
		return b.buildShow(v)
	case *plannercore.Simple:
		return b.buildSimple(v)
	case *plannercore.Set:
		return b.buildSet(v)
	case *plannercore.SetConfig:
		return b.buildSetConfig(v)
	case *physicalop.PhysicalSort:
		return b.buildSort(v)
	case *physicalop.PhysicalTopN:
		return b.buildTopN(v)
	case *physicalop.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *physicalop.Update:
		return b.buildUpdate(v)
	case *physicalop.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *physicalop.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *physicalop.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *physicalop.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *physicalop.PhysicalIndexMergeJoin:
		return b.buildIndexLookUpMergeJoin(v)
	case *physicalop.PhysicalIndexHashJoin:
		return b.buildIndexNestedLoopHashJoin(v)
	case *physicalop.PhysicalSelection:
		return b.buildSelection(v)
	case *physicalop.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *physicalop.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *physicalop.PhysicalProjection:
		return b.buildProjection(v)
	case *physicalop.PhysicalMemTable:
		return b.buildMemTable(v)
	case *physicalop.PhysicalTableDual:
		return b.buildTableDual(v)
	case *physicalop.PhysicalApply:
		return b.buildApply(v)
	case *physicalop.PhysicalMaxOneRow:
		return b.buildMaxOneRow(v)
	case *plannercore.Analyze:
		return b.buildAnalyze(v)
	case *physicalop.PhysicalTableReader:
		return b.buildTableReader(v)
	case *physicalop.PhysicalTableSample:
		return b.buildTableSample(v)
	case *physicalop.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *physicalop.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	case *physicalop.PhysicalWindow:
		return b.buildWindow(v)
	case *physicalop.PhysicalShuffle:
		return b.buildShuffle(v)
	case *physicalop.PhysicalShuffleReceiverStub:
		return b.buildShuffleReceiverStub(v)
	case *plannercore.SQLBindPlan:
		return b.buildSQLBindExec(v)
	case *plannercore.SplitRegion:
		return b.buildSplitRegion(v)
	case *plannercore.DistributeTable:
		return b.buildDistributeTable(v)
	case *physicalop.PhysicalIndexMergeReader:
		return b.buildIndexMergeReader(v)
	case *plannercore.SelectInto:
		return b.buildSelectInto(v)
	case *physicalop.PhysicalCTE:
		return b.buildCTE(v)
	case *physicalop.PhysicalCTETable:
		return b.buildCTETableReader(v)
	case *plannercore.CompactTable:
		return b.buildCompactTable(v)
	case *plannercore.AdminShowBDRRole:
		return b.buildAdminShowBDRRole(v)
	case *physicalop.PhysicalExpand:
		return b.buildExpand(v)
	case *plannercore.RecommendIndexPlan:
		return b.buildRecommendIndex(v)
	case *plannercore.WorkloadRepoCreate:
		return b.buildWorkloadRepoCreate(v)
	default:
		if mp, ok := p.(testutil.MockPhysicalPlan); ok {
			return mp.GetExecutor()
		}

		b.err = exeerrors.ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}


func (b *executorBuilder) buildSelectLock(v *physicalop.PhysicalLock) exec.Executor {
	if !b.inSelectLockStmt {
		b.inSelectLockStmt = true
		defer func() { b.inSelectLockStmt = false }()
	}
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	if !b.ctx.GetSessionVars().PessimisticLockEligible() {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	// If the `PhysicalLock` is not ignored by the above logic, set the `hasLock` flag.
	b.hasLock = true
	e := &SelectLockExec{
		BaseExecutor:       exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		Lock:               v.Lock,
		tblID2Handle:       v.TblID2Handle,
		tblID2PhysTblIDCol: v.TblID2PhysTblIDCol,
	}

	// filter out temporary tables because they do not store any record in tikv and should not write any lock
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	for tblID := range e.tblID2Handle {
		tblInfo, ok := is.TableByID(context.Background(), tblID)
		if !ok {
			b.err = errors.Errorf("Can not get table %d", tblID)
		}

		if tblInfo.Meta().TempTableType != model.TempTableNone {
			delete(e.tblID2Handle, tblID)
		}
	}

	return e
}

func (b *executorBuilder) buildLimit(v *physicalop.PhysicalLimit) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(min(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.SetInitCap(n)
	e := &LimitExec{
		BaseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}

	childSchemaLen := v.Children()[0].Schema().Len()
	childUsedSchema := markChildrenUsedCols(v.Schema().Columns, v.Children()[0].Schema())[0]
	e.columnIdxsUsedByChild = make([]int, 0, len(childUsedSchema))
	e.columnIdxsUsedByChild = append(e.columnIdxsUsedByChild, childUsedSchema...)
	if len(e.columnIdxsUsedByChild) == childSchemaLen {
		e.columnIdxsUsedByChild = nil // indicates that all columns are used. LimitExec will improve performance for this condition.
	} else {
		// construct a project evaluator to do the inline projection
		e.columnSwapHelper = chunk.NewColumnSwapHelper(e.columnIdxsUsedByChild)
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	return &PrepareExec{
		BaseExecutor: base,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) exec.Executor {
	e := &ExecuteExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.Params,
		stmt:         v.Stmt,
		plan:         v.Plan,
		outputNames:  v.OutputNames(),
	}

	failpoint.Inject("assertExecutePrepareStatementStalenessOption", func(val failpoint.Value) {
		vs := strings.Split(val.(string), "_")
		assertTS, assertReadReplicaScope := vs[0], vs[1]
		staleread.AssertStmtStaleness(b.ctx, true)
		ts, err := sessiontxn.GetTxnManager(b.ctx).GetStmtReadTS()
		if err != nil {
			panic(e)
		}

		if strconv.FormatUint(ts, 10) != assertTS ||
			assertReadReplicaScope != b.readReplicaScope {
			panic("execute prepare statement have wrong staleness option")
		}
	})

	return e
}

func (b *executorBuilder) buildShow(v *physicalop.PhysicalShow) exec.Executor {
	e := &ShowExec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Tp:                    v.Tp,
		CountWarningsOrErrors: v.CountWarningsOrErrors,
		DBName:                ast.NewCIStr(v.DBName),
		Table:                 v.Table,
		Partition:             v.Partition,
		Column:                v.Column,
		IndexName:             v.IndexName,
		ResourceGroupName:     ast.NewCIStr(v.ResourceGroupName),
		Flag:                  v.Flag,
		Roles:                 v.Roles,
		User:                  v.User,
		is:                    b.is,
		Full:                  v.Full,
		IfNotExists:           v.IfNotExists,
		GlobalScope:           v.GlobalScope,
		Extended:              v.Extended,
		Extractor:             v.Extractor,
		ImportJobID:           v.ImportJobID,
		DistributionJobID:     v.DistributionJobID,
		ImportGroupKey:        v.ImportGroupKey,
	}
	if e.Tp == ast.ShowMasterStatus || e.Tp == ast.ShowBinlogStatus {
		// show master status need start ts.
		if _, err := e.Ctx().Txn(true); err != nil {
			b.err = err
		}
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) exec.Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	case *ast.BRIEStmt:
		return b.buildBRIE(s, v.Schema())
	case *ast.CreateUserStmt, *ast.AlterUserStmt:
		var lockOptions []*ast.PasswordOrLockOption
		if b.Ti.AccountLockTelemetry == nil {
			b.Ti.AccountLockTelemetry = &AccountLockTelemetryInfo{}
		}
		b.Ti.AccountLockTelemetry.CreateOrAlterUser++
		if stmt, ok := v.Statement.(*ast.CreateUserStmt); ok {
			lockOptions = stmt.PasswordOrLockOptions
		} else if stmt, ok := v.Statement.(*ast.AlterUserStmt); ok {
			lockOptions = stmt.PasswordOrLockOptions
		}
		if len(lockOptions) > 0 {
			// Multiple lock options are supported for the parser, but only the last one option takes effect.
			for i := len(lockOptions) - 1; i >= 0; i-- {
				if lockOptions[i].Type == ast.Lock {
					b.Ti.AccountLockTelemetry.LockUser++
					break
				} else if lockOptions[i].Type == ast.Unlock {
					b.Ti.AccountLockTelemetry.UnlockUser++
					break
				}
			}
		}
	case *ast.CalibrateResourceStmt:
		return &calibrateresource.Executor{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), 0),
			WorkloadType: s.Tp,
			OptionList:   s.DynamicCalibrateResourceOptionList,
		}
	case *ast.AddQueryWatchStmt:
		return &querywatch.AddExecutor{
			BaseExecutor:         exec.NewBaseExecutor(b.ctx, v.Schema(), 0),
			QueryWatchOptionList: s.QueryWatchOptionList,
		}
	case *ast.ImportIntoActionStmt:
		return &ImportIntoActionExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, 0),
			tp:           s.Tp,
			jobID:        s.JobID,
		}
	case *ast.CancelDistributionJobStmt:
		return &CancelDistributionJobExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, 0),
			jobID:        uint64(s.JobID),
		}
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &SimpleExec{
		BaseExecutor:    base,
		Statement:       v.Statement,
		ResolveCtx:      v.ResolveCtx,
		IsFromRemote:    v.IsFromRemote,
		is:              b.is,
		staleTxnStartTS: v.StaleTxnStartTS,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &SetExecutor{
		BaseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildSetConfig(v *plannercore.SetConfig) exec.Executor {
	return &SetConfigExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		p:            v,
	}
}

func (b *executorBuilder) buildUnionScanExec(v *physicalop.PhysicalUnionScan) exec.Executor {
	oriEncounterUnionScan := b.encounterUnionScan
	b.encounterUnionScan = true
	defer func() {
		b.encounterUnionScan = oriEncounterUnionScan
	}()
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

func collectColIdxFromByItems(byItems []*plannerutil.ByItems, cols []*model.ColumnInfo) ([]int, error) {
	var colIdxs []int
	for _, item := range byItems {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, errors.Errorf("Not support non-column in orderBy pushed down")
		}
		for i, c := range cols {
			if c.ID == col.ID {
				colIdxs = append(colIdxs, i)
				break
			}
		}
	}
	return colIdxs, nil
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader exec.Executor, v *physicalop.PhysicalUnionScan) exec.Executor {
	// If reader is union, it means a partition table and we should transfer as above.
	if x, ok := reader.(*unionexec.UnionExec); ok {
		for i, child := range x.AllChildren() {
			x.SetChildren(i, b.buildUnionScanFromReader(child, v))
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanExec{BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle column index of the below Plan.
	us.handleCols = v.HandleCols
	us.mutableRow = chunk.MutRowFromTypes(exec.RetTypes(us))

	// If the push-downed condition contains virtual column, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.Children(0)
	}

	us.collators = make([]collate.Collator, 0, len(us.columns))
	for _, tp := range exec.RetTypes(us) {
		us.collators = append(us.collators, collate.GetCollator(tp.GetCollate()))
	}

	startTS, err := b.getSnapshotTS()
	sessionVars := b.ctx.GetSessionVars()
	if err != nil {
		b.err = err
		return nil
	}

	switch x := reader.(type) {
	case *MPPGather:
		us.desc = false
		us.keepOrder = false
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *TableReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		} else {
			for _, ic := range x.index.Columns {
				for i, col := range x.columns {
					if col.Name.L == ic.Name.L {
						us.usedIndex = append(us.usedIndex, i)
						break
					}
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.partitionIDMap = x.partitionIDMap
		us.table = x.table
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexLookUpExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		} else {
			for _, ic := range x.index.Columns {
				for i, col := range x.columns {
					if col.Name.L == ic.Name.L {
						us.usedIndex = append(us.usedIndex, i)
						break
					}
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.partitionIDMap = x.partitionIDMap
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexMergeReaderExecutor:
		if len(x.byItems) != 0 {
			us.keepOrder = x.keepOrder
			us.desc = x.byItems[0].Desc
			colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
			if err != nil {
				b.err = err
				return nil
			}
			us.usedIndex = colIdxes
			us.needExtraSorting = true
		}
		us.partitionIDMap = x.partitionIDMap
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
	case *PointGetExecutor, *BatchPointGetExec,
		// PointGet and BatchPoint can handle virtual columns and dirty txn data themselves.
		// If TableDual, the result must be empty, so we can skip UnionScan and use TableDual directly here.
		// TableSample only supports sampling from disk, don't need to consider in-memory txn data for simplicity.
		*TableDualExec,
		*TableSampleExecutor:
		return originReader
	default:
		// TODO: consider more operators like Projection.
		b.err = errors.NewNoStackErrorf("unexpected operator %T under UnionScan", reader)
		return nil
	}
	return us
}

type bypassDataSourceExecutor interface {
	dataSourceExecutor
	setDummy()
}

func (us *UnionScanExec) handleCachedTable(b *executorBuilder, x bypassDataSourceExecutor, vars *variable.SessionVars, startTS uint64) {
	tbl := x.Table()
	if tbl.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		cachedTable := tbl.(table.CachedTable)
		// Determine whether the cache can be used.
		leaseDuration := time.Duration(vardef.TableCacheLease.Load()) * time.Second
		cacheData, loading := cachedTable.TryReadFromCache(startTS, leaseDuration)
		if cacheData != nil {
			vars.StmtCtx.ReadFromTableCache = true
			x.setDummy()
			us.cacheTable = cacheData
		} else if loading {
			return
		} else if !b.inUpdateStmt && !b.inDeleteStmt && !b.inInsertStmt && !vars.StmtCtx.InExplainStmt {
			store := b.ctx.GetStore()
			cachedTable.UpdateLockForRead(context.Background(), store, startTS, leaseDuration)
		}
	}
}

// buildMergeJoin builds MergeJoinExec executor.
// `getSnapshotTS` returns for-update-ts if in insert/update/delete/lock statement otherwise the isolation read ts
// Please notice that in RC isolation, the above two ts are the same
func (b *executorBuilder) getSnapshotTS() (ts uint64, err error) {
	if b.forDataReaderBuilder {
		return b.dataReaderTS, nil
	}

	txnManager := sessiontxn.GetTxnManager(b.ctx)
	if b.inInsertStmt || b.inUpdateStmt || b.inDeleteStmt || b.inSelectLockStmt {
		return txnManager.GetStmtForUpdateTS()
	}
	return txnManager.GetStmtReadTS()
}

// getSnapshot get the appropriate snapshot from txnManager and set
// the relevant snapshot options before return.
func (b *executorBuilder) getSnapshot() (kv.Snapshot, error) {
	var snapshot kv.Snapshot
	var err error

	txnManager := sessiontxn.GetTxnManager(b.ctx)
	if b.inInsertStmt || b.inUpdateStmt || b.inDeleteStmt || b.inSelectLockStmt {
		snapshot, err = txnManager.GetSnapshotWithStmtForUpdateTS()
	} else {
		snapshot, err = txnManager.GetSnapshotWithStmtReadTS()
	}
	if err != nil {
		return nil, err
	}

	InitSnapshotWithSessCtx(snapshot, b.ctx, &b.readReplicaScope)
	return snapshot, nil
}

// InitSnapshotWithSessCtx initialize snapshot using session context
func InitSnapshotWithSessCtx(snapshot kv.Snapshot, ctx sessionctx.Context, txnReplicaReadTypePtr *string) {
	sessVars := ctx.GetSessionVars()
	replicaReadType := sessVars.GetReplicaRead()
	var txnReplicaReadType string
	if txnReplicaReadTypePtr == nil {
		txnManager := sessiontxn.GetTxnManager(ctx)
		txnReplicaReadType = txnManager.GetReadReplicaScope()
	} else {
		txnReplicaReadType = *txnReplicaReadTypePtr
	}
	snapshot.SetOption(kv.ReadReplicaScope, txnReplicaReadType)
	snapshot.SetOption(kv.TaskID, sessVars.StmtCtx.TaskID)
	snapshot.SetOption(kv.TiKVClientReadTimeout, sessVars.GetTiKVClientReadTimeout())
	snapshot.SetOption(kv.ResourceGroupName, sessVars.StmtCtx.ResourceGroupName)
	snapshot.SetOption(kv.ExplicitRequestSourceType, sessVars.ExplicitRequestSourceType)

	if replicaReadType.IsClosestRead() && txnReplicaReadType != kv.GlobalTxnScope {
		snapshot.SetOption(kv.MatchStoreLabels, []*metapb.StoreLabel{
			{
				Key:   placement.DCLabelKey,
				Value: txnReplicaReadType,
			},
		})
	}
}


func buildHandleColsForSplit(tbInfo *model.TableInfo) plannerutil.HandleCols {
	if tbInfo.IsCommonHandle {
		primaryIdx := tables.FindPrimaryIndex(tbInfo)
		tableCols := make([]*expression.Column, len(tbInfo.Columns))
		for i, col := range tbInfo.Columns {
			tableCols[i] = &expression.Column{
				ID:      col.ID,
				RetType: &col.FieldType,
			}
		}
		for i, pkCol := range primaryIdx.Columns {
			tableCols[pkCol.Offset].Index = i
		}
		return plannerutil.NewCommonHandleCols(tbInfo, primaryIdx, tableCols)
	}
	intCol := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	return plannerutil.NewIntHandleCols(intCol)
}

func (b *executorBuilder) buildDistributeTable(v *plannercore.DistributeTable) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(1)
	base.SetMaxChunkSize(1)
	return &DistributeTableExec{
		BaseExecutor:   base,
		tableInfo:      v.TableInfo,
		partitionNames: v.PartitionNames,
		rule:           v.Rule,
		engine:         v.Engine,
		is:             b.is,
		timeout:        v.Timeout,
	}
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(1)
	base.SetMaxChunkSize(1)
	if v.IndexInfo != nil {
		return &SplitIndexRegionExec{
			BaseExecutor:   base,
			tableInfo:      v.TableInfo,
			partitionNames: v.PartitionNames,
			indexInfo:      v.IndexInfo,
			lower:          v.Lower,
			upper:          v.Upper,
			num:            v.Num,
			valueLists:     v.ValueLists,
		}
	}
	handleCols := buildHandleColsForSplit(v.TableInfo)
	if len(v.ValueLists) > 0 {
		return &SplitTableRegionExec{
			BaseExecutor:   base,
			tableInfo:      v.TableInfo,
			partitionNames: v.PartitionNames,
			handleCols:     handleCols,
			valueLists:     v.ValueLists,
		}
	}
	return &SplitTableRegionExec{
		BaseExecutor:   base,
		tableInfo:      v.TableInfo,
		partitionNames: v.PartitionNames,
		handleCols:     handleCols,
		lower:          v.Lower,
		upper:          v.Upper,
		num:            v.Num,
	}
}

func (b *executorBuilder) buildUpdate(v *physicalop.Update) exec.Executor {
	b.inUpdateStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	multiUpdateOnSameTable := make(map[int64]bool)
	for _, info := range v.TblColPosInfos {
		tbl, _ := b.is.TableByID(context.Background(), info.TblID)
		if _, ok := tblID2table[info.TblID]; ok {
			multiUpdateOnSameTable[info.TblID] = true
		}
		tblID2table[info.TblID] = tbl
		if len(v.PartitionedTable) > 0 {
			// The v.PartitionedTable collects the partitioned table.
			// Replace the original table with the partitioned table to support partition selection.
			// e.g. update t partition (p0, p1), the new values are not belong to the given set p0, p1
			// Using the table in v.PartitionedTable returns a proper error, while using the original table can't.
			for _, p := range v.PartitionedTable {
				if info.TblID == p.Meta().ID {
					tblID2table[info.TblID] = p
				}
			}
		}
	}
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.SetInitCap(chunk.ZeroCapacity)
	var assignFlag []int
	assignFlag, b.err = getAssignFlag(b.ctx, v, selExec.Schema().Len())
	if b.err != nil {
		return nil
	}
	// should use the new tblID2table, since the update's schema may have been changed in Execstmt.
	b.err = plannercore.CheckUpdateList(assignFlag, v, tblID2table)
	if b.err != nil {
		return nil
	}
	updateExec := &UpdateExec{
		BaseExecutor:              base,
		OrderedList:               v.OrderedList,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		virtualAssignmentsOffset:  v.VirtualAssignmentsOffset,
		multiUpdateOnSameTable:    multiUpdateOnSameTable,
		tblID2table:               tblID2table,
		tblColPosInfos:            v.TblColPosInfos,
		assignFlag:                assignFlag,
		IgnoreError:               v.IgnoreError,
	}
	updateExec.fkChecks, b.err = buildTblID2FKCheckExecs(b.ctx, tblID2table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	updateExec.fkCascades, b.err = b.buildTblID2FKCascadeExecs(tblID2table, v.FKCascades)
	if b.err != nil {
		return nil
	}
	return updateExec
}

func getAssignFlag(ctx sessionctx.Context, v *physicalop.Update, schemaLen int) ([]int, error) {
	assignFlag := make([]int, schemaLen)
	for i := range assignFlag {
		assignFlag[i] = -1
	}
	for _, assign := range v.OrderedList {
		if !ctx.GetSessionVars().AllowWriteRowID && assign.Col.ID == model.ExtraHandleID {
			return nil, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported")
		}
		tblIdx, found := v.TblColPosInfos.FindTblIdx(assign.Col.Index)
		if found {
			colIdx := assign.Col.Index
			assignFlag[colIdx] = tblIdx
		}
	}
	return assignFlag, nil
}

func (b *executorBuilder) buildDelete(v *physicalop.Delete) exec.Executor {
	b.inDeleteStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	for _, info := range v.TblColPosInfos {
		tblID2table[info.TblID], _ = b.is.TableByID(context.Background(), info.TblID)
	}

	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.SetInitCap(chunk.ZeroCapacity)
	deleteExec := &DeleteExec{
		BaseExecutor:   base,
		tblID2Table:    tblID2table,
		IsMultiTable:   v.IsMultiTable,
		tblColPosInfos: v.TblColPosInfos,
		ignoreErr:      v.IgnoreErr,
	}
	deleteExec.fkChecks, b.err = buildTblID2FKCheckExecs(b.ctx, tblID2table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	deleteExec.fkCascades, b.err = b.buildTblID2FKCascadeExecs(tblID2table, v.FKCascades)
	if b.err != nil {
		return nil
	}
	return deleteExec
}

func (b *executorBuilder) updateForUpdateTS() error {
	// GetStmtForUpdateTS will auto update the for update ts if it is necessary
	_, err := sessiontxn.GetTxnManager(b.ctx).GetStmtForUpdateTS()
	return err
}

// retrieveColumnIdxsUsedByChild retrieve column indices map from child physical plan schema columns.
//
//	E.g. columnIdxsUsedByChild = [2, 3, 1] means child[col2, col3, col1] -> parent[col0, col1, col2].
//	`columnMissing` indicates whether one or more columns in `selfSchema` are not found in `childSchema`.
//	And `-1` in `columnIdxsUsedByChild` indicates the column not found.
//	If columnIdxsUsedByChild == nil, means selfSchema and childSchema are equal.
func retrieveColumnIdxsUsedByChild(selfSchema *expression.Schema, childSchema *expression.Schema) ([]int, bool) {
	equalSchema := (selfSchema.Len() == childSchema.Len())
	columnMissing := false
	columnIdxsUsedByChild := make([]int, 0, selfSchema.Len())
	for selfIdx, selfCol := range selfSchema.Columns {
		colIdxInChild := childSchema.ColumnIndex(selfCol)
		if !columnMissing && colIdxInChild == -1 {
			columnMissing = true
		}
		if equalSchema && selfIdx != colIdxInChild {
			equalSchema = false
		}
		columnIdxsUsedByChild = append(columnIdxsUsedByChild, colIdxInChild)
	}
	if equalSchema {
		columnIdxsUsedByChild = nil
	}
	return columnIdxsUsedByChild, columnMissing
}

// markChildrenUsedCols compares each child with the output schema, and mark
// each column of the child is used by output or not.
func markChildrenUsedCols(outputCols []*expression.Column, childSchemas ...*expression.Schema) (childrenUsed [][]int) {
	childrenUsed = make([][]int, 0, len(childSchemas))
	markedOffsets := make(map[int]int)
	// keep the original maybe reversed order.
	for originalIdx, col := range outputCols {
		markedOffsets[col.Index] = originalIdx
	}
	prefixLen := 0
	type intPair struct {
		first  int
		second int
	}
	// for example here.
	// left child schema: [col11]
	// right child schema: [col21, col22]
	// output schema is [col11, col22, col21], if not records the original derived order after physical resolve index.
	// the lused will be [0], the rused will be [0,1], while the actual order is dismissed, [1,0] is correct for rused.
	for _, childSchema := range childSchemas {
		usedIdxPair := make([]intPair, 0, len(childSchema.Columns))
		for i := range childSchema.Columns {
			if originalIdx, ok := markedOffsets[prefixLen+i]; ok {
				usedIdxPair = append(usedIdxPair, intPair{first: originalIdx, second: i})
			}
		}
		// sort the used idxes according their original indexes derived after resolveIndex.
		slices.SortFunc(usedIdxPair, func(a, b intPair) int {
			return cmp.Compare(a.first, b.first)
		})
		usedIdx := make([]int, 0, len(childSchema.Columns))
		for _, one := range usedIdxPair {
			usedIdx = append(usedIdx, one.second)
		}
		childrenUsed = append(childrenUsed, usedIdx)
		prefixLen += childSchema.Len()
	}
	return
}

func (*executorBuilder) corColInDistPlan(plans []base.PhysicalPlan) bool {
	for _, p := range plans {
		switch x := p.(type) {
		case *physicalop.PhysicalSelection:
			for _, cond := range x.Conditions {
				if len(expression.ExtractCorColumns(cond)) > 0 {
					return true
				}
			}
		case *physicalop.PhysicalProjection:
			for _, expr := range x.Exprs {
				if len(expression.ExtractCorColumns(expr)) > 0 {
					return true
				}
			}
		case *physicalop.PhysicalTopN:
			for _, byItem := range x.ByItems {
				if len(expression.ExtractCorColumns(byItem.Expr)) > 0 {
					return true
				}
			}
		case *physicalop.PhysicalTableScan:
			for _, cond := range x.LateMaterializationFilterCondition {
				if len(expression.ExtractCorColumns(cond)) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// corColInAccess checks whether there's correlated column in access conditions.
func (*executorBuilder) corColInAccess(p base.PhysicalPlan) bool {
	var access []expression.Expression
	switch x := p.(type) {
	case *physicalop.PhysicalTableScan:
		access = x.AccessCondition
	case *physicalop.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *executorBuilder) newDataReaderBuilder(p base.PhysicalPlan) (*dataReaderBuilder, error) {
	ts, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	builderForDataReader := *b
	builderForDataReader.forDataReaderBuilder = true
	builderForDataReader.dataReaderTS = ts

	return &dataReaderBuilder{
		plan:            p,
		executorBuilder: &builderForDataReader,
	}, nil
}


func (b *executorBuilder) buildSQLBindExec(v *plannercore.SQLBindPlan) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)

	e := &SQLBindExec{
		BaseExecutor: base,
		isGlobal:     v.IsGlobal,
		sqlBindOp:    v.SQLBindOp,
		details:      v.Details,
		isFromRemote: v.IsFromRemote,
	}
	return e
}

// NewRowDecoder creates a chunk decoder for new row format row value decode.
func NewRowDecoder(ctx sessionctx.Context, schema *expression.Schema, tbl *model.TableInfo) *rowcodec.ChunkDecoder {
	getColInfoByID := func(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
		for _, col := range tbl.Columns {
			if col.ID == colID {
				return col
			}
		}
		return nil
	}
	var pkCols []int64
	reqCols := make([]rowcodec.ColInfo, len(schema.Columns))
	for i := range schema.Columns {
		idx, col := i, schema.Columns[i]
		isPK := (tbl.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.GetFlag())) || col.ID == model.ExtraHandleID
		if isPK {
			pkCols = append(pkCols, col.ID)
		}
		isGeneratedCol := false
		if col.VirtualExpr != nil {
			isGeneratedCol = true
		}
		reqCols[idx] = rowcodec.ColInfo{
			ID:            col.ID,
			VirtualGenCol: isGeneratedCol,
			Ft:            col.RetType,
		}
	}
	if len(pkCols) == 0 {
		pkCols = tables.TryGetCommonPkColumnIds(tbl)
		if len(pkCols) == 0 {
			pkCols = []int64{-1}
		}
	}
	defVal := func(i int, chk *chunk.Chunk) error {
		if reqCols[i].ID < 0 {
			// model.ExtraHandleID, ExtraPhysTblID... etc
			// Don't set the default value for that column.
			chk.AppendNull(i)
			return nil
		}

		ci := getColInfoByID(tbl, reqCols[i].ID)
		d, err := table.GetColOriginDefaultValue(ctx.GetExprCtx(), ci)
		if err != nil {
			return err
		}
		chk.AppendDatum(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqCols, pkCols, defVal, ctx.GetSessionVars().Location())
}

func (b *executorBuilder) buildBatchPointGet(plan *physicalop.BatchPointGetPlan) exec.Executor {
	var err error
	if err = b.validCanReadTemporaryOrCacheTable(plan.TblInfo); err != nil {
		b.err = err
		return nil
	}

	if plan.Lock && !b.inSelectLockStmt {
		b.inSelectLockStmt = true
		defer func() {
			b.inSelectLockStmt = false
		}()
	}
	handles, isTableDual, err := plan.PrunePartitionsAndValues(b.ctx)
	if err != nil {
		b.err = err
		return nil
	}
	if isTableDual {
		// No matching partitions
		return &TableDualExec{
			BaseExecutorV2: exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), plan.Schema(), plan.ID()),
			numDualRows:    0,
		}
	}

	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)

	decoder := NewRowDecoder(b.ctx, plan.Schema(), plan.TblInfo)
	e := &BatchPointGetExec{
		BaseExecutor:       exec.NewBaseExecutor(b.ctx, plan.Schema(), plan.ID()),
		indexUsageReporter: b.buildIndexUsageReporter(plan, true),
		tblInfo:            plan.TblInfo,
		idxInfo:            plan.IndexInfo,
		rowDecoder:         decoder,
		keepOrder:          plan.KeepOrder,
		desc:               plan.Desc,
		lock:               plan.Lock,
		waitTime:           plan.LockWaitTime,
		columns:            plan.Columns,
		handles:            handles,
		idxVals:            plan.IndexValues,
		partitionNames:     plan.PartitionNames,
	}

	e.snapshot, err = b.getSnapshot()
	if err != nil {
		b.err = err
		return nil
	}
	if e.Ctx().GetSessionVars().IsReplicaReadClosestAdaptive() {
		e.snapshot.SetOption(kv.ReplicaReadAdjuster, newReplicaReadAdjuster(e.Ctx(), plan.GetAvgRowSize()))
	}
	if e.RuntimeStats() != nil {
		snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		e.snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
	}

	if plan.IndexInfo != nil {
		sctx := b.ctx.GetSessionVars().StmtCtx
		sctx.IndexNames = append(sctx.IndexNames, plan.TblInfo.Name.O+":"+plan.IndexInfo.Name.O)
	}

	failpoint.Inject("assertBatchPointReplicaOption", func(val failpoint.Value) {
		assertScope := val.(string)
		if e.Ctx().GetSessionVars().GetReplicaRead().IsClosestRead() && assertScope != b.readReplicaScope {
			panic("batch point get replica option fail")
		}
	})

	snapshotTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	if plan.TblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		if cacheTable := b.getCacheTable(plan.TblInfo, snapshotTS); cacheTable != nil {
			e.snapshot = cacheTableSnapshot{e.snapshot, cacheTable}
		}
	}

	if plan.TblInfo.TempTableType != model.TempTableNone {
		// Temporary table should not do any lock operations
		e.lock = false
		e.waitTime = 0
	}

	if e.lock {
		b.hasLock = true
	}
	if pi := plan.TblInfo.GetPartitionInfo(); pi != nil && len(plan.PartitionIdxs) > 0 {
		defs := plan.TblInfo.GetPartitionInfo().Definitions
		if plan.SinglePartition {
			e.singlePartID = defs[plan.PartitionIdxs[0]].ID
		} else {
			e.planPhysIDs = make([]int64, len(plan.PartitionIdxs))
			for i, idx := range plan.PartitionIdxs {
				e.planPhysIDs[i] = defs[idx].ID
			}
		}
	}

	capacity := len(e.handles)
	if capacity == 0 {
		capacity = len(e.idxVals)
	}
	e.SetInitCap(capacity)
	e.SetMaxChunkSize(capacity)
	e.buildVirtualColumnInfo()
	return e
}

func newReplicaReadAdjuster(ctx sessionctx.Context, avgRowSize float64) txnkv.ReplicaReadAdjuster {
	return func(count int) (tikv.StoreSelectorOption, clientkv.ReplicaReadType) {
		if int64(avgRowSize*float64(count)) >= ctx.GetSessionVars().ReplicaClosestReadThreshold {
			return tikv.WithMatchLabels([]*metapb.StoreLabel{
				{
					Key:   placement.DCLabelKey,
					Value: config.GetTxnScopeFromConfig(),
				},
			}), clientkv.ReplicaReadMixed
		}
		// fallback to read from leader if the request is small
		return nil, clientkv.ReplicaReadLeader
	}
}

func isCommonHandleRead(tbl *model.TableInfo, idx *model.IndexInfo) bool {
	return tbl.IsCommonHandle && idx.Primary
}

func getPhysicalTableID(t table.Table) int64 {
	if p, ok := t.(table.PhysicalTable); ok {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}

func (builder *dataReaderBuilder) partitionPruning(tbl table.PartitionedTable, planPartInfo *physicalop.PhysPlanPartInfo) ([]table.PhysicalTable, error) {
	builder.once.Do(func() {
		condPruneResult, err := partitionPruning(builder.executorBuilder.ctx, tbl, planPartInfo)
		builder.once.condPruneResult = condPruneResult
		builder.once.err = err
	})
	return builder.once.condPruneResult, builder.once.err
}

func partitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, planPartInfo *physicalop.PhysPlanPartInfo) ([]table.PhysicalTable, error) {
	var pruningConds []expression.Expression
	var partitionNames []ast.CIStr
	var columns []*expression.Column
	var columnNames types.NameSlice
	if planPartInfo != nil {
		pruningConds = planPartInfo.PruningConds
		partitionNames = planPartInfo.PartitionNames
		columns = planPartInfo.Columns
		columnNames = planPartInfo.ColumnNames
	}
	idxArr, err := partitionpruning.PartitionPruning(ctx.GetPlanCtx(), tbl, pruningConds, partitionNames, columns, columnNames)
	if err != nil {
		return nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	var ret []table.PhysicalTable
	if fullRangePartition(idxArr) {
		ret = make([]table.PhysicalTable, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			p := tbl.GetPartition(def.ID)
			ret = append(ret, p)
		}
	} else {
		ret = make([]table.PhysicalTable, 0, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			p := tbl.GetPartition(pid)
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func getPartitionIDsAfterPruning(ctx sessionctx.Context, tbl table.PartitionedTable, physPlanPartInfo *physicalop.PhysPlanPartInfo) (map[int64]struct{}, error) {
	if physPlanPartInfo == nil {
		return nil, errors.New("physPlanPartInfo in getPartitionIDsAfterPruning must not be nil")
	}
	idxArr, err := partitionpruning.PartitionPruning(ctx.GetPlanCtx(), tbl, physPlanPartInfo.PruningConds, physPlanPartInfo.PartitionNames, physPlanPartInfo.Columns, physPlanPartInfo.ColumnNames)
	if err != nil {
		return nil, err
	}

	var ret map[int64]struct{}

	pi := tbl.Meta().GetPartitionInfo()
	if fullRangePartition(idxArr) {
		ret = make(map[int64]struct{}, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ret[def.ID] = struct{}{}
		}
	} else {
		ret = make(map[int64]struct{}, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			ret[pid] = struct{}{}
		}
	}
	return ret, nil
}

func fullRangePartition(idxArr []int) bool {
	return len(idxArr) == 1 && idxArr[0] == rule.FullRange
}
