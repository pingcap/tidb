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
	"context"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/lockstats"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)
func (b *executorBuilder) buildInsert(v *physicalop.Insert) exec.Executor {
	b.inInsertStmt = true
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	var children []exec.Executor
	if selectExec != nil {
		children = append(children, selectExec)
	}
	baseExec := exec.NewBaseExecutor(b.ctx, nil, v.ID(), children...)
	baseExec.SetInitCap(chunk.ZeroCapacity)

	ivs := &InsertValues{
		BaseExecutor:              baseExec,
		Table:                     v.Table,
		Columns:                   v.Columns,
		Lists:                     v.Lists,
		GenExprs:                  v.GenCols.Exprs,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		hasRefCols:                v.NeedFillDefaultValue,
		SelectExec:                selectExec,
		rowLen:                    v.RowLen,
		ignoreErr:                 v.IgnoreErr,
	}
	err := ivs.initInsertColumns()
	if err != nil {
		b.err = err
		return nil
	}
	ivs.fkChecks, b.err = buildFKCheckExecs(b.ctx, ivs.Table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	ivs.fkCascades, b.err = b.buildFKCascadeExecs(ivs.Table, v.FKCascades)
	if b.err != nil {
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
	}
	return insert
}

func (b *executorBuilder) buildImportInto(v *plannercore.ImportInto) exec.Executor {
	// see planBuilder.buildImportInto for detail why we use the latest schema here.
	latestIS := b.ctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := latestIS.TableByID(context.Background(), v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	if !tbl.Meta().IsBaseTable() {
		b.err = plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tbl.Meta().Name.O, "IMPORT")
		return nil
	}

	var (
		selectExec exec.Executor
		children   []exec.Executor
	)
	if v.SelectPlan != nil {
		selectExec = b.build(v.SelectPlan)
		if b.err != nil {
			return nil
		}
		children = append(children, selectExec)
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), children...)
	executor, err := newImportIntoExec(base, selectExec, b.ctx, v, tbl)
	if err != nil {
		b.err = err
		return nil
	}

	return executor
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) exec.Executor {
	tbl, ok := b.is.TableByID(context.Background(), v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	if !tbl.Meta().IsBaseTable() {
		b.err = plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tbl.Meta().Name.O, "LOAD")
		return nil
	}

	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	worker, err := NewLoadDataWorker(b.ctx, v, tbl)
	if err != nil {
		b.err = err
		return nil
	}

	return &LoadDataExec{
		BaseExecutor:   base,
		loadDataWorker: worker,
		FileLocRef:     v.FileLocRef,
	}
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) exec.Executor {
	e := &LoadStatsExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildLockStats(v *plannercore.LockStats) exec.Executor {
	e := &lockstats.LockExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		Tables:       v.Tables,
	}
	return e
}

func (b *executorBuilder) buildUnlockStats(v *plannercore.UnlockStats) exec.Executor {
	e := &lockstats.UnlockExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		Tables:       v.Tables,
	}
	return e
}

func (b *executorBuilder) buildPlanReplayer(v *plannercore.PlanReplayer) exec.Executor {
	if v.Load {
		e := &PlanReplayerLoadExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			info:         &PlanReplayerLoadInfo{Path: v.File, Ctx: b.ctx},
		}
		return e
	}
	if v.Capture {
		e := &PlanReplayerExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			CaptureInfo: &PlanReplayerCaptureInfo{
				SQLDigest:  v.SQLDigest,
				PlanDigest: v.PlanDigest,
			},
		}
		return e
	}
	if v.Remove {
		e := &PlanReplayerExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			CaptureInfo: &PlanReplayerCaptureInfo{
				SQLDigest:  v.SQLDigest,
				PlanDigest: v.PlanDigest,
				Remove:     true,
			},
		}
		return e
	}

	e := &PlanReplayerExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		DumpInfo: &PlanReplayerDumpInfo{
			Analyze:           v.Analyze,
			Path:              v.File,
			ctx:               b.ctx,
			HistoricalStatsTS: v.HistoricalStatsTS,
		},
	}
	if len(v.StmtList) > 0 {
		// Parse multiple SQL strings from StmtList
		e.DumpInfo.ExecStmts = make([]ast.StmtNode, 0, len(v.StmtList))
		for _, sqlStr := range v.StmtList {
			node, err := b.ctx.GetRestrictedSQLExecutor().ParseWithParams(context.Background(), sqlStr)
			if err != nil {
				// If parsing fails, propagate the error immediately so the statement fails.
				b.err = errors.Errorf("plan replayer: failed to parse SQL: %s, error: %v", sqlStr, err)
				return nil
			}
			e.DumpInfo.ExecStmts = append(e.DumpInfo.ExecStmts, node)
		}
	} else if v.ExecStmt != nil {
		e.DumpInfo.ExecStmts = []ast.StmtNode{v.ExecStmt}
	} else {
		e.BaseExecutor = exec.NewBaseExecutor(b.ctx, nil, v.ID())
	}
	return e
}

func (b *executorBuilder) buildTraffic(traffic *plannercore.Traffic) exec.Executor {
	switch traffic.OpType {
	case ast.TrafficOpCapture:
		exec := &TrafficCaptureExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, traffic.Schema(), traffic.ID()),
			Args: map[string]string{
				"output": traffic.Dir,
			},
		}
		for _, option := range traffic.Options {
			switch option.OptionType {
			case ast.TrafficOptionDuration:
				exec.Args["duration"] = option.StrValue
			case ast.TrafficOptionEncryptionMethod:
				exec.Args["encrypt-method"] = option.StrValue
			case ast.TrafficOptionCompress:
				exec.Args["compress"] = strconv.FormatBool(option.BoolValue)
			}
		}
		return exec
	case ast.TrafficOpReplay:
		exec := &TrafficReplayExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, traffic.Schema(), traffic.ID()),
			Args: map[string]string{
				"input": traffic.Dir,
			},
		}
		for _, option := range traffic.Options {
			switch option.OptionType {
			case ast.TrafficOptionUsername:
				exec.Args["username"] = option.StrValue
			case ast.TrafficOptionPassword:
				exec.Args["password"] = option.StrValue
			case ast.TrafficOptionSpeed:
				if v := option.FloatValue.GetValue(); v != nil {
					if dec, ok := v.(*types.MyDecimal); ok {
						exec.Args["speed"] = dec.String()
					}
				}
			case ast.TrafficOptionReadOnly:
				exec.Args["readonly"] = strconv.FormatBool(option.BoolValue)
			}
		}
		return exec
	case ast.TrafficOpCancel:
		return &TrafficCancelExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, traffic.Schema(), traffic.ID()),
		}
	case ast.TrafficOpShow:
		return &TrafficShowExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, traffic.Schema(), traffic.ID()),
		}
	}
	// impossible here
	return nil
}

func (*executorBuilder) buildReplace(vals *InsertValues) exec.Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) exec.Executor {
	e := &GrantExec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, nil, 0),
		Privs:                 grant.Privs,
		ObjectType:            grant.ObjectType,
		Level:                 grant.Level,
		Users:                 grant.Users,
		WithGrant:             grant.WithGrant,
		AuthTokenOrTLSOptions: grant.AuthTokenOrTLSOptions,
		is:                    b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) exec.Executor {
	e := &RevokeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, 0),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) setTelemetryInfo(v *plannercore.DDL) {
	if v == nil || b.Ti == nil {
		return
	}
	switch s := v.Statement.(type) {
	case *ast.AlterTableStmt:
		if len(s.Specs) > 1 {
			b.Ti.UseMultiSchemaChange = true
		}
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableDropFirstPartition:
				if b.Ti.PartitionTelemetry == nil {
					b.Ti.PartitionTelemetry = &PartitionTelemetryInfo{}
				}
				b.Ti.PartitionTelemetry.UseDropIntervalPartition = true
			case ast.AlterTableAddLastPartition:
				if b.Ti.PartitionTelemetry == nil {
					b.Ti.PartitionTelemetry = &PartitionTelemetryInfo{}
				}
				b.Ti.PartitionTelemetry.UseAddIntervalPartition = true
			case ast.AlterTableExchangePartition:
				b.Ti.UseExchangePartition = true
			case ast.AlterTableReorganizePartition:
				if b.Ti.PartitionTelemetry == nil {
					b.Ti.PartitionTelemetry = &PartitionTelemetryInfo{}
				}
				b.Ti.PartitionTelemetry.UseReorganizePartition = true
			}
		}
	case *ast.CreateTableStmt:
		if s.Partition == nil {
			break
		}

		p := s.Partition
		if b.Ti.PartitionTelemetry == nil {
			b.Ti.PartitionTelemetry = &PartitionTelemetryInfo{}
		}
		b.Ti.PartitionTelemetry.TablePartitionMaxPartitionsNum = max(p.Num, uint64(len(p.Definitions)))
		b.Ti.PartitionTelemetry.UseTablePartition = true

		switch p.Tp {
		case ast.PartitionTypeRange:
			if p.Sub == nil {
				if len(p.ColumnNames) > 0 {
					b.Ti.PartitionTelemetry.UseTablePartitionRangeColumns = true
					if len(p.ColumnNames) > 1 {
						b.Ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt1 = true
					}
					if len(p.ColumnNames) > 2 {
						b.Ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt2 = true
					}
					if len(p.ColumnNames) > 3 {
						b.Ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt3 = true
					}
				} else {
					b.Ti.PartitionTelemetry.UseTablePartitionRange = true
				}
				if p.Interval != nil {
					b.Ti.PartitionTelemetry.UseCreateIntervalPartition = true
				}
			}
		case ast.PartitionTypeHash:
			if p.Sub == nil {
				b.Ti.PartitionTelemetry.UseTablePartitionHash = true
			}
		case ast.PartitionTypeList:
			if p.Sub == nil {
				if len(p.ColumnNames) > 0 {
					b.Ti.PartitionTelemetry.UseTablePartitionListColumns = true
				} else {
					b.Ti.PartitionTelemetry.UseTablePartitionList = true
				}
			}
		}
	case *ast.FlashBackToTimestampStmt:
		b.Ti.UseFlashbackToCluster = true
	}
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) exec.Executor {
	b.setTelemetryInfo(v)

	e := &DDLExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ddlExecutor:  domain.GetDomain(b.ctx).DDLExecutor(),
		stmt:         v.Statement,
		is:           b.is,
		tempTableDDL: temptable.GetTemporaryTableDDL(b.ctx),
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) exec.Executor {
	t := &TraceExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmtNode:     v.StmtNode,
		resolveCtx:   v.ResolveCtx,
		builder:      b,
		format:       v.Format,

		optimizerTrace:       v.OptimizerTrace,
		optimizerTraceTarget: v.OptimizerTraceTarget,
	}
	if t.format == plannercore.TraceFormatLog && !t.optimizerTrace {
		return &sortexec.SortExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), t),
			ByItems: []*plannerutil.ByItems{
				{Expr: &expression.Column{
					Index:   0,
					RetType: types.NewFieldType(mysql.TypeTimestamp),
				}},
			},
			ExecSchema: v.Schema(),
		}
	}
	return t
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) exec.Executor {
	explainExec := &ExplainExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		explain:      v,
	}
	if v.Analyze {
		if b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil {
			b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
		}
	}
	// Needs to build the target plan, even if not executing it
	// to get partition pruning.
	explainExec.analyzeExec = b.build(v.TargetPlan)
	return explainExec
}

func (b *executorBuilder) buildSelectInto(v *plannercore.SelectInto) exec.Executor {
	child := b.build(v.TargetPlan)
	if b.err != nil {
		return nil
	}
	return &SelectIntoExec{
		BaseExecutor:   exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), child),
		intoOpt:        v.IntoOpt,
		LineFieldsInfo: v.LineFieldsInfo,
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
