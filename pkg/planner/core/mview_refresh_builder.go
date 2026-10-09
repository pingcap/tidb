// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func (b *PlanBuilder) buildRefreshMaterializedView(_ context.Context, stmt *ast.RefreshMaterializedViewStmt) (base.Plan, error) {
	if stmt == nil || stmt.ViewName == nil {
		return nil, errors.New("REFRESH MATERIALIZED VIEW: missing view name")
	}
	dbName := stmt.ViewName.Schema.L
	if dbName == "" {
		dbName = b.ctx.GetSessionVars().CurrentDB
	}
	if dbName == "" {
		return nil, plannererrors.ErrNoDB
	}
	var authErr error
	if user := b.ctx.GetSessionVars().User; user != nil {
		authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs(
			"OPERATE VIEW", user.AuthUsername, user.AuthHostname, stmt.ViewName.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.OperateViewPriv, dbName, stmt.ViewName.Name.L, "", authErr)

	switch stmt.ObserveType {
	case ast.RefreshMaterializedViewObserveNone:
		return &RefreshMaterializedView{Statement: stmt}, nil
	case ast.RefreshMaterializedViewObserveDryRun:
		b.addRefreshObserveVisitInfo(dbName, stmt.ViewName.Name.L)
		p := &DryRunRefreshMaterializedView{Statement: stmt}
		schema, names := refreshObserveSchema()
		p.SetSchemaAndNames(schema, names)
		return p, nil
	case ast.RefreshMaterializedViewObserveProfile:
		b.addRefreshObserveVisitInfo(dbName, stmt.ViewName.Name.L)
		p := &ProfileRefreshMaterializedView{Statement: stmt}
		schema, names := refreshObserveSchema()
		p.SetSchemaAndNames(schema, names)
		return p, nil
	default:
		return nil, errors.New("REFRESH MATERIALIZED VIEW: invalid observe option")
	}
}

func (b *PlanBuilder) addRefreshObserveVisitInfo(dbName, tableName string) {
	var authErr error
	if user := b.ctx.GetSessionVars().User; user != nil {
		authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs(
			"SHOW VIEW", user.AuthUsername, user.AuthHostname, tableName)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, dbName, tableName, "", authErr)
}

func refreshObserveSchema() (*expression.Schema, types.NameSlice) {
	cols := newColumnsWithNames(1)
	col, name := buildColumnWithName("", "refresh steps", mysql.TypeString, mysql.MaxBlobWidth)
	cols.Append(col, name)
	return cols.col2Schema(), cols.names
}

// buildRefreshMaterializedViewImplement builds the physical maintenance plan used by
// the refresh executor for FAST and COMPLETE DELTA APPLY modes. The statement is
// internal and carries the timestamp window selected by the refresh executor.
func (b *PlanBuilder) buildRefreshMaterializedViewImplement(ctx context.Context, stmt *ast.RefreshMaterializedViewImplementStmt) (base.Plan, error) {
	if stmt == nil || stmt.RefreshStmt == nil || stmt.RefreshStmt.ViewName == nil {
		return nil, errors.New("RefreshMaterializedViewImplementStmt: missing RefreshStmt/ViewName")
	}
	mode, err := stmt.RefreshStmt.Mode()
	if err != nil {
		return nil, err
	}
	if mode != ast.RefreshMaterializedViewModeFast && mode != ast.RefreshMaterializedViewModeCompleteDeltaApply {
		return nil, errors.Errorf("RefreshMaterializedViewImplementStmt: unsupported mode %s", mode.String())
	}

	viewName := stmt.RefreshStmt.ViewName
	dbName := viewName.Schema.L
	if dbName == "" {
		dbName = b.ctx.GetSessionVars().CurrentDB
	}
	if dbName == "" {
		return nil, plannererrors.ErrNoDB
	}
	mvTable, err := b.is.TableByName(ctx, ast.NewCIStr(dbName), viewName.Name)
	if err != nil {
		return nil, err
	}
	mvInfo := mvTable.Meta()
	if mvInfo == nil || mvInfo.MaterializedView == nil {
		return nil, errors.Errorf("table %s.%s is not a materialized view", dbName, viewName.Name.O)
	}
	txn, err := b.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	if txn == nil || !txn.Valid() || txn.StartTS() == 0 {
		return nil, errors.New("RefreshMaterializedViewImplementStmt: invalid transaction")
	}
	sctx, ok := b.ctx.(sessionctx.Context)
	if !ok {
		return nil, errors.New("RefreshMaterializedViewImplementStmt: invalid session context")
	}
	ensureSessionExtendedInfoSchema := func(planIS infoschema.InfoSchema) infoschema.InfoSchema {
		if _, ok := planIS.(*infoschema.SessionExtendedInfoSchema); ok {
			return planIS
		}
		return &infoschema.SessionExtendedInfoSchema{InfoSchema: planIS}
	}

	// Build the derived SELECT with a standalone builder so the outer builder's
	// visit state and clause bookkeeping are not affected.
	optimizeSelect := func(optCtx context.Context, sel *ast.SelectStmt, planIS infoschema.InfoSchema, useInfoSchemaAsIs bool) (base.PhysicalPlan, error) {
		planIS = ensureSessionExtendedInfoSchema(planIS)
		nodeW := resolve.NewNodeW(sel)
		preprocessOpts := []PreprocessOpt{
			WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: planIS}),
		}
		if useInfoSchemaAsIs {
			preprocessOpts = append(preprocessOpts, useProvidedInfoSchemaAsIs)
		}
		if err := Preprocess(optCtx, sctx, nodeW, preprocessOpts...); err != nil {
			return nil, err
		}
		savedBlockNames := b.ctx.GetSessionVars().PlannerSelectBlockAsName.Load()
		defer b.ctx.GetSessionVars().PlannerSelectBlockAsName.Store(savedBlockNames)
		var builderOpts []PlanBuilderOpt
		if useInfoSchemaAsIs {
			builderOpts = append(builderOpts, planBuilderOptUseProvidedInfoSchemaAsIs{})
		}
		innerBuilder, _ := NewPlanBuilder(builderOpts...).Init(b.ctx, planIS, hint.NewQBHintHandler(nil))
		p, err := innerBuilder.Build(optCtx, nodeW)
		if err != nil {
			return nil, err
		}
		logic, ok := p.(base.LogicalPlan)
		if !ok {
			return nil, errors.Errorf("mview: expected logical plan from select, got %T", p)
		}
		physical, _, err := DoOptimize(optCtx, b.ctx, innerBuilder.GetOptFlag(), logic)
		return physical, err
	}

	if mode == ast.RefreshMaterializedViewModeFast {
		res, err := mview.Build(b.ctx, b.is, mvInfo, mview.BuildOptions{
			FromTS: stmt.LastSuccessfulRefreshReadTSO,
			ToTS:   stmt.TargetRefreshReadTSO,
		}, nil)
		if err != nil {
			return nil, err
		}
		if res.MergeSourceSelect == nil {
			return nil, errors.New("mview: merge source select is nil")
		}
		txnManager := sessiontxn.GetTxnManager(sctx)
		retainedUpperTSO := txn.StartTS()
		if txnManager.GetContextProvider() != nil {
			retainedUpperTSO, err = txnManager.GetStmtForUpdateTS()
			if err != nil {
				return nil, err
			}
		}
		var source base.PhysicalPlan
		if estimationCtx, ok := b.ctx.(planctx.MLogCommitTSEstimationContext); ok {
			estimation := &planctx.MLogCommitTSEstimation{
				MLogTableID: res.MLogTableID, RetainedLowerTSO: stmt.MLogRetainedLowerTSO, RetainedUpperTSO: retainedUpperTSO,
			}
			err = estimationCtx.WithMLogCommitTSEstimation(estimation, func() error {
				var optimizeErr error
				source, optimizeErr = optimizeSelect(ctx, res.MergeSourceSelect, b.is, false)
				return optimizeErr
			})
		} else {
			source, err = optimizeSelect(ctx, res.MergeSourceSelect, b.is, false)
		}
		if err != nil {
			return nil, err
		}
		if source.Schema().Len() != res.SourceColumnCount {
			return nil, errors.Errorf("unexpected merge-source schema length: got %d, expected %d", source.Schema().Len(), res.SourceColumnCount)
		}
		var (
			fullInner      base.PhysicalPlan
			fullInnerCount int
			fullRanges     ranger.MutableRanges
			fullKeyMap     []int
			fullKeyResult  []int
			fullOutput     []int
			fullSnapshot   *DataReaderSnapshot
		)
		if res.FullUpdateLookupTemplateSelect != nil {
			if res.FullUpdateLookupColumnCount <= 0 || len(res.FullUpdateLookupMVOffsets) != res.FullUpdateLookupColumnCount {
				return nil, errors.New("mview full-update lookup template: invalid output column metadata")
			}
			lookupIS := b.is
			if stmt.TargetRefreshReadTSO > 0 {
				gcSafePoint, gcErr := gcutil.GetGCSafePoint(sctx)
				if gcErr != nil {
					return nil, gcErr
				}
				if gcErr = gcutil.ValidateSnapshotWithGCSafePoint(stmt.TargetRefreshReadTSO, gcSafePoint); gcErr != nil {
					return nil, gcErr
				}
				lookupIS, err = staleread.GetSessionSnapshotInfoSchema(sctx, stmt.TargetRefreshReadTSO)
				if err != nil {
					return nil, err
				}
				lookupIS = ensureSessionExtendedInfoSchema(lookupIS)
				fullSnapshot = &DataReaderSnapshot{TS: stmt.TargetRefreshReadTSO, InfoSchema: lookupIS}
			}
			indexes, err := validateMVFullUpdateSupportingIndex(ctx, lookupIS, res.BaseTableID, res.GroupKeyBaseCols)
			if err != nil {
				return nil, err
			}
			if err := mview.SetFullUpdateLookupIndexHint(res.FullUpdateLookupTemplateSelect, indexes); err != nil {
				return nil, err
			}
			savedEnableINLJoinInnerMultiPattern := b.ctx.GetSessionVars().EnableINLJoinInnerMultiPattern
			b.ctx.GetSessionVars().EnableINLJoinInnerMultiPattern = true
			lookupPlan, err := func() (base.PhysicalPlan, error) {
				defer func() {
					b.ctx.GetSessionVars().EnableINLJoinInnerMultiPattern = savedEnableINLJoinInnerMultiPattern
				}()
				return optimizeSelect(ctx, res.FullUpdateLookupTemplateSelect, lookupIS, stmt.TargetRefreshReadTSO > 0)
			}()
			if err != nil {
				return nil, err
			}
			if lookupPlan.Schema().Len() != res.FullUpdateLookupColumnCount {
				return nil, errors.Errorf("mview full-update lookup template: unexpected output schema length: got %d, expected %d", lookupPlan.Schema().Len(), res.FullUpdateLookupColumnCount)
			}
			tmpl, err := extractMVFullUpdateLookupTemplate(lookupPlan, res.FullUpdateLookupColumnCount, len(res.GroupKeyMVOffsets), res.FullUpdateLookupMVOffsets, res.GroupKeyMVOffsets)
			if err != nil {
				return nil, err
			}
			fullInner, fullInnerCount, fullRanges, fullKeyMap, fullKeyResult, fullOutput = tmpl.InnerSource, tmpl.InnerColumnCount, tmpl.IndexRanges, tmpl.KeyOff2IdxOff, tmpl.KeyResultColIdxes, tmpl.OutputMVOffsets
		}
		plan := &MViewDeltaMerge{
			Source:                source,
			FullUpdateInnerSource: fullInner, FullUpdateInnerColumnCount: fullInnerCount,
			FullUpdateIndexRanges: fullRanges, FullUpdateKeyOff2IdxOff: fullKeyMap,
			FullUpdateKeyResultColIdxes: fullKeyResult, FullUpdateOutputMVOffsets: fullOutput,
			FullUpdateSnapshot: fullSnapshot,
			MVTableID:          res.MVTableID, BaseTableID: res.BaseTableID, MLogTableID: res.MLogTableID,
			MVColumnCount: res.MVColumnCount, DeltaColumnCount: res.DeltaColumnCount,
			MVTablePKCols: res.MVTablePKCols, GroupKeyMVOffsets: res.GroupKeyMVOffsets,
			CountStarMVOffset: res.CountStarMVOffset, AggInfos: res.AggInfos,
		}
		return plan.Init(b.ctx), nil
	}

	diffRes, err := mview.BuildCompleteDiffSource(b.ctx, b.is, mvInfo)
	if err != nil {
		return nil, err
	}
	if diffRes.DiffSourceSelect == nil {
		return nil, errors.New("complete diff: diff source select is nil")
	}
	sessVars := b.ctx.GetSessionVars()
	savedFullOuterJoin := sessVars.EnableFullOuterJoin
	savedCascades := sessVars.EnableCascadesPlanner
	savedHint := sessVars.StmtCtx.HasEnableCascadesPlannerHint
	savedStmtCascades := sessVars.StmtCtx.EnableCascadesPlanner
	sessVars.EnableFullOuterJoin = true
	sessVars.SetEnableCascadesPlanner(false)
	sessVars.StmtCtx.HasEnableCascadesPlannerHint = false
	sessVars.StmtCtx.EnableCascadesPlanner = false
	source, err := func() (base.PhysicalPlan, error) {
		defer func() {
			sessVars.EnableFullOuterJoin = savedFullOuterJoin
			sessVars.SetEnableCascadesPlanner(savedCascades)
			sessVars.StmtCtx.HasEnableCascadesPlannerHint = savedHint
			sessVars.StmtCtx.EnableCascadesPlanner = savedStmtCascades
		}()
		return optimizeSelect(ctx, diffRes.DiffSourceSelect, b.is, false)
	}()
	if err != nil {
		return nil, err
	}
	if err := diffRes.ValidateSourceLayout(source.Schema().Len()); err != nil {
		return nil, err
	}
	plan := &MViewCompleteDeltaApply{
		Source: source, MVTableID: mvInfo.ID, MVColumnCount: diffRes.MVColumnCount,
		OpColID: diffRes.OpColOffset, MarkerMVOffset: diffRes.MarkerMVOffset,
		GroupKeyMVOffsets:        append([]int(nil), diffRes.GroupKeyMVOffsets...),
		CurrentHandleCols:        diffRes.CurrentHandleCols,
		CurrentRowInputColIDs:    append([]int(nil), diffRes.CurrentRowOffsets...),
		RecomputedRowInputColIDs: append([]int(nil), diffRes.RecomputedRowOffsets...),
	}
	return plan.Init(b.ctx), nil
}
