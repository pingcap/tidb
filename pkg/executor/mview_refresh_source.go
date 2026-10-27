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

package executor

import (
	"context"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
)

func checkMVRefreshLogBaseline(sources []model.MViewRefreshSource, registered []int64) error {
	for _, source := range sources {
		if source.MLogID == 0 {
			return errors.New("refresh materialized view fast: source has no materialized view log; run COMPLETE refresh to rebuild the input log baseline")
		}
	}
	if !slices.Equal(mviewutil.RefreshSourceLogIDs(sources), registered) {
		return errors.New("refresh materialized view fast: input log baseline has changed; run COMPLETE refresh to rebuild it")
	}
	return nil
}

func validateMVFastRefreshInput(
	ctx context.Context, sctx sessionctx.Context, mviewID int64, plan *plannercore.MViewDeltaMerge,
	readTSO uint64, registered []int64,
) error {
	snapshotIS, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(readTSO)
	if err != nil {
		return errors.Annotate(err, "refresh materialized view: cannot load computation snapshot schema")
	}
	failpoint.InjectCall("mvRefreshValidateInputSnapshot", readTSO, plan, &snapshotIS)
	sources, err := mviewutil.ResolveRefreshSources(ctx, snapshotIS, mviewID)
	if err != nil {
		return err
	}
	if err := checkMVRefreshLogBaseline(sources, registered); err != nil {
		return err
	}
	if !slices.ContainsFunc(sources, func(source model.MViewRefreshSource) bool {
		return source.TableID == plan.BaseTableID && source.MLogID == plan.MLogTableID
	}) {
		return errors.New("refresh materialized view: planned source/log binding differs from the computation snapshot; retry refresh")
	}
	return nil
}

func validateMVCompleteRefreshInput(
	ctx context.Context, sctx sessionctx.Context, mviewID int64, plan base.Plan, readTSO uint64,
) ([]model.MViewRefreshSource, error) {
	if plan == nil || readTSO == 0 {
		return nil, errors.New("refresh materialized view: computation snapshot or plan is missing")
	}
	snapshotIS, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(readTSO)
	if err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: cannot load computation snapshot schema")
	}
	failpoint.InjectCall("mvRefreshValidateInputSnapshot", readTSO, plan, &snapshotIS)
	sources, err := mviewutil.ResolveRefreshSources(ctx, snapshotIS, mviewID)
	if err != nil {
		return nil, err
	}
	// COMPLETE does not consume input logs. Validate the sources still present
	// in its plan, then register the logs resolved at the computation snapshot.
	if err := checkMVCompleteRefreshPlanSources(plan, mviewID, sources); err != nil {
		return nil, err
	}
	return sources, nil
}

func checkMVCompleteRefreshPlanSources(plan base.Plan, mviewID int64, sources []model.MViewRefreshSource) error {
	allowed := map[int64]struct{}{mviewID: {}}
	for _, source := range sources {
		allowed[source.TableID] = struct{}{}
	}
	flat := plannercore.FlattenPhysicalPlan(plan, false)
	trees := append([]plannercore.FlatPlanTree{flat.Main}, flat.CTEs...)
	for _, tree := range trees {
		for _, op := range tree {
			var tableInfo *model.TableInfo
			switch p := op.Origin.(type) {
			case *plannercore.PhysicalTableScan:
				tableInfo = p.Table
			case *plannercore.PhysicalIndexScan:
				tableInfo = p.Table
			case *plannercore.PointGetPlan:
				tableInfo = p.TblInfo
			case *plannercore.BatchPointGetPlan:
				tableInfo = p.TblInfo
			}
			if tableInfo != nil {
				if _, ok := allowed[tableInfo.ID]; !ok {
					return errors.Errorf("refresh materialized view: planned input table %d differs from the computation snapshot; retry refresh", tableInfo.ID)
				}
			}
		}
	}
	return nil
}
