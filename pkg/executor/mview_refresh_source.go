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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
)

type mvRefreshInputKey struct{}

// mvRefreshInput retains the final statement attempt before follow-up SQL can
// overwrite its plan or advance for_update_ts. The compilation schema also
// retains bindings for sources whose scans were eliminated (e.g. WHERE FALSE).
type mvRefreshInput struct {
	targetID   int64
	plan       base.Plan
	infoSchema infoschema.InfoSchema
	readTSO    uint64
}

func (input *mvRefreshInput) matches(plan base.Plan) bool {
	switch p := plan.(type) {
	case *plannercore.MViewDeltaMerge:
		return p.MVTableID == input.targetID
	case *plannercore.MViewCompleteDeltaApply:
		return p.MVTableID == input.targetID
	case *plannercore.Insert:
		return p.Table.Meta().ID == input.targetID
	case *plannercore.ImportInto:
		return p.Table.TableInfo.ID == input.targetID
	}
	return false
}

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

func validateMVRefreshInput(
	ctx context.Context, sctx sessionctx.Context, mviewID int64, input *mvRefreshInput, readTSO uint64,
	fast bool, registered []int64,
) ([]model.MViewRefreshSource, error) {
	if input.plan == nil || input.infoSchema == nil || readTSO == 0 {
		return nil, errors.New("refresh materialized view: computation snapshot or plan is missing")
	}
	compiled, err := mviewutil.ResolveRefreshSources(ctx, input.infoSchema, mviewID)
	if err != nil {
		return nil, err
	}
	snapshotIS, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(readTSO)
	if err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: cannot load computation snapshot schema")
	}
	failpoint.InjectCall("mvRefreshValidateInputSnapshot", readTSO, input.plan, &snapshotIS)
	sources, err := mviewutil.ResolveRefreshSources(ctx, snapshotIS, mviewID)
	if err != nil {
		return nil, err
	}
	// COMPLETE does not consume input logs. It must still read the physical
	// sources represented by schema@readTSO, then register that snapshot's logs.
	if len(compiled) != len(sources) {
		return nil, errors.New("refresh materialized view: source identities differ from the computation snapshot; retry refresh")
	}
	for i, source := range sources {
		if compiled[i].SchemaID != source.SchemaID || compiled[i].TableID != source.TableID ||
			(fast && compiled[i].MLogID != source.MLogID) {
			return nil, errors.New("refresh materialized view: source identities differ from the computation snapshot; retry refresh")
		}
	}
	if fast {
		if err := checkMVRefreshLogBaseline(sources, registered); err != nil {
			return nil, err
		}
	}
	if err := checkMVRefreshPlanSources(input.plan, mviewID, compiled, fast); err != nil {
		return nil, err
	}
	if merge, ok := input.plan.(*plannercore.MViewDeltaMerge); ok && merge.FullUpdateSnapshot != nil {
		// Bounded MIN/MAX probes use the requested upper bound, independently
		// of the current MLog scan snapshot. Both must retain the same baseline.
		historical, err := mviewutil.ResolveRefreshSources(ctx, merge.FullUpdateSnapshot.InfoSchema, mviewID)
		if err != nil {
			return nil, err
		}
		if !slices.Equal(historical, sources) {
			return nil, errors.New("refresh materialized view fast: MIN/MAX snapshot input identities have changed; run COMPLETE refresh")
		}
	}
	return sources, nil
}

func checkMVRefreshPlanSources(plan base.Plan, mviewID int64, sources []model.MViewRefreshSource, fast bool) error {
	allowed := map[int64]struct{}{mviewID: {}}
	for _, source := range sources {
		allowed[source.TableID] = struct{}{}
		if fast {
			allowed[source.MLogID] = struct{}{}
		}
	}
	if merge, ok := plan.(*plannercore.MViewDeltaMerge); ok {
		matched := slices.ContainsFunc(sources, func(source model.MViewRefreshSource) bool {
			return source.TableID == merge.BaseTableID && source.MLogID == merge.MLogTableID
		})
		if !matched {
			return errors.New("refresh materialized view: planned source/log binding differs from the computation snapshot; retry refresh")
		}
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
				if fast {
					for _, source := range sources {
						if tableInfo.ID == source.MLogID && (tableInfo.MaterializedViewLog == nil || tableInfo.MaterializedViewLog.BaseTableID != source.TableID) {
							return errors.New("refresh materialized view: planned log owner differs from the computation snapshot; retry refresh")
						}
					}
				}
				if _, ok := allowed[tableInfo.ID]; !ok {
					return errors.Errorf("refresh materialized view: planned input table %d differs from the computation snapshot; retry refresh", tableInfo.ID)
				}
			}
		}
	}
	return nil
}
