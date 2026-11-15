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

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// addSPFreshIndexWithLocalIngest shares routed writers and the completion barrier
// with distributed builds. A zero-input scan still creates an empty Ready index.
// It deliberately has no partial-import checkpoint: remote writes cannot replay.
func (dc *ddlCtx) addSPFreshIndexWithLocalIngest(ctx context.Context, pool *sess.Pool, tbl table.PhysicalTable, info *reorgInfo, indexes []*model.IndexInfo) (err error) {
	defer func() {
		if err != nil {
			logutil.DDLLogger().Warn("SPFresh local build failed", zap.Int64("jobID", info.Job.ID), zap.Error(err))
			// Retry classification checks message substrings before SQL error codes.
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SPFresh local build failed; remote writes cannot be replayed")
		}
	}()
	plan := dc.jobContext(info.Job.ID, info.ReorgMeta).spfresh.legacyRoute
	if plan == nil {
		return errors.New("SPFresh local build route is missing")
	}
	if err := initializeSPFreshBuild(ctx, dc.store, tbl.Meta(), plan); err != nil {
		return err
	}
	targets, err := resolveRoutedSPFreshWorkerTargets(ctx, dc.store, plan)
	if err != nil {
		return err
	}
	defer cleanupSPFreshBuildTasksByRoutePlan(ctx, util.InternalHTTPClient(), targets.clusterID, plan, logutil.DDLLogger())
	bc, err := ingest.NewBackendCtxBuilder(ctx, dc.store, info.Job).
		WithRemoteBackend(false, 0).WithSPFreshBuildRoutePlan(plan).Build(nil, nil)
	if err != nil {
		return err
	}
	defer bc.Close()
	engines, err := bc.Register([]int64{plan.IndexID}, []bool{false}, tbl)
	if err != nil {
		return err
	}
	sctx, err := pool.Get()
	if err != nil {
		return err
	}
	avgRowSize := estimateTableRowSize(ctx, dc.store, sctx.GetRestrictedSQLExecutor(), tbl)
	pool.Put(sctx)
	wctx := NewLocalWorkerCtx(ctx, info.Job.ID)
	defer wctx.Cancel()
	listener := &localRowCntListener{
		reorgCtx: dc.getReorgCtx(info.Job.ID),
		counter:  metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, info.Job.SchemaName, info.Job.TableName, indexes[0].Name.O),
	}
	pipe, err := NewAddIndexIngestPipeline(wctx, dc.store, pool, bc, engines, info.Job.ID,
		tbl, indexes, info.StartKey, info.EndKey, info.ReorgMeta, avgRowSize,
		info.ReorgMeta.GetConcurrencyOrDefault(int(variable.GetDDLReorgWorkerCounter())), listener)
	if err != nil {
		return err
	}
	if err := executeAndClosePipeline(wctx, pipe, info, bc, avgRowSize); err != nil {
		_ = bc.FinishAndUnregisterEngines(ingest.OptCloseEngines)
		return err
	}
	finisher, ok := bc.(spfreshBuildFinisher)
	if !ok {
		return errors.New("SPFresh build finisher is unavailable")
	}
	hasData, err := finisher.FinishSPFreshBuild()
	if err != nil {
		return err
	}
	coordinator, err := newRemoteIngestExecutor(tbl, dc.store, plan, util.InternalHTTPClient())
	if err != nil {
		return err
	}
	return coordinator.runSPFreshBuildCoordinator(ctx, hasData)
}
