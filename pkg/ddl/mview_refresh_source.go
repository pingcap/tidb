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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
)

func (w *worker) mviewRefreshSourceInfoContext(jobCtx *jobContext) context.Context {
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	if ctx.Value(kv.RequestSourceKey) == nil {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	}
	return ctx
}

func (w *worker) insertMViewRefreshSourceInfo(jobCtx *jobContext, mviewID int64, mlogIDs []int64) error {
	jobCtx.mustRollbackTxnOnError = true
	return mviewutil.InsertRefreshSourceIDs(w.mviewRefreshSourceInfoContext(jobCtx), w.sess.GetSQLExecutor(), mviewID, mlogIDs)
}

func (w *worker) deleteMViewRefreshSourceInfos(jobCtx *jobContext, mviewIDs []int64) error {
	if len(mviewIDs) == 0 {
		return nil
	}
	jobCtx.mustRollbackTxnOnError = true
	err := mviewutil.DeleteRefreshSourceInfo(w.mviewRefreshSourceInfoContext(jobCtx), w.sess.GetSQLExecutor(), mviewIDs)
	if infoschema.ErrTableNotExists.Equal(err) {
		// Cleanup must also work for jobs created before this table existed.
		return nil
	}
	return errors.Trace(err)
}

func (w *worker) migrateMViewRefreshSourceInfo(jobCtx *jobContext, oldMViewID, newMViewID int64, buildSources []model.MViewRefreshSource) error {
	ctx := w.mviewRefreshSourceInfoContext(jobCtx)
	// Keep this migration within the DDL step's full-transaction error handling.
	jobCtx.mustRollbackTxnOnError = true
	mlogIDs, err := mviewutil.ReadRefreshSourceIDs(ctx, w.sess.GetSQLExecutor(), oldMViewID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(mlogIDs) == 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: input log records are missing")
	}
	newIDs, err := mviewutil.ReadRefreshSourceIDs(ctx, w.sess.GetSQLExecutor(), newMViewID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(newIDs) != 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: shadow already has input log records")
	}
	if err := mviewutil.DeleteRefreshSourceInfo(ctx, w.sess.GetSQLExecutor(), []int64{oldMViewID}); err != nil {
		return err
	}
	return mviewutil.InsertRefreshSourceIDs(ctx, w.sess.GetSQLExecutor(), newMViewID, mviewutil.RefreshSourceLogIDs(buildSources))
}

// Validate the persisted build bindings against historical metadata, not the
// current input log. An upstream cutover after the build snapshot must not
// silently replace the baseline confirmed by that build.
func (w *worker) validateMViewRefreshBuildSources(job *model.Job, args *model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs) error {
	snapshot := meta.NewReader(w.store.GetSnapshot(kv.NewVersion(args.BuildReadTSO)))
	mv, err := snapshot.GetTable(job.SchemaID, args.OldMViewID)
	if err != nil {
		return errors.Trace(err)
	}
	if mv == nil || mv.MaterializedView == nil || len(args.BuildSources) == 0 ||
		len(args.BuildSources) != len(mv.MaterializedView.BaseTableIDs) {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: build source set differs from the build snapshot")
	}
	seen := make(map[int64]struct{}, len(args.BuildSources))
	for _, binding := range args.BuildSources {
		if _, ok := seen[binding.TableID]; ok || !slices.Contains(mv.MaterializedView.BaseTableIDs, binding.TableID) {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: invalid build source set")
		}
		seen[binding.TableID] = struct{}{}
		source, err := snapshot.GetTable(binding.SchemaID, binding.TableID)
		if err != nil {
			return errors.Trace(err)
		}
		if source == nil {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: build source is missing from the build snapshot")
		}
		var log *model.TableInfo
		if binding.MLogID != 0 {
			log, err = snapshot.GetTable(binding.SchemaID, binding.MLogID)
			if err != nil {
				return errors.Trace(err)
			}
		}
		actual, err := mviewutil.RefreshSourceBinding(binding.SchemaID, source, log)
		if err != nil {
			return err
		}
		if actual != binding {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view complete OUT OF PLACE cutover: input log binding differs from the build snapshot")
		}
	}
	return nil
}
