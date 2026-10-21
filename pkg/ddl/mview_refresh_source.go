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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
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

func (w *worker) migrateMViewRefreshSourceInfo(jobCtx *jobContext, oldMViewID, newMViewID int64) error {
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
	_, err = w.sess.Execute(ctx,
		"UPDATE mysql.tidb_mview_fast_refresh_source_info SET MVIEW_ID = %? WHERE MVIEW_ID = %?",
		"mview-refresh-cutover-migrate-source-info", newMViewID, oldMViewID)
	return errors.Trace(err)
}
