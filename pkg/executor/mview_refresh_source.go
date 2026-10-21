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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
)

// initMVRefreshSourceInfo runs before the refresh mutex and data transaction.
// A legacy MV has only ordinary-table sources, whose log identities cannot
// change under the currently supported DDL. New MVs register at CREATE time.
func (e *RefreshMaterializedViewExec) initMVRefreshSourceInfo(ctx context.Context, sctx sessionctx.Context, mviewID int64) error {
	// Retry the entire initialization, including the metadata checks and the
	// refresh-info conflict write, rather than replaying only its SQL statements.
	sessVars := sctx.GetSessionVars()
	retryLimit := sessVars.RetryLimit
	sessVars.RetryLimit = 0
	defer func() { sessVars.RetryLimit = retryLimit }()
	var err error
	for range 3 {
		err = e.tryInitMVRefreshSourceInfo(ctx, sctx, mviewID)
		if err == nil || (!kv.IsTxnRetryableError(err) && !kv.ErrKeyExists.Equal(err)) {
			return err
		}
	}
	return err
}

func (e *RefreshMaterializedViewExec) tryInitMVRefreshSourceInfo(ctx context.Context, sctx sessionctx.Context, mviewID int64) error {
	sqlExec := sctx.GetSQLExecutor()
	if _, err := sqlExec.ExecuteInternal(ctx, "BEGIN OPTIMISTIC"); err != nil {
		return errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = sqlExec.ExecuteInternal(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	txn, err := sctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	is, err := domain.GetDomain(e.Ctx()).GetSnapshotInfoSchema(txn.StartTS())
	if err != nil {
		return errors.Trace(err)
	}
	registered, err := mviewutil.ReadRefreshSourceIDs(ctx, sqlExec, mviewID)
	if err != nil || len(registered) != 0 {
		return errors.Trace(err)
	}
	mview, ok := is.TableByID(ctx, mviewID)
	if !ok {
		return errors.Errorf("refresh materialized view: materialized view %d does not exist", mviewID)
	}
	if mview.Meta().State != model.StatePublic || mview.Meta().MaterializedView == nil || !mview.Meta().MaterializedView.GetInitBuildState().IsReady() {
		return errors.New("refresh materialized view: cannot initialize input logs for an unavailable materialized view")
	}
	mlogIDs, err := legacyMVRefreshSourceIDs(ctx, is, mview.Meta())
	if err != nil {
		return err
	}

	refreshInfo, err := is.TableByName(ctx, pmodel.NewCIStr(mysql.SystemDB), pmodel.NewCIStr("tidb_mview_refresh_info"))
	if err != nil {
		return errors.Trace(err)
	}
	// Preserve the exact value, but write this existing row so DROP and target
	// OOP cannot commit cleanup from a snapshot that missed our new records.
	// Whichever transaction loses the write conflict retries with fresh state.
	key := tablecodec.EncodeRecordKey(refreshInfo.RecordPrefix(), kv.IntHandle(mviewID))
	value, err := txn.Get(ctx, key)
	if kv.IsErrNotFound(err) {
		return errors.New("refresh materialized view: required refresh info row is missing")
	}
	if err != nil {
		return errors.Trace(err)
	}
	if err := txn.Set(key, value); err != nil {
		return errors.Trace(err)
	}
	if err := mviewutil.InsertRefreshSourceIDs(ctx, sqlExec, mviewID, mlogIDs); err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("beforeMViewRefreshSourceInfoInitCommit", mviewID)
	if _, err := sqlExec.ExecuteInternal(ctx, "COMMIT"); err != nil {
		return errors.Trace(err)
	}
	committed = true
	return nil
}

func legacyMVRefreshSourceIDs(ctx context.Context, is infoschema.InfoSchema, mview *model.TableInfo) ([]int64, error) {
	if len(mview.MaterializedView.BaseTableIDs) == 0 {
		return nil, errors.New("refresh materialized view: source table IDs are missing")
	}
	ids := make(map[int64]struct{}, len(mview.MaterializedView.BaseTableIDs))
	for _, sourceID := range mview.MaterializedView.BaseTableIDs {
		source, ok := is.TableByID(ctx, sourceID)
		if !ok {
			return nil, errors.Errorf("refresh materialized view: source table %d does not exist", sourceID)
		}
		meta := source.Meta()
		if meta.MaterializedView != nil {
			return nil, errors.New("refresh materialized view: input log records are missing for a nested materialized view")
		}
		if meta.IsView() || meta.IsSequence() || meta.TempTableType != model.TempTableNone ||
			meta.MaterializedViewLog != nil || meta.MaterializedViewShadow != nil || meta.State != model.StatePublic {
			return nil, errors.Errorf("refresh materialized view: invalid source table %d", sourceID)
		}
		if meta.MaterializedViewBase == nil || meta.MaterializedViewBase.MLogID == 0 {
			return nil, errors.Errorf("refresh materialized view: source table %d has no materialized view log", sourceID)
		}
		mlogID := meta.MaterializedViewBase.MLogID
		mlog, ok := is.TableByID(ctx, mlogID)
		if !ok || mlog.Meta().MaterializedViewLog == nil || mlog.Meta().MaterializedViewLog.BaseTableID != sourceID {
			return nil, errors.Errorf("refresh materialized view: invalid log %d for source table %d", mlogID, sourceID)
		}
		ids[mlogID] = struct{}{}
	}
	mlogIDs := make([]int64, 0, len(ids))
	for id := range ids {
		mlogIDs = append(mlogIDs, id)
	}
	slices.Sort(mlogIDs)
	return mlogIDs, nil
}
