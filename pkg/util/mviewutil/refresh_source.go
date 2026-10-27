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

package mviewutil

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// RefreshSourceInfoTableName stores each MV's registered input MLog identities.
const RefreshSourceInfoTableName = "tidb_mview_fast_refresh_source_info"

// ReadRefreshSourceIDs reads the complete registered log set in the caller's transaction.
// MLOG_ID zero is the placeholder for a source without a log.
func ReadRefreshSourceIDs(ctx context.Context, exec sqlexec.SQLExecutor, mviewID int64) ([]int64, error) {
	rows, err := sqlexec.ExecSQL(ctx, exec,
		"SELECT MLOG_ID FROM mysql."+RefreshSourceInfoTableName+" WHERE MVIEW_ID = %? ORDER BY MLOG_ID", mviewID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.GetInt64(0))
	}
	return ids, nil
}

// InsertRefreshSourceIDs registers a whole log set in the caller's transaction.
// Duplicate records are errors; ignoring them could combine different baselines.
func InsertRefreshSourceIDs(ctx context.Context, exec sqlexec.SQLExecutor, mviewID int64, mlogIDs []int64) error {
	if len(mlogIDs) == 0 {
		return nil
	}
	args := make([]any, 0, len(mlogIDs)*2)
	for _, mlogID := range mlogIDs {
		args = append(args, mviewID, mlogID)
	}
	// Only placeholder counts are dynamic; values are escaped by ExecuteInternal.
	_, err := sqlexec.ExecSQL(ctx, exec,
		"INSERT INTO mysql."+RefreshSourceInfoTableName+" (MVIEW_ID, MLOG_ID) VALUES "+strings.Repeat("(%?, %?),", len(mlogIDs)-1)+"(%?, %?)", args...)
	return errors.Trace(err)
}

// DeleteRefreshSourceInfo removes complete log sets in the caller's transaction.
func DeleteRefreshSourceInfo(ctx context.Context, exec sqlexec.SQLExecutor, mviewIDs []int64) error {
	const batchSize = 1000
	for start := 0; start < len(mviewIDs); start += batchSize {
		batch := mviewIDs[start:min(start+batchSize, len(mviewIDs))]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		// Only placeholder counts are dynamic; values are escaped by ExecuteInternal.
		_, err := sqlexec.ExecSQL(ctx, exec,
			"DELETE FROM mysql."+RefreshSourceInfoTableName+" WHERE MVIEW_ID IN ("+strings.Repeat("%?,", len(batch)-1)+"%?)", args...)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveRefreshSources resolves source table IDs and their recorded MLog IDs in the supplied schema.
// MLogID is zero when a source has no materialized view log.
func ResolveRefreshSources(ctx context.Context, is infoschema.InfoSchema, mviewID int64) ([]model.MViewRefreshSource, error) {
	mv, ok := is.TableByID(ctx, mviewID)
	if !ok || mv.Meta().MaterializedView == nil {
		return nil, errors.Errorf("refresh materialized view: target %d is missing from the computation schema", mviewID)
	}
	sources := make([]model.MViewRefreshSource, 0, len(mv.Meta().MaterializedView.BaseTableIDs))
	for _, id := range mv.Meta().MaterializedView.BaseTableIDs {
		tbl, ok := is.TableByID(ctx, id)
		if !ok {
			return nil, errors.Errorf("refresh materialized view: source %d is missing from the computation schema", id)
		}
		source := tbl.Meta()
		binding := model.MViewRefreshSource{TableID: source.ID}
		if source.MaterializedViewBase != nil {
			binding.MLogID = source.MaterializedViewBase.MLogID
		}
		sources = append(sources, binding)
	}
	slices.SortFunc(sources, func(a, b model.MViewRefreshSource) int { return cmp.Compare(a.TableID, b.TableID) })
	if len(sources) == 0 {
		return nil, errors.New("refresh materialized view: source set is empty")
	}
	return sources, nil
}

// RefreshSourceLogIDs returns the sorted, distinct whole log set, including a
// single zero placeholder if any source has no log.
func RefreshSourceLogIDs(sources []model.MViewRefreshSource) []int64 {
	ids := make([]int64, 0, len(sources))
	for _, source := range sources {
		ids = append(ids, source.MLogID)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// ReplaceRefreshSourceIDs atomically replaces a baseline in the caller's transaction.
func ReplaceRefreshSourceIDs(ctx context.Context, exec sqlexec.SQLExecutor, mviewID int64, mlogIDs []int64) error {
	if err := DeleteRefreshSourceInfo(ctx, exec, []int64{mviewID}); err != nil {
		return err
	}
	return InsertRefreshSourceIDs(ctx, exec, mviewID, mlogIDs)
}
