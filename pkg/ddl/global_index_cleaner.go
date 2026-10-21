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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
)

// leftoverGlobalIndexes returns the non-public global indexes of a table that is not being
// reorganized and that have a public index with the same name and columns, i.e. the copies left
// behind by a cancelled partition reorganization. Requiring the public twin keeps an index being
// added by ADD INDEX ... GLOBAL, which has a new name, from being treated as a leftover.
// DDLChangedIndex is not checked, since a successful REORGANIZE PARTITION keeps it.
// Unique copies are skipped: a rollback never leaves them behind, and removing a writable unique
// index without a delete only step can raise false duplicate key errors on stale schemas.
func leftoverGlobalIndexes(tblInfo *model.TableInfo) []*model.IndexInfo {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || pi.DDLState != model.StateNone || len(pi.AddingDefinitions) > 0 || len(pi.DroppingDefinitions) > 0 {
		return nil
	}
	var leftovers []*model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if !idx.Global || idx.Unique || idx.State == model.StatePublic {
			continue
		}
		if slices.ContainsFunc(tblInfo.Indices, func(other *model.IndexInfo) bool {
			return other.State == model.StatePublic && other.Name.L == idx.Name.L &&
				slices.EqualFunc(other.Columns, idx.Columns, func(a, b *model.IndexColumn) bool {
					return a.Name.L == b.Name.L && a.Length == b.Length
				})
		}) {
			leftovers = append(leftovers, idx)
		}
	}
	return leftovers
}

// gcNoopJobTypes are the job types for which the GC worker changes neither placement rules nor
// label rules when it finishes a delete range keyed by such a job, see doGCPlacementRules and
// doGCLabelRules in pkg/store/gcworker.
var gcNoopJobTypes = []model.ActionType{
	model.ActionCreateTable,
	model.ActionAddIndex,
	model.ActionDropIndex,
	model.ActionAddColumn,
	model.ActionModifyColumn,
}

// gcJobForLeftoverIndexes returns the latest finished history job of the table with a type in
// gcNoopJobTypes. The GC worker looks up the history job of each delete range and never completes a
// range whose job is missing, so the delete ranges of the leftover copies are keyed by this job.
func gcJobForLeftoverIndexes(ctx context.Context, se *sess.Session, m *meta.Mutator, tableID int64) (int64, error) {
	rows, err := se.Execute(ctx, "select job_id from mysql.tidb_ddl_history where table_ids = %? order by job_id desc",
		"get_table_history_jobs", strconv.FormatInt(tableID, 10))
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, row := range rows {
		job, err := m.GetHistoryDDLJob(row.GetInt64(0))
		if err != nil {
			return 0, errors.Trace(err)
		}
		if job != nil && job.TableID == tableID && (job.IsSynced() || job.IsDone()) && slices.Contains(gcNoopJobTypes, job.Type) {
			return job.ID, nil
		}
	}
	return 0, errors.Errorf("table %d has no finished DDL history job of types %v to key the delete ranges of its leftover global indexes",
		tableID, gcNoopJobTypes)
}

// maxDeleteRangeElementID returns the max element ID of the delete ranges of a job, done or not.
func maxDeleteRangeElementID(ctx context.Context, se *sess.Session, jobID int64) (int64, error) {
	rows, err := se.Execute(ctx, `select max(element_id) from (
		select element_id from mysql.gc_delete_range where job_id = %?
		union all select element_id from mysql.gc_delete_range_done where job_id = %?) r`,
		"get_max_delete_range_element_id", jobID, jobID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetInt64(0), nil
}

// CleanupLeftoverIndexes removes the leftover global index copies of a table from its meta and
// queues their data for delete range, in one transaction with a new schema version and schema
// diff. The copies are never read, and their columns, key flags and hidden columns are shared with
// the public index, so they are removed directly. No DDL job is created: the delete ranges reuse a
// history job of the table, see gcJobForLeftoverIndexes. It returns the dropped index IDs, the new
// schema version and the reused job ID, or no IDs when the table has no leftovers.
func CleanupLeftoverIndexes(ctx context.Context, sctx sessionctx.Context, dbID, tableID int64) (indexIDs []int64, schemaVersion, gcJobID int64, err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	se := sess.NewSession(sctx)
	if err = se.Begin(ctx); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			se.Rollback()
		}
	}()
	txn, err := se.Txn()
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	m := meta.NewMutator(txn)
	tblInfo, err := m.GetTable(dbID, tableID)
	if err != nil || tblInfo == nil {
		return nil, 0, 0, errors.Trace(err)
	}
	leftovers := leftoverGlobalIndexes(tblInfo)
	if len(leftovers) == 0 {
		return nil, 0, 0, nil
	}
	gcJobID, err = gcJobForLeftoverIndexes(ctx, se, m, tableID)
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	maxElementID, err := maxDeleteRangeElementID(ctx, se, gcJobID)
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	indexIDs = make([]int64, 0, len(leftovers))
	for _, idx := range leftovers {
		removeIndexInfo(tblInfo, idx)
		indexIDs = append(indexIDs, idx.ID)
	}
	if err = m.UpdateTable(dbID, tblInfo); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	ver, err := m.GenSchemaVersion()
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	if err = m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionDropIndex, SchemaID: dbID, TableID: tableID}); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	wrapper := newDelRangeExecWrapper(sctx)
	if err = wrapper.UpdateTSOForJob(); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	// (job_id, element_id) is unique in both delete range tables, and inserts into them ignore duplicates.
	ea := elementIDAlloc{offset: maxElementID}
	if err = doBatchDeleteIndiceRange(ctx, wrapper, gcJobID, tableID, indexIDs, &ea, "leftover global index copies"); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	if err = se.Commit(ctx); err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	committed = true
	logutil.DDLLogger().Info("dropped leftover global indexes from table meta", zap.Int64("tableID", tableID),
		zap.Int64s("indexIDs", indexIDs), zap.Int64("schemaVersion", ver), zap.Int64("gcJobID", gcJobID),
		zap.Int64("firstElementID", maxElementID+1))
	return indexIDs, ver, gcJobID, nil
}
