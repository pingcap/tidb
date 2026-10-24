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
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
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
	model.ActionCreateTables,
	model.ActionAddIndex,
	model.ActionDropIndex,
	model.ActionRenameIndex,
	model.ActionAddColumn,
	model.ActionModifyColumn,
}

// historyJobBatchSize is small because the job to use is usually among the newest jobs of the
// table, while TiKV scans the history of all tables backwards to fill a batch.
const historyJobBatchSize = 4

var errNoLeftoverIndexesGCJob = errors.New("no DDL history job to key the delete ranges of leftover global indexes")

// gcJobForLeftoverIndexes returns a history job of the table to key the delete ranges of the
// leftover copies, since the GC worker never completes a delete range whose history job is missing.
// It walks the history of the table from the newest job and takes the first finished job with a
// type in gcNoopJobTypes, or rolled back REORGANIZE PARTITION, which exists for every real leftover.
// For the latter, GC only drops the placement rules of the new partitions the rollback removed,
// whose IDs are never reused, and a job whose args GC cannot decode is skipped. Jobs are read by m.GetHistoryDDLJob, the lookup GC does through
// ddl.GetHistoryJobByID.
func gcJobForLeftoverIndexes(ctx context.Context, se *sess.Session, m *meta.Mutator, tableID int64) (*model.Job, error) {
	lastJobID := int64(math.MaxInt64)
	for {
		// Without the hint, the planner sorts a full scan by TopN instead of scanning job_id backwards.
		rows, err := se.Execute(ctx, "select /*+ order_index(mysql.tidb_ddl_history, primary) */ job_id from mysql.tidb_ddl_history "+
			"where job_id < %? and table_ids = %? order by job_id desc limit %?",
			"get_table_history_jobs", lastJobID, strconv.FormatInt(tableID, 10), historyJobBatchSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, row := range rows {
			lastJobID = row.GetInt64(0)
			job, err := m.GetHistoryDDLJob(lastJobID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if job == nil || job.TableID != tableID {
				continue
			}
			if (job.IsSynced() || job.IsDone()) && slices.Contains(gcNoopJobTypes, job.Type) {
				return job, nil
			}
			if job.Type == model.ActionReorganizePartition && job.IsRollbackDone() {
				// GC decodes these args to get the partitions whose placement rules to drop.
				if _, err = model.GetFinishedTablePartitionArgs(job); err != nil {
					logutil.DDLLogger().Warn("skip DDL history job whose args cannot be decoded", zap.Int64("jobID", job.ID), zap.Error(err))
					continue
				}
				return job, nil
			}
		}
		if len(rows) < historyJobBatchSize {
			return nil, errors.Annotatef(errNoLeftoverIndexesGCJob, "table %d", tableID)
		}
	}
}

// mergeIndexIDRanges merges the dropped index IDs into runs [first, last] of consecutive IDs. Only
// a kept index ID splits a run. Other IDs in a run belong to indexes dropped before, and index IDs
// are never reused, so deleting their key ranges again is harmless.
func mergeIndexIDRanges(keptIDs, droppedIDs []int64) [][2]int64 {
	kept := slices.Sorted(slices.Values(keptIDs))
	dropped := slices.Sorted(slices.Values(droppedIDs))
	var ranges [][2]int64
	k := 0
	for _, id := range dropped {
		if n := len(ranges); n > 0 {
			for k < len(kept) && kept[k] <= ranges[n-1][1] {
				k++
			}
			if k == len(kept) || kept[k] > id {
				ranges[n-1][1] = id
				continue
			}
		}
		ranges = append(ranges, [2]int64{id, id})
	}
	return ranges
}

// insertIndexIDRanges queues the index key ranges of the given index ID runs of a table for delete
// range, with element IDs from firstElementID.
func insertIndexIDRanges(ctx context.Context, wrapper DelRangeExecWrapper, jobID, tableID int64, ranges [][2]int64, firstElementID int64) error {
	var buf strings.Builder
	buf.WriteString(insertDeleteRangeSQLPrefix)
	wrapper.PrepareParamsList(len(ranges) * 5)
	for i, r := range ranges {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(insertDeleteRangeSQLValue)
		startKey := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, r[0]))
		endKey := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, r[1]+1))
		wrapper.AppendParamsList(jobID, firstElementID+int64(i), startKey, endKey)
	}
	return errors.Trace(wrapper.ConsumeDeleteRange(ctx, buf.String()))
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

// LeftoverIndexesCleanup is the result of CleanupLeftoverIndexes.
type LeftoverIndexesCleanup struct {
	IndexIDs      []int64
	SchemaVersion int64
	// GCJob is the history job that keys the delete ranges.
	GCJob *model.Job
	// DeleteRanges are the runs [first, last] of index IDs whose key ranges are queued for delete range.
	DeleteRanges [][2]int64
}

// CleanupLeftoverIndexes removes the leftover global index copies of a table from its meta and
// queues their data for delete range, in one transaction with a new schema version and schema
// diff. The copies are never read, and their columns, key flags and hidden columns are shared with
// the public index, so they are removed directly. No DDL job is created: the delete ranges reuse a
// history job of the table, see gcJobForLeftoverIndexes. It returns an empty result when the table
// has no leftovers, and ErrTableNotExists when the table is not in meta under dbID.
func CleanupLeftoverIndexes(ctx context.Context, sctx sessionctx.Context, dbID, tableID int64) (*LeftoverIndexesCleanup, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	se := sess.NewSession(sctx)
	if err := se.Begin(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			se.Rollback()
		}
	}()
	txn, err := se.Txn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := meta.NewMutator(txn)
	tblInfo, err := getTableInfo(m, tableID, dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	leftovers := leftoverGlobalIndexes(tblInfo)
	if len(leftovers) == 0 {
		return &LeftoverIndexesCleanup{}, nil
	}
	// A DDL step allocates its schema version before it commits the schema diff. Taking the next
	// version in between would make schema loaders skip the diff of that step.
	curVer, err := m.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	diffVer, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if curVer != diffVer {
		return nil, errors.Errorf("schema version %d has no schema diff yet, a DDL step may be in flight, retry later", curVer)
	}
	gcJob, err := gcJobForLeftoverIndexes(ctx, se, m, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	maxElementID, err := maxDeleteRangeElementID(ctx, se, gcJob.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	indexIDs := make([]int64, 0, len(leftovers))
	leftoverIDs := make(map[int64]struct{}, len(leftovers))
	for _, idx := range leftovers {
		indexIDs = append(indexIDs, idx.ID)
		leftoverIDs[idx.ID] = struct{}{}
	}
	tblInfo.Indices = slices.DeleteFunc(tblInfo.Indices, func(idx *model.IndexInfo) bool {
		_, ok := leftoverIDs[idx.ID]
		return ok
	})
	keptIDs := make([]int64, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		keptIDs = append(keptIDs, idx.ID)
	}
	ranges := mergeIndexIDRanges(keptIDs, indexIDs)
	if err = m.UpdateTable(dbID, tblInfo); err != nil {
		return nil, errors.Trace(err)
	}
	ver, err := m.GenSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionDropIndex, SchemaID: dbID, TableID: tableID}); err != nil {
		return nil, errors.Trace(err)
	}
	wrapper := newDelRangeExecWrapper(sctx)
	if err = wrapper.UpdateTSOForJob(); err != nil {
		return nil, errors.Trace(err)
	}
	// (job_id, element_id) is unique in both delete range tables, and inserts into them ignore duplicates.
	if err = insertIndexIDRanges(ctx, wrapper, gcJob.ID, tableID, ranges, maxElementID+1); err != nil {
		return nil, errors.Trace(err)
	}
	if err = se.Commit(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	committed = true
	logutil.DDLLogger().Info("dropped leftover global indexes from table meta", zap.Int64("tableID", tableID),
		zap.Int64s("indexIDs", indexIDs), zap.Int64("schemaVersion", ver), zap.Int64("gcJobID", gcJob.ID), zap.Stringer("gcJobType", gcJob.Type),
		zap.Any("deleteRanges", ranges), zap.Int64("firstElementID", maxElementID+1))
	return &LeftoverIndexesCleanup{IndexIDs: indexIDs, SchemaVersion: ver, GCJob: gcJob, DeleteRanges: ranges}, nil
}
