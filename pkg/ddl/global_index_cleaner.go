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

// CleanupLeftoverIndexes removes the leftover global index copies of a table from its meta and
// queues their data for delete range, in one transaction with a new schema version and schema
// diff. The copies are never read, and their columns, key flags and hidden columns are shared with
// the public index, so they are removed directly. It returns the dropped index IDs and the new
// schema version, or no IDs when the table has no leftovers.
func CleanupLeftoverIndexes(ctx context.Context, sctx sessionctx.Context, dbID, tableID int64) (indexIDs []int64, schemaVersion int64, err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	se := sess.NewSession(sctx)
	if err = se.Begin(ctx); err != nil {
		return nil, 0, errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			se.Rollback()
		}
	}()
	txn, err := se.Txn()
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	m := meta.NewMutator(txn)
	tblInfo, err := m.GetTable(dbID, tableID)
	if err != nil || tblInfo == nil {
		return nil, 0, errors.Trace(err)
	}
	leftovers := leftoverGlobalIndexes(tblInfo)
	if len(leftovers) == 0 {
		return nil, 0, nil
	}
	indexIDs = make([]int64, 0, len(leftovers))
	for _, idx := range leftovers {
		removeIndexInfo(tblInfo, idx)
		indexIDs = append(indexIDs, idx.ID)
	}
	if err = m.UpdateTable(dbID, tblInfo); err != nil {
		return nil, 0, errors.Trace(err)
	}
	ver, err := m.GenSchemaVersion()
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	if err = m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionDropIndex, SchemaID: dbID, TableID: tableID}); err != nil {
		return nil, 0, errors.Trace(err)
	}
	// The delete range rows are keyed by a job ID, use a fresh global ID so they do not clash with real jobs.
	delRangeID, err := m.GenGlobalID()
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	wrapper := newDelRangeExecWrapper(sctx)
	if err = wrapper.UpdateTSOForJob(); err != nil {
		return nil, 0, errors.Trace(err)
	}
	var ea elementIDAlloc
	if err = doBatchDeleteIndiceRange(ctx, wrapper, delRangeID, tableID, indexIDs, &ea, "leftover global index copies"); err != nil {
		return nil, 0, errors.Trace(err)
	}
	if err = se.Commit(ctx); err != nil {
		return nil, 0, errors.Trace(err)
	}
	committed = true
	logutil.DDLLogger().Info("dropped leftover global indexes from table meta", zap.Int64("tableID", tableID),
		zap.Int64s("indexIDs", indexIDs), zap.Int64("schemaVersion", ver), zap.Int64("delRangeID", delRangeID))
	return indexIDs, ver, nil
}
