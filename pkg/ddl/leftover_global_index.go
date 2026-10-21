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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

// cleanupLeftoverGlobalIndexes drops the global index copies left behind by REORGANIZE PARTITION
// jobs cancelled on older versions. It runs once when this node becomes the DDL owner and only
// scans partitioned tables.
func (s *jobScheduler) cleanupLeftoverGlobalIndexes() {
	if !config.GetGlobalConfig().Experimental.CleanupLeftoverGlobalIndexes {
		return
	}
	is := s.infoCache.GetLatest()
	if is == nil {
		return
	}
	for _, db := range is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute) {
		dbInfo, ok := is.SchemaByName(db.DBName)
		if !ok {
			continue
		}
		for _, tblInfo := range db.TableInfos {
			if len(leftoverGlobalIndexes(tblInfo)) == 0 {
				continue
			}
			if err := s.dropLeftoverGlobalIndexes(dbInfo.ID, tblInfo.ID); err != nil {
				logutil.DDLLogger().Warn("failed to drop leftover global indexes",
					zap.String("db", db.DBName.O), zap.String("table", tblInfo.Name.O), zap.Error(err))
			}
		}
	}
}

// leftoverGlobalIndexes returns the non-public global indexes of a table that is not being
// reorganized and that have a public index with the same name and columns. Such an index is a
// copy left behind by a cancelled partition reorganization, while an index being added never
// shares its name with a public one.
func leftoverGlobalIndexes(tblInfo *model.TableInfo) []*model.IndexInfo {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || pi.DDLState != model.StateNone || len(pi.DDLChangedIndex) > 0 {
		return nil
	}
	var leftovers []*model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if !idx.Global || idx.State == model.StatePublic {
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

// dropLeftoverGlobalIndexes removes the leftover copies from the table meta and queues their data
// for delete range in one transaction. The copies are never read, and their columns, key flags
// and hidden columns are shared with the public index, so they are removed directly.
func (s *jobScheduler) dropLeftoverGlobalIndexes(dbID, tableID int64) error {
	sctx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sctx)
	se := sess.NewSession(sctx)
	ctx := kv.WithInternalSourceType(s.schCtx, kv.InternalTxnDDL)
	if err = se.Begin(ctx); err != nil {
		return errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			se.Rollback()
		}
	}()
	txn, err := se.Txn()
	if err != nil {
		return errors.Trace(err)
	}
	m := meta.NewMutator(txn)
	tblInfo, err := m.GetTable(dbID, tableID)
	if err != nil || tblInfo == nil {
		return errors.Trace(err)
	}
	leftovers := leftoverGlobalIndexes(tblInfo)
	if len(leftovers) == 0 {
		return nil
	}
	indexIDs := make([]int64, 0, len(leftovers))
	for _, idx := range leftovers {
		removeIndexInfo(tblInfo, idx)
		indexIDs = append(indexIDs, idx.ID)
	}
	if err = m.UpdateTable(dbID, tblInfo); err != nil {
		return errors.Trace(err)
	}
	ver, err := m.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}
	if err = m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionDropIndex, SchemaID: dbID, TableID: tableID}); err != nil {
		return errors.Trace(err)
	}
	// The delete range rows are keyed by a job ID, use a fresh global ID so they do not clash with real jobs.
	delRangeID, err := m.GenGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	wrapper := newDelRangeExecWrapper(sctx)
	if err = wrapper.UpdateTSOForJob(); err != nil {
		return errors.Trace(err)
	}
	var ea elementIDAlloc
	if err = doBatchDeleteIndiceRange(ctx, wrapper, delRangeID, tableID, indexIDs, &ea, "leftover global index copies"); err != nil {
		return errors.Trace(err)
	}
	if err = se.Commit(ctx); err != nil {
		return errors.Trace(err)
	}
	committed = true
	logutil.DDLLogger().Info("dropped leftover global indexes", zap.Int64("tableID", tableID),
		zap.Int64s("indexIDs", indexIDs), zap.Int64("schemaVersion", ver), zap.Int64("delRangeID", delRangeID))
	return errors.Trace(s.schemaVerSyncer.OwnerUpdateGlobalVersion(ctx, ver))
}
