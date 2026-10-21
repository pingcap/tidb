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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
)

const leftoverGlobalIndexRetryCnt = 3

// cleanupLeftoverGlobalIndexes drops the leftover global indexes, e.g. the copies left behind by
// REORGANIZE PARTITION jobs cancelled on older versions. It runs once when this node becomes the
// DDL owner, before any job is scheduled, and only scans partitioned tables.
func (s *jobScheduler) cleanupLeftoverGlobalIndexes() {
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
			if s.schCtx.Err() != nil {
				return
			}
			if len(leftoverGlobalIndexes(tblInfo)) == 0 {
				continue
			}
			var err error
			for i := range leftoverGlobalIndexRetryCnt {
				if i > 0 {
					select {
					case <-s.schCtx.Done():
						return
					case <-time.After(time.Duration(i) * 100 * time.Millisecond):
					}
				}
				if err = s.dropLeftoverGlobalIndexes(dbInfo.ID, tblInfo.ID); err == nil {
					break
				}
			}
			if err != nil {
				logutil.DDLLogger().Warn("failed to drop leftover global indexes",
					zap.String("db", db.DBName.O), zap.String("table", tblInfo.Name.O), zap.Error(err))
			}
		}
	}
}

func (s *jobScheduler) dropLeftoverGlobalIndexes(dbID, tableID int64) error {
	sctx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sctx)
	indexIDs, ver, err := CleanupLeftoverIndexes(s.schCtx, sctx, dbID, tableID)
	if err != nil || len(indexIDs) == 0 {
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("dropped leftover global indexes", zap.Int64("tableID", tableID),
		zap.Int("indexCount", len(indexIDs)), zap.Int64("schemaVersion", ver))
	if err = s.schemaVerSyncer.OwnerUpdateGlobalVersion(s.schCtx, ver); err != nil {
		logutil.DDLLogger().Warn("failed to update global schema version", zap.Int64("schemaVersion", ver), zap.Error(err))
	}
	return nil
}

// leftoverGlobalIndexes returns the non-public global indexes of a table that is not being
// reorganized. They are leftovers, e.g. copies left behind by a cancelled partition
// reorganization, only if no DDL job on the table is in progress, which this function does not
// check. DDLChangedIndex is not checked, since a successful REORGANIZE PARTITION keeps it.
func leftoverGlobalIndexes(tblInfo *model.TableInfo) []*model.IndexInfo {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || pi.DDLState != model.StateNone || len(pi.AddingDefinitions) > 0 || len(pi.DroppingDefinitions) > 0 {
		return nil
	}
	var leftovers []*model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Global && idx.State != model.StatePublic {
			leftovers = append(leftovers, idx)
		}
	}
	return leftovers
}

// CleanupLeftoverIndexes removes the leftover global indexes of a table from its meta like
// DROP INDEX does, together with the column flags and hidden columns no other index uses, and
// queues their data for delete range, in one transaction with a new schema version and schema
// diff. It returns the dropped index IDs and the new schema version, or no IDs when the table has
// no leftovers.
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
		DropIndexColumnFlag(tblInfo, idx)
		RemoveDependentHiddenColumns(tblInfo, idx)
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
	if err = doBatchDeleteIndiceRange(ctx, wrapper, delRangeID, tableID, indexIDs, &ea, "leftover global indexes"); err != nil {
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
