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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"go.uber.org/zap"
)

const leftoverGlobalIndexRetryCnt = 3

// cleanupLeftoverGlobalIndexes drops the global index copies left behind by REORGANIZE PARTITION
// jobs cancelled on older versions. It runs once when this node becomes the
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
	res, err := CleanupLeftoverIndexes(s.schCtx, sctx, dbID, tableID)
	if err != nil || len(res.IndexIDs) == 0 {
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("dropped leftover global indexes", zap.Int64("tableID", tableID),
		zap.Int("indexCount", len(res.IndexIDs)), zap.Int64("schemaVersion", res.SchemaVersion),
		zap.Int64("gcJobID", res.GCJob.ID), zap.Any("deleteRanges", res.DeleteRanges))
	if err = s.schemaVerSyncer.OwnerUpdateGlobalVersion(s.schCtx, res.SchemaVersion); err != nil {
		logutil.DDLLogger().Warn("failed to update global schema version", zap.Int64("schemaVersion", res.SchemaVersion), zap.Error(err))
	}
	return nil
}
