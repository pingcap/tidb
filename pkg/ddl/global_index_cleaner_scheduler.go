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
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"go.uber.org/zap"
)

const (
	leftoverGlobalIndexRetryCnt = 3
	leftoverGlobalIndexTimeout  = 30 * time.Second
)

// cleanupLeftoverGlobalIndexes drops the global index copies left behind by REORGANIZE PARTITION
// jobs cancelled on older versions. It runs once when this node becomes the
// DDL owner, before any job is scheduled, and only scans partitioned tables.
func (s *jobScheduler) cleanupLeftoverGlobalIndexes() {
	is := s.infoCache.GetLatest()
	if is == nil {
		return
	}
	ctx, cancel := context.WithTimeout(s.schCtx, leftoverGlobalIndexTimeout)
	defer cancel()
	start := time.Now()
	var scanned, withLeftovers, cleaned int
	logutil.DDLLogger().Info("start cleaning up leftover global indexes", zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	defer func() {
		logutil.DDLLogger().Info("finish cleaning up leftover global indexes", zap.Duration("elapsed", time.Since(start)),
			zap.Int("scannedTables", scanned), zap.Int("tablesWithLeftovers", withLeftovers), zap.Int("cleanedTables", cleaned),
			zap.Bool("interrupted", ctx.Err() != nil))
	}()
	for _, db := range is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute) {
		dbInfo, ok := is.SchemaByName(db.DBName)
		if !ok {
			continue
		}
		for _, tblInfo := range db.TableInfos {
			if ctx.Err() != nil {
				return
			}
			scanned++
			if len(leftoverGlobalIndexes(tblInfo)) == 0 {
				continue
			}
			withLeftovers++
			var (
				dropped int
				err     error
			)
			for i := range leftoverGlobalIndexRetryCnt {
				if i > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(i) * 100 * time.Millisecond):
					}
				}
				dropped, err = s.dropLeftoverGlobalIndexes(ctx, dbInfo.ID, tblInfo.ID)
				if err == nil || isPermanentLeftoverCleanupErr(err) {
					break
				}
			}
			switch {
			case err == nil:
				if dropped > 0 {
					cleaned++
				}
			case infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err):
				// The cached infoschema is behind meta, e.g. the table was dropped or truncated.
				logutil.DDLLogger().Info("skip leftover global indexes of a table not in meta",
					zap.String("db", db.DBName.O), zap.String("table", tblInfo.Name.O), zap.Error(err))
			default:
				logutil.DDLLogger().Warn("failed to drop leftover global indexes",
					zap.String("db", db.DBName.O), zap.String("table", tblInfo.Name.O), zap.Error(err))
			}
		}
	}
}

func isPermanentLeftoverCleanupErr(err error) bool {
	return infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) ||
		errors.Cause(err) == errNoLeftoverIndexesGCJob
}

func (s *jobScheduler) dropLeftoverGlobalIndexes(ctx context.Context, dbID, tableID int64) (int, error) {
	sctx, err := s.sessPool.Get()
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer s.sessPool.Put(sctx)
	res, err := CleanupLeftoverIndexes(ctx, sctx, dbID, tableID)
	if err != nil || len(res.IndexIDs) == 0 {
		return 0, errors.Trace(err)
	}
	logutil.DDLLogger().Info("dropped leftover global indexes", zap.Int64("tableID", tableID),
		zap.Int("indexCount", len(res.IndexIDs)), zap.Int64("schemaVersion", res.SchemaVersion),
		zap.Int64("gcJobID", res.GCJob.ID), zap.Any("deleteRanges", res.DeleteRanges))
	if err = s.schemaVerSyncer.OwnerUpdateGlobalVersion(ctx, res.SchemaVersion); err != nil {
		logutil.DDLLogger().Warn("failed to update global schema version", zap.Int64("schemaVersion", res.SchemaVersion), zap.Error(err))
	}
	return len(res.IndexIDs), nil
}
