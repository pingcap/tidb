// Copyright 2025 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// AffinityKeyRangeCodec encodes start/end keys to match the storage layout (e.g. keyspace aware).
type AffinityKeyRangeCodec interface {
	EncodeRegionRange(start, end []byte) ([]byte, []byte)
}

func buildAffinityGroupKeyRange(codec AffinityKeyRangeCodec, physicalID int64) pdhttp.AffinityGroupKeyRange {
	startKey := tablecodec.EncodeTablePrefix(physicalID)
	endKey := tablecodec.EncodeTablePrefix(physicalID + 1)
	if codec != nil {
		startKey, endKey = codec.EncodeRegionRange(startKey, endKey)
	}
	return pdhttp.AffinityGroupKeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

// buildAffinityGroupDefinitions constructs affinity group definitions based on table's affinity configuration.
// It generates affinity group IDs in two different formats depending on the affinity level:
//   - Table-level affinity: "_tidb_t_{tableID}" - one group for the entire table
//   - Partition-level affinity: "_tidb_pt_{tableID}_p{partitionID}" - one group per partition
func buildAffinityGroupDefinitions(codec AffinityKeyRangeCodec, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil, nil
	}

	switch tblInfo.Affinity.Level {
	case ast.TableAffinityLevelTable:
		groupID := fmt.Sprintf("_tidb_t_%d", tblInfo.ID)
		return map[string][]pdhttp.AffinityGroupKeyRange{
			groupID: {buildAffinityGroupKeyRange(codec, tblInfo.ID)},
		}, nil
	case ast.TableAffinityLevelPartition:
		definitions := partitionDefs
		if definitions == nil && tblInfo.Partition != nil {
			definitions = tblInfo.Partition.Definitions
		}
		if len(definitions) == 0 {
			return nil, errors.Errorf("partition affinity requires partition definitions for table %s", tblInfo.Name.O)
		}

		groups := make(map[string][]pdhttp.AffinityGroupKeyRange, len(definitions))
		for _, def := range definitions {
			groupID := fmt.Sprintf("_tidb_pt_%d_p%d", tblInfo.ID, def.ID)
			groups[groupID] = []pdhttp.AffinityGroupKeyRange{buildAffinityGroupKeyRange(codec, def.ID)}
		}
		return groups, nil
	default:
		return nil, errors.Errorf("invalid affinity level: %s", tblInfo.Affinity.Level)
	}
}

func collectAffinityGroupIDs(groups map[string][]pdhttp.AffinityGroupKeyRange) []string {
	if len(groups) == 0 {
		return nil
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	return ids
}

// updateTableAffinityGroupInPD updates affinity groups in PD for DDL operations.
// It handles both creation and deletion of affinity groups:
//   - newTblInfo: the new table info (for CREATE/ALTER/TRUNCATE), or nil (for DROP)
//   - oldTblInfo: the old table info (for ALTER/TRUNCATE/DROP), or nil (for CREATE)
//   - oldPartitionDefs: explicit old partition definitions (for TRUNCATE PARTITION where
//     oldTblInfo.Partition.Definitions may already be updated), or nil to use oldTblInfo.Partition.Definitions
//
// Creation failures will fail the DDL (critical operation).
// Deletion failures are logged but don't fail the DDL (best-effort cleanup).
func updateTableAffinityGroupInPD(jobCtx *jobContext, newTblInfo *model.TableInfo, oldTblInfo *model.TableInfo, oldPartitionDefs []model.PartitionDefinition) error {
	tableID := int64(0)
	if newTblInfo != nil {
		tableID = newTblInfo.ID
	} else if oldTblInfo != nil {
		tableID = oldTblInfo.ID
	}

	ctx := context.Background()
	var codec AffinityKeyRangeCodec
	if jobCtx != nil {
		if jobCtx.stepCtx != nil {
			ctx = jobCtx.stepCtx
		} else if jobCtx.ctx != nil {
			ctx = jobCtx.ctx
		}
		if jobCtx.store != nil {
			codec = jobCtx.store.GetCodec()
		}
	}

	oldGroups, err := buildAffinityGroupDefinitions(codec, oldTblInfo, oldPartitionDefs)
	if err != nil {
		return errors.Trace(err)
	}
	newGroups, err := buildAffinityGroupDefinitions(codec, newTblInfo, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if len(oldGroups) == 0 && len(newGroups) == 0 {
		return nil
	}

	// Create new affinity groups first (critical operation for: CREATE TABLE, ALTER TABLE AFFINITY = 'xxx', TRUNCATE)
	// If this fails, the DDL should fail.
	if len(newGroups) > 0 {
		logutil.DDLLogger().Info("creating affinity groups in PD", zap.Int64("tableID", tableID), zap.Strings("groupIDs", collectAffinityGroupIDs(newGroups)))
		if err := infosync.CreateAffinityGroupsIfNotExists(ctx, newGroups); err != nil {
			return errors.Trace(err)
		}
	}

	// Delete old affinity groups (cleanup operation for: ALTER TABLE AFFINITY = '', TRUNCATE, DROP TABLE)
	// If this fails, just log warning and continue. The orphan groups in PD won't cause correctness issues.
	if len(oldGroups) > 0 {
		ids := collectAffinityGroupIDs(oldGroups)
		logutil.DDLLogger().Info("deleting old affinity groups in PD", zap.Int64("tableID", tableID), zap.Strings("groupIDs", ids))
		if err := infosync.DeleteAffinityGroupsWithDefaultRetry(ctx, ids); err != nil {
			logutil.DDLLogger().Warn("failed to delete old affinity groups, but operation will continue",
				zap.Error(err),
				zap.Int64("tableID", tableID),
				zap.Strings("groupIDs", ids))
		}
	}

	return nil
}

// batchDeleteTableAffinityGroups deletes affinity groups for multiple tables in PD.
// This is used for DROP DATABASE to clean up all table affinity groups at once.
// Deletion failures are logged but don't fail the operation (best-effort cleanup).
func batchDeleteTableAffinityGroups(jobCtx *jobContext, tables []*model.TableInfo) {
	if len(tables) == 0 {
		return
	}

	ctx := context.Background()
	var codec AffinityKeyRangeCodec
	if jobCtx != nil {
		if jobCtx.stepCtx != nil {
			ctx = jobCtx.stepCtx
		} else if jobCtx.ctx != nil {
			ctx = jobCtx.ctx
		}
		if jobCtx.store != nil {
			codec = jobCtx.store.GetCodec()
		}
	}

	groupIDs := make(map[string]struct{})
	for _, tblInfo := range tables {
		groups, err := buildAffinityGroupDefinitions(codec, tblInfo, nil)
		if err != nil {
			logutil.DDLLogger().Warn("failed to build affinity group definitions, but operation will continue",
				zap.Error(err),
				zap.Int64("tableID", tblInfo.ID))
			continue
		}
		for id := range groups {
			groupIDs[id] = struct{}{}
		}
	}
	if len(groupIDs) == 0 {
		return
	}

	ids := make([]string, 0, len(groupIDs))
	for id := range groupIDs {
		ids = append(ids, id)
	}

	logutil.DDLLogger().Info("deleting affinity groups for batch tables", zap.Strings("groupIDs", ids))
	if err := infosync.DeleteAffinityGroupsWithDefaultRetry(ctx, ids); err != nil {
		logutil.DDLLogger().Warn("failed to delete affinity groups for batch tables, but operation will continue",
			zap.Error(err),
			zap.Strings("groupIDs", ids))
	}
}

// BuildAffinityGroupDefinitionsForTest is exported for testing.
func BuildAffinityGroupDefinitionsForTest(
	codec AffinityKeyRangeCodec,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	return buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
}
