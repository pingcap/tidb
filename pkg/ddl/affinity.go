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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

func buildAffinityGroupKeyRange(codec tikv.Codec, physicalID int64) pdhttp.AffinityGroupKeyRange {
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
func buildAffinityGroupDefinitions(codec tikv.Codec, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
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

// createTableAffinityGroupsInPD creates affinity groups for a table in PD.
// This is a critical operation. If it fails, the DDL should fail.
// Used by: CREATE TABLE, ALTER TABLE AFFINITY = 'xxx', TRUNCATE TABLE, TRUNCATE PARTITION.
func createTableAffinityGroupsInPD(jobCtx *jobContext, tblInfo *model.TableInfo) error {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil
	}

	ctx := jobCtx.stepCtx
	codec := jobCtx.store.GetCodec()

	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if len(groups) == 0 {
		return nil
	}

	logutil.DDLLogger().Info("creating affinity groups in PD",
		zap.Int64("tableID", tblInfo.ID),
		zap.Strings("groupIDs", collectAffinityGroupIDs(groups)))
	if err := infosync.CreateAffinityGroupsIfNotExists(ctx, groups); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// deleteTableAffinityGroupsInPD deletes affinity groups for a table in PD.
// This is a best-effort cleanup operation. Failures are logged but the operation continues.
// Used by: DROP TABLE, ALTER TABLE AFFINITY = ”, TRUNCATE TABLE, TRUNCATE PARTITION.
func deleteTableAffinityGroupsInPD(jobCtx *jobContext, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) error {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil
	}

	ctx := jobCtx.stepCtx
	codec := jobCtx.store.GetCodec()

	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
	if err != nil {
		return errors.Trace(err)
	}

	if len(groups) == 0 {
		return nil
	}

	ids := collectAffinityGroupIDs(groups)
	return infosync.DeleteAffinityGroupsWithDefaultRetry(ctx, ids)
}

// batchDeleteTableAffinityGroups deletes affinity groups for multiple tables in PD.
// This is used for DROP DATABASE to clean up all table affinity groups at once.
// Deletion failures are logged but don't fail the operation (best-effort cleanup).
func batchDeleteTableAffinityGroups(jobCtx *jobContext, tables []*model.TableInfo) {
	if len(tables) == 0 {
		return
	}

	ctx := jobCtx.stepCtx
	codec := jobCtx.store.GetCodec()

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
	codec tikv.Codec,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	return buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
}
