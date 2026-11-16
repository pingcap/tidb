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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// GetTableAffinityGroupID returns the affinity group ID for a table, including
// the keyspace ID when the table uses a NextGen keyspace codec.
// Classic format: _tidb_t_{tableID}.
// NextGen format: _tidb_ks{keyspaceID}_t_{tableID}.
func GetTableAffinityGroupID(codec tikv.Codec, tableID int64) string {
	if label.UseKeyspaceAwareRules(codec) {
		return fmt.Sprintf("_tidb_ks%d_t_%d", codec.GetKeyspaceID(), tableID)
	}
	return fmt.Sprintf("_tidb_t_%d", tableID)
}

// GetPartitionAffinityGroupID returns the affinity group ID for a partition,
// including the keyspace ID when the partition uses a NextGen keyspace codec.
// Classic format: _tidb_pt_{tableID}_p{partitionID}.
// NextGen format: _tidb_ks{keyspaceID}_pt_{tableID}_p{partitionID}.
func GetPartitionAffinityGroupID(codec tikv.Codec, tableID, partitionID int64) string {
	if label.UseKeyspaceAwareRules(codec) {
		return fmt.Sprintf("_tidb_ks%d_pt_%d_p%d", codec.GetKeyspaceID(), tableID, partitionID)
	}
	return fmt.Sprintf("_tidb_pt_%d_p%d", tableID, partitionID)
}

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
// It generates affinity group IDs in two different formats depending on the affinity level.
// In NextGen, the keyspace ID is included with an explicit marker to keep IDs
// unique when different keyspaces allocate the same table ID.
func buildAffinityGroupDefinitions(codec tikv.Codec, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil, nil
	}

	switch tblInfo.Affinity.Level {
	case ast.TableAffinityLevelTable:
		groupID := GetTableAffinityGroupID(codec, tblInfo.ID)
		return map[string][]pdhttp.AffinityGroupKeyRange{
			groupID: {buildAffinityGroupKeyRange(codec, tblInfo.ID)},
		}, nil
	case ast.TableAffinityLevelPartition:
		definitions := partitionDefs
		if definitions == nil && tblInfo.GetPartitionInfo() != nil {
			definitions = tblInfo.GetPartitionInfo().Definitions
		}
		if len(definitions) == 0 {
			return nil, errors.Errorf("partition affinity requires partition definitions for table %s (ID: %d), table metadata may be corrupted", tblInfo.Name.O, tblInfo.ID)
		}

		groups := make(map[string][]pdhttp.AffinityGroupKeyRange, len(definitions))
		for _, def := range definitions {
			groupID := GetPartitionAffinityGroupID(codec, tblInfo.ID, def.ID)
			groups[groupID] = []pdhttp.AffinityGroupKeyRange{buildAffinityGroupKeyRange(codec, def.ID)}
		}
		return groups, nil
	default:
		return nil, errors.Errorf("invalid affinity level: %s for table %s (ID: %d)", tblInfo.Affinity.Level, tblInfo.Name.O, tblInfo.ID)
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
	if err := affinity.CreateGroupsIfNotExists(ctx, groups); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// deleteTableAffinityGroupsInPD deletes affinity groups for a table in PD.
// This is a best-effort cleanup operation. Failures are logged but the operation continues.
// Used by: DROP TABLE, ALTER TABLE AFFINITY = ”, TRUNCATE TABLE, TRUNCATE PARTITION.
// TODO: add gc for unused affinity groups, which is similar to the placement rules.
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
	return affinity.DeleteGroupsWithRetry(ctx, ids)
}

// collectTableAffinityGroupIDs adds the affinity group IDs of a table to groupIDs.
func collectTableAffinityGroupIDs(
	codec tikv.Codec,
	tblInfo *model.TableInfo,
	groupIDs map[string]struct{},
) error {
	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for id := range groups {
		groupIDs[id] = struct{}{}
	}
	return nil
}

// batchDeleteTableAffinityGroups deletes affinity groups for multiple tables in PD.
// This is used for DROP DATABASE to clean up all table affinity groups at once.
// Returns error to let the caller decide whether to continue or fail.
func batchDeleteTableAffinityGroups(jobCtx *jobContext, groupIDs map[string]struct{}) error {
	if len(groupIDs) == 0 {
		return nil
	}

	ctx := jobCtx.stepCtx
	ids := make([]string, 0, len(groupIDs))
	for id := range groupIDs {
		ids = append(ids, id)
	}

	logutil.DDLLogger().Info("deleting affinity groups for batch tables", zap.Strings("groupIDs", ids))
	return affinity.DeleteGroupsWithRetry(ctx, ids)
}

// BuildAffinityGroupDefinitionsForTest is exported for testing.
func BuildAffinityGroupDefinitionsForTest(
	codec tikv.Codec,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	return buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
}

// createRecoveredTablesAffinityGroups creates the affinity groups of the tables
// being recovered: dropping or truncating a table deleted its groups. Recovery
// rescues data that GC deletes soon, so a failure here does not cancel it. The
// tables are recovered without some of their groups, and the job returns a
// warning that names them, so the groups can be created again with ALTER TABLE
// ... AFFINITY. It runs after the checks that can cancel the recovery step, and
// creating groups is idempotent, so retrying the step is safe. If a retry is
// cancelled after an earlier attempt created groups, the groups are kept: the
// same table ID may be recovered by another job, whose groups they then are.
// A later recovery or ALTER TABLE ... AFFINITY of the table takes them over.
func createRecoveredTablesAffinityGroups(jobCtx *jobContext, job *model.Job, recoverInfos []*model.RecoverTableInfo) {
	var (
		failedTables []string
		firstErr     error
	)
	for _, recoverInfo := range recoverInfos {
		if recoverInfo.TableInfo.Affinity == nil {
			continue
		}
		var err error
		failpoint.InjectCall("beforeCreateRecoveredTableAffinityGroups", recoverInfo.TableInfo, &err)
		if err == nil {
			err = createTableAffinityGroupsInPD(jobCtx, recoverInfo.TableInfo)
		}
		if err == nil {
			// If the job is cancelled later, the groups are kept; the log tells
			// which job requested them.
			logutil.DDLLogger().Info("create affinity groups request of a recovered table succeeded",
				zap.Int64("jobID", job.ID), zap.Int64("tableID", recoverInfo.TableInfo.ID))
			continue
		}
		logutil.DDLLogger().Error("failed to create affinity groups of a recovered table",
			zap.Int64("jobID", job.ID), zap.Int64("tableID", recoverInfo.TableInfo.ID), zap.Error(err))
		failedTables = append(failedTables, recoverInfo.TableInfo.Name.O)
		if firstErr == nil {
			firstErr = err
		}
	}
	if len(failedTables) == 0 {
		// Drop the warning of an earlier attempt of this step.
		if job.ReorgMeta != nil {
			id := dbterror.ClassDDL.Synthesize(terror.CodeUnknown, "").ID()
			delete(job.ReorgMeta.Warnings, id)
			delete(job.ReorgMeta.WarningsCount, id)
		}
		return
	}
	warn := dbterror.ClassDDL.Synthesize(terror.CodeUnknown, fmt.Sprintf(
		"failed to create affinity groups for recovered tables %s in job %d, run ALTER TABLE ... AFFINITY to create them again: %v",
		strings.Join(failedTables, ", "), job.ID, firstErr))
	if job.ReorgMeta == nil {
		job.ReorgMeta = &model.DDLReorgMeta{}
	}
	job.SetWarnings(map[errors.ErrorID]*terror.Error{warn.ID(): warn}, map[errors.ErrorID]int64{warn.ID(): 1})
}
