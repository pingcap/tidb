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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	affinityCleanupTimeout = 10 * time.Second

	// Bound OldAffinityGroupIDs persisted in a single DDL job row so large
	// operations such as DROP DATABASE cannot exceed kv.TxnEntrySizeLimit
	// (kv:8025). Any overflow groups are cleaned up by affinityReconciler once
	// GC removes their mysql.gc_delete_range rows.
	maxPersistedAffinityGroupIDs   = 1024
	maxPersistedAffinityGroupBytes = 32 * 1024
	ddlJobRowBaseOverheadBytes     = 1024
	ddlJobEntrySafetyMarginBytes   = 1024
	ddlJobAffinityJSONEnvelope     = 64
)

// affinityPDGate serializes PD affinity group mutations on the DDL owner
// between affinity-changing DDL execution and the background affinityReconciler.
type affinityPDGate struct {
	sem chan struct{}
}

func newAffinityPDGate() *affinityPDGate {
	return &affinityPDGate{sem: make(chan struct{}, 1)}
}

func (g *affinityPDGate) lock(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx == nil {
		g.sem <- struct{}{}
		return nil
	}
	select {
	case g.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *affinityPDGate) unlock() {
	if g == nil {
		return
	}
	<-g.sem
}

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

// ParseAffinityGroupID strictly parses an affinity group ID belonging to the
// namespace of codec (Classic or the current NextGen keyspace).
// It rejects IDs from other keyspaces, Classic IDs under NextGen, NextGen IDs
// under Classic, non-_tidb_ IDs, non-positive IDs, leading zeros, and trailing data.
func ParseAffinityGroupID(codec tikv.Codec, groupID string) (tableID, partitionID int64, isPartition, ok bool) {
	prefix := "_tidb_"
	if label.UseKeyspaceAwareRules(codec) {
		prefix = fmt.Sprintf("_tidb_ks%d_", codec.GetKeyspaceID())
	}
	rest, hasPrefix := strings.CutPrefix(groupID, prefix)
	if !hasPrefix {
		return 0, 0, false, false
	}

	if rawTableID, isTable := strings.CutPrefix(rest, "t_"); isTable {
		tid, err := strconv.ParseInt(rawTableID, 10, 64)
		if err != nil || tid <= 0 || GetTableAffinityGroupID(codec, tid) != groupID {
			return 0, 0, false, false
		}
		return tid, 0, false, true
	}

	if rawPart, isPart := strings.CutPrefix(rest, "pt_"); isPart {
		rawTableID, rawPartID, found := strings.Cut(rawPart, "_p")
		if !found {
			return 0, 0, false, false
		}
		tid, err1 := strconv.ParseInt(rawTableID, 10, 64)
		pid, err2 := strconv.ParseInt(rawPartID, 10, 64)
		if err1 != nil || err2 != nil || tid <= 0 || pid <= 0 || GetPartitionAffinityGroupID(codec, tid, pid) != groupID {
			return 0, 0, false, false
		}
		return tid, pid, true, true
	}

	return 0, 0, false, false
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
// With no partition definitions, it uses all partitions of the table.
// Callers that intend to create groups for only selected partitions must skip
// this call if their selection is empty. Callers must handle creation errors
// according to whether the DDL can still be rolled back.
// Used by: CREATE TABLE, ALTER TABLE AFFINITY = 'xxx', TRUNCATE TABLE, TRUNCATE PARTITION.
func createTableAffinityGroupsInPD(jobCtx *jobContext, tblInfo *model.TableInfo, partitionDefs ...model.PartitionDefinition) error {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil
	}

	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = jobCtx.ctx
	}
	codec := jobCtx.store.GetCodec()

	if len(partitionDefs) == 0 {
		partitionDefs = nil
	}
	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
	if err != nil {
		return errors.Trace(err)
	}

	if len(groups) == 0 {
		return nil
	}

	failpoint.InjectCall("beforeAffinityDDLPDOp")
	if err := jobCtx.affinityPDGate.lock(ctx); err != nil {
		return errors.Trace(err)
	}
	defer jobCtx.affinityPDGate.unlock()

	logutil.DDLLogger().Info("creating affinity groups in PD",
		zap.Int64("tableID", tblInfo.ID),
		zap.Strings("groupIDs", collectAffinityGroupIDs(groups)))
	if err := affinity.CreateGroupsIfNotExists(ctx, groups); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// collectAffinityGroupIDsForTable returns a sorted slice of affinity group IDs
// for tblInfo (or nil if tblInfo has no affinity configured).
func collectAffinityGroupIDsForTable(codec tikv.Codec, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) ([]string, error) {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return nil, nil
	}
	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ids := collectAffinityGroupIDs(groups)
	slices.Sort(ids)
	return ids, nil
}

func deleteTableAffinityGroupsWithTimeout(jobCtx *jobContext, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) error {
	// Rollback and post-commit cleanup can run after the step context is canceled.
	ctx, cancel := context.WithTimeout(jobCtx.ctx, affinityCleanupTimeout)
	defer cancel()
	failpoint.InjectCall("beforeAffinityDDLPDOp")
	if err := jobCtx.affinityPDGate.lock(ctx); err != nil {
		return errors.Trace(err)
	}
	defer jobCtx.affinityPDGate.unlock()
	return deleteTableAffinityGroups(ctx, jobCtx.store.GetCodec(), tblInfo, partitionDefs)
}

func deleteTableAffinityGroups(ctx context.Context, codec tikv.Codec, tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition) error {
	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
	if err != nil {
		return errors.Trace(err)
	}
	return affinity.DeleteGroupsWithRetry(ctx, collectAffinityGroupIDs(groups))
}

// partitionDefinitionsFromIDs returns an explicit list even when ids is empty.
// A nil list would select all of a table's partitions for affinity operations.
func partitionDefinitionsFromIDs(ids []int64) []model.PartitionDefinition {
	definitions := make([]model.PartitionDefinition, 0, len(ids))
	for _, id := range ids {
		definitions = append(definitions, model.PartitionDefinition{ID: id})
	}
	return definitions
}

// collectTableAffinityGroupIDsCapped adds the affinity group IDs of a table to
// groupIDs up to limit entries. It returns truncated=true when any group ID was
// omitted due to limit.
func collectTableAffinityGroupIDsCapped(
	codec tikv.Codec,
	tblInfo *model.TableInfo,
	groupIDs map[string]struct{},
	limit int,
) (bool, error) {
	if tblInfo == nil || tblInfo.Affinity == nil {
		return false, nil
	}
	if limit > 0 && len(groupIDs) >= limit {
		return true, nil
	}
	groups, err := buildAffinityGroupDefinitions(codec, tblInfo, nil)
	if err != nil {
		return false, errors.Trace(err)
	}
	if limit <= 0 || len(groupIDs)+len(groups) <= limit {
		for id := range groups {
			groupIDs[id] = struct{}{}
		}
		return false, nil
	}
	ids := collectAffinityGroupIDs(groups)
	slices.Sort(ids)
	truncated := false
	for _, id := range ids {
		if _, exists := groupIDs[id]; exists {
			continue
		}
		if len(groupIDs) >= limit {
			truncated = true
			break
		}
		groupIDs[id] = struct{}{}
	}
	return truncated, nil
}

func effectiveTxnEntrySizeLimit() uint64 {
	limit := kv.TxnEntrySizeLimit.Load()
	if varLimit := vardef.TxnEntrySizeLimit.Load(); varLimit > 0 && (limit == 0 || varLimit < limit) {
		limit = varLimit
	}
	return limit
}

func truncateAffinityGroupIDs(groupIDs []string, maxCount, maxBytes int) []string {
	if len(groupIDs) == 0 || maxCount <= 0 || maxBytes <= 0 {
		return nil
	}
	usedBytes := 0
	keep := 0
	for _, id := range groupIDs {
		if keep >= maxCount {
			break
		}
		// Each ASCII group ID encodes in JSON as `"id",` (len(id) + 3 bytes).
		elemBytes := len(id) + 3
		if usedBytes+elemBytes > maxBytes {
			break
		}
		usedBytes += elemBytes
		keep++
	}
	if keep == 0 {
		return nil
	}
	return groupIDs[:keep]
}

// boundOldAffinityGroupIDsForJob returns a prefix of groupIDs that fits safely
// within both the static cap and the remaining KV entry-size headroom for job.
// Callers must pass job after populating its finished/base args with
// OldAffinityGroupIDs left empty so the base encoded size is measured accurately.
func boundOldAffinityGroupIDsForJob(job *model.Job, groupIDs []string) []string {
	if len(groupIDs) == 0 {
		return nil
	}
	entryLimit := effectiveTxnEntrySizeLimit()
	if job == nil || entryLimit == 0 {
		return truncateAffinityGroupIDs(groupIDs, maxPersistedAffinityGroupIDs, maxPersistedAffinityGroupBytes)
	}

	origRawArgs := job.RawArgs
	baseBytes, err := job.Encode(true)
	job.RawArgs = origRawArgs
	if err != nil {
		return truncateAffinityGroupIDs(groupIDs, maxPersistedAffinityGroupIDs, maxPersistedAffinityGroupBytes)
	}
	rowOverhead := uint64(ddlJobRowBaseOverheadBytes + ddlJobAffinityJSONEnvelope + len(job.SchemaName) + len(job.TableName))
	used := uint64(len(baseBytes)) + rowOverhead
	if entryLimit <= used+ddlJobEntrySafetyMarginBytes {
		return nil
	}
	budget := min(entryLimit-used-ddlJobEntrySafetyMarginBytes, uint64(maxPersistedAffinityGroupBytes))
	return truncateAffinityGroupIDs(groupIDs, maxPersistedAffinityGroupIDs, int(budget))
}

func sortedAffinityGroupIDs(groupIDs map[string]struct{}) []string {
	if len(groupIDs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(groupIDs))
	for id := range groupIDs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// BuildAffinityGroupDefinitionsForTest is exported for testing.
func BuildAffinityGroupDefinitionsForTest(
	codec tikv.Codec,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
) (map[string][]pdhttp.AffinityGroupKeyRange, error) {
	return buildAffinityGroupDefinitions(codec, tblInfo, partitionDefs)
}

// BoundOldAffinityGroupIDsForJobForTest is exported for testing.
func BoundOldAffinityGroupIDsForJobForTest(job *model.Job, groupIDs []string) []string {
	return boundOldAffinityGroupIDsForJob(job, groupIDs)
}
