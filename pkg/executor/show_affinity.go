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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/collate"
)

func (e *ShowExec) fetchShowAffinity(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	is := do.InfoSchema()

	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}

	// Get all tables with affinity attribute from infoschema
	tableInfoResults := is.ListTablesWithSpecialAttribute(infoschemacontext.AffinityAttribute)

	// Collect table/partition info with affinity
	type tablePartitionInfo struct {
		dbName        string
		tableName     string
		partitionName any    // nil for table-level, string for partition-level
		groupID       string // Still needed to query PD for affinity state
	}
	infos := make([]tablePartitionInfo, 0, len(tableInfoResults))

	for _, result := range tableInfoResults {
		dbName := result.DBName.O

		for _, tblInfo := range result.TableInfos {
			if tblInfo.Affinity == nil {
				continue
			}

			// Apply field filter for table name
			if fieldFilter != "" && tblInfo.Name.L != fieldFilter {
				continue
			} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(tblInfo.Name.L) {
				continue
			}

			switch tblInfo.Affinity.Level {
			case ast.TableAffinityLevelTable:
				groupID := ddl.GetTableAffinityGroupID(tblInfo.ID)
				infos = append(infos, tablePartitionInfo{
					dbName:        dbName,
					tableName:     tblInfo.Name.O,
					partitionName: nil, // nil for table-level affinity (no partition)
					groupID:       groupID,
				})

			case ast.TableAffinityLevelPartition:
				if tblInfo.Partition != nil {
					for _, def := range tblInfo.Partition.Definitions {
						groupID := ddl.GetPartitionAffinityGroupID(tblInfo.ID, def.ID)
						infos = append(infos, tablePartitionInfo{
							dbName:        dbName,
							tableName:     tblInfo.Name.O,
							partitionName: def.Name.O,
							groupID:       groupID,
						})
					}
				}
			}
		}
	}

	// Get affinity group states from PD
	// Use GetAllAffinityGroupStates to directly query PD client (similar to GetReplicationState)
	// This bypasses the manager layer to allow test mocking with SetPDHttpCliForTest
	// TODO: update it after pd support batch get
	allAffinityStates, err := infosync.GetAllAffinityGroupStates(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// Build result rows
	for _, info := range infos {
		state, exists := allAffinityStates[info.groupID]

		var (
			leaderStoreID       any = nil
			voterStoreIDs       any = nil
			status              any = nil
			regionCount         any = nil
			affinityRegionCount any = nil
		)

		if exists && state != nil {
			if state.LeaderStoreID != 0 {
				leaderStoreID = state.LeaderStoreID
			}

			// Convert voter store IDs to comma-separated string
			if len(state.VoterStoreIDs) > 0 {
				storeIDStrs := make([]string, len(state.VoterStoreIDs))
				for i, id := range state.VoterStoreIDs {
					storeIDStrs[i] = fmt.Sprintf("%d", id)
				}
				voterStoreIDs = strings.Join(storeIDStrs, ",")
			}

			// Map phase to status
			switch state.Phase {
			case "pending":
				status = "Pending"
			case "preparing":
				status = "Preparing"
			case "stable":
				status = "Stable"
			default:
				status = state.Phase
			}

			regionCount = state.RegionCount
			affinityRegionCount = state.AffinityRegionCount
		}
		// If group doesn't exist in PD, all fields remain as nil (native NULL)
		e.appendRow([]any{
			info.dbName,
			info.tableName,
			info.partitionName,
			leaderStoreID,
			voterStoreIDs,
			status,
			regionCount,
			affinityRegionCount,
		})
	}

	return nil
}
