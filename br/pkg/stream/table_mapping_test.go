// Copyright 2024 PingCAP, Inc.
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

package stream

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestToProto(t *testing.T) {
	var (
		dbName, tblName            string       = "db1", "t1"
		oldDBID                    UpstreamID   = 100
		newDBID                    DownstreamID = 200
		oldTblID, oldPID1, oldPID2 UpstreamID   = 101, 102, 103
		newTblID, newPID1, newPID2 DownstreamID = 201, 202, 203
	)

	// create table Replace
	tr := NewTableReplace(tblName, newTblID)
	tr.PartitionMap[oldPID1] = newPID1
	tr.PartitionMap[oldPID2] = newPID2
	tr.FilteredOut = true

	dr := NewDBReplace(dbName, newDBID)
	dr.TableMap[oldTblID] = tr
	dr.FilteredOut = true

	drs := make(map[UpstreamID]*DBReplace)
	drs[oldDBID] = dr

	// create schemas replace and test ToProto().
	tm := NewTableMappingManager()
	err := tm.FromDBReplaceMap(drs)
	require.NoError(t, err)

	dbMap := tm.ToProto()
	require.Equal(t, len(dbMap), 1)
	require.Equal(t, dbMap[0].Name, dbName)
	require.Equal(t, dbMap[0].IdMap.UpstreamId, oldDBID)
	require.Equal(t, dbMap[0].IdMap.DownstreamId, newDBID)
	require.Equal(t, dbMap[0].FilteredOut, true)

	tableMap := dbMap[0].Tables
	require.Equal(t, len(tableMap), 1)
	require.Equal(t, tableMap[0].Name, tblName)
	require.Equal(t, tableMap[0].IdMap.UpstreamId, oldTblID)
	require.Equal(t, tableMap[0].IdMap.DownstreamId, newTblID)
	require.Equal(t, tableMap[0].FilteredOut, true)

	partitionMap := tableMap[0].Partitions
	require.Equal(t, len(partitionMap), 2)

	if partitionMap[0].UpstreamId == oldPID1 {
		require.Equal(t, partitionMap[0].DownstreamId, newPID1)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID2)
		require.Equal(t, partitionMap[1].DownstreamId, newPID2)
	} else {
		require.Equal(t, partitionMap[0].DownstreamId, newPID2)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID1)
		require.Equal(t, partitionMap[1].DownstreamId, newPID1)
	}

	// test FromDBMapProto()
	drs2 := FromDBMapProto(dbMap)
	require.Equal(t, drs2, drs)
}

func TestMergeBaseDBReplace(t *testing.T) {
	tests := []struct {
		name     string
		existing map[UpstreamID]*DBReplace
		base     map[UpstreamID]*DBReplace
		expected map[UpstreamID]*DBReplace
	}{
		{
			name:     "merge into empty existing map",
			existing: map[UpstreamID]*DBReplace{},
			base: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
			},
		},
		{
			name: "merge empty base map",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -10, Name: "table1"},
					},
				},
			},
			base: map[UpstreamID]*DBReplace{},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -10, Name: "table1"},
					},
				},
			},
		},
		{
			name: "merge new database with partitions",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -10,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -100,
							},
						},
					},
				},
			},
			base: map[UpstreamID]*DBReplace{
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {
							TableID: 2020,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								200: 2200,
							},
						},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -10,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -100,
							},
						},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {
							TableID: 2020,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								200: 2200,
							},
						},
					},
				},
			},
		},
		{
			name: "merge existing database with multiple tables",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -10, Name: "table1"},
						11: {TableID: -11, Name: "table2"},
					},
				},
			},
			base: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2"},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2"},
					},
				},
			},
		},
		{
			name: "merge with complex partition updates",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -10,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -100,
								101: -101,
							},
						},
					},
				},
			},
			base: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: 1101,
								102: 1102, // new partition
							},
						},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: 1101,
								102: 1102,
							},
						},
					},
				},
			},
		},
		{
			name: "merge multiple databases with mixed states",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -10, Name: "table1"},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table2"},
					},
				},
			},
			base: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
				3: {
					Name: "db3",
					DbID: 3000,
					TableMap: map[UpstreamID]*TableReplace{
						30: {TableID: 3030, Name: "table3"},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table2"},
					},
				},
				3: {
					Name: "db3",
					DbID: 3000,
					TableMap: map[UpstreamID]*TableReplace{
						30: {TableID: 3030, Name: "table3"},
					},
				},
			},
		},
		{
			name: "merge with filtered fields",
			existing: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -10,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -100,
							},
							FilteredOut: true,
						},
					},
					FilteredOut: true,
				},
			},
			base: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
							},
							FilteredOut: true,
						},
					},
					FilteredOut: true,
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
							},
							FilteredOut: true,
						},
					},
					FilteredOut: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := NewTableMappingManager()
			tm.DBReplaceMap = tt.existing
			tm.MergeBaseDBReplace(tt.base)
			require.Equal(t, tt.expected, tm.DBReplaceMap)

			// Additional verification for deep equality of nested structures
			for dbID, dbReplace := range tt.expected {
				require.Contains(t, tm.DBReplaceMap, dbID)
				require.Equal(t, dbReplace.Name, tm.DBReplaceMap[dbID].Name)
				require.Equal(t, dbReplace.DbID, tm.DBReplaceMap[dbID].DbID)

				for tblID, tblReplace := range dbReplace.TableMap {
					require.Contains(t, tm.DBReplaceMap[dbID].TableMap, tblID)
					require.Equal(t, tblReplace.Name, tm.DBReplaceMap[dbID].TableMap[tblID].Name)
					require.Equal(t, tblReplace.TableID, tm.DBReplaceMap[dbID].TableMap[tblID].TableID)
					require.Equal(t, tblReplace.PartitionMap, tm.DBReplaceMap[dbID].TableMap[tblID].PartitionMap)
				}
			}
		})
	}
}

func TestFilterDBReplaceMap(t *testing.T) {
	tests := []struct {
		name     string
		initial  map[UpstreamID]*DBReplace
		filter   *utils.PiTRIdTracker
		expected map[UpstreamID]*DBReplace
	}{
		{
			name: "empty filter marks all as filtered out",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
			},
			filter: &utils.PiTRIdTracker{
				DBIdToTableId: map[int64]map[int64]struct{}{},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1", FilteredOut: true},
					},
					FilteredOut: true,
				},
			},
		},
		{
			name: "filter specific database",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table2"},
					},
				},
			},
			filter: &utils.PiTRIdTracker{
				DBIdToTableId: map[int64]map[int64]struct{}{
					1: {10: struct{}{}},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table2", FilteredOut: true},
					},
					FilteredOut: true,
				},
			},
		},
		{
			name: "filter specific tables within database",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2"},
						12: {TableID: 1012, Name: "table3"},
					},
				},
			},
			filter: &utils.PiTRIdTracker{
				DBIdToTableId: map[int64]map[int64]struct{}{
					1: {
						10: struct{}{},
						12: struct{}{},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2", FilteredOut: true},
						12: {TableID: 1012, Name: "table3"},
					},
				},
			},
		},
		{
			name: "filter tables with partitions",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: 1101,
							},
						},
						11: {
							TableID: 1011,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: 1102,
								103: 1103,
							},
						},
					},
				},
			},
			filter: &utils.PiTRIdTracker{
				DBIdToTableId: map[int64]map[int64]struct{}{
					1: {10: struct{}{}},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: 1101,
							},
						},
						11: {
							TableID: 1011,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: 1102,
								103: 1103,
							},
							FilteredOut: true,
						},
					},
				},
			},
		},
		{
			name: "filter with multiple databases and tables",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2"},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table3"},
						21: {TableID: 2021, Name: "table4"},
					},
				},
				3: {
					Name: "db3",
					DbID: 3000,
					TableMap: map[UpstreamID]*TableReplace{
						30: {TableID: 3030, Name: "table5"},
					},
				},
			},
			filter: &utils.PiTRIdTracker{
				DBIdToTableId: map[int64]map[int64]struct{}{
					1: {10: struct{}{}},
					2: {
						20: struct{}{},
						21: struct{}{},
					},
				},
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
						11: {TableID: 1011, Name: "table2", FilteredOut: true},
					},
				},
				2: {
					Name: "db2",
					DbID: 2000,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2020, Name: "table3"},
						21: {TableID: 2021, Name: "table4"},
					},
				},
				3: {
					Name: "db3",
					DbID: 3000,
					TableMap: map[UpstreamID]*TableReplace{
						30: {TableID: 3030, Name: "table5", FilteredOut: true},
					},
					FilteredOut: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := NewTableMappingManager()
			tm.DBReplaceMap = tt.initial

			tm.ApplyFilterToDBReplaceMap(tt.filter)

			// verify DBReplaceMap is as expected
			require.Equal(t, tt.expected, tm.DBReplaceMap)

			// Additional verification for FilteredOut flags
			for dbID, dbReplace := range tt.expected {
				require.Contains(t, tm.DBReplaceMap, dbID)
				require.Equal(t, dbReplace.FilteredOut, tm.DBReplaceMap[dbID].FilteredOut)

				for tblID, tblReplace := range dbReplace.TableMap {
					require.Contains(t, tm.DBReplaceMap[dbID].TableMap, tblID)
					require.Equal(t, tblReplace.FilteredOut, tm.DBReplaceMap[dbID].TableMap[tblID].FilteredOut)
				}
			}
		})
	}
}

func TestReplaceTemporaryIDs(t *testing.T) {
	tests := []struct {
		name         string
		initial      map[UpstreamID]*DBReplace
		tempCounter  DownstreamID
		genGlobalIDs func(context.Context, int) ([]int64, error)
		expected     map[UpstreamID]*DBReplace
		expectedErr  error
	}{
		{
			name: "no temporary IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
			},
			tempCounter: InitialTempId,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				return nil, nil
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: 1010, Name: "table1"},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "replace all temporary IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -2,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -3,
							},
						},
					},
				},
			},
			tempCounter: -3,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				return []int64{1000, 1010, 1020}, nil
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1020,
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "mixed temporary and global IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -1,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: -2,
							},
						},
					},
				},
				2: {
					Name: "db2",
					DbID: -3,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2000, Name: "table2"},
					},
				},
			},
			tempCounter: -3,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				return []int64{2010, 2020, 2030}, nil
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 2010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1100,
								101: 2020,
							},
						},
					},
				},
				2: {
					Name: "db2",
					DbID: 2030,
					TableMap: map[UpstreamID]*TableReplace{
						20: {TableID: 2000, Name: "table2"},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "error generating global IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -2, Name: "table1"},
					},
				},
			},
			tempCounter: -2,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				return nil, errors.New("failed to generate global IDs")
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {TableID: -2, Name: "table1"},
					},
				},
			},
			expectedErr: errors.New("failed to generate global IDs"),
		},
		{
			name: "complex structure with multiple temporary IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -1,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -2,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -3,
								101: -4,
							},
						},
						11: {
							TableID: -5,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: -6,
							},
						},
					},
				},
				2: {
					Name: "db2",
					DbID: -7,
					TableMap: map[UpstreamID]*TableReplace{
						20: {
							TableID: -8,
							Name:    "table3",
						},
					},
				},
			},
			tempCounter: -8,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				ids := make([]int64, n)
				for i := 0; i < n; i++ {
					ids[i] = int64(1000 + i*10)
				}
				return ids, nil
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 1000,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 1010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 1020,
								101: 1030,
							},
						},
						11: {
							TableID: 1040,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: 1050,
							},
						},
					},
				},
				2: {
					Name: "db2",
					DbID: 1060,
					TableMap: map[UpstreamID]*TableReplace{
						20: {
							TableID: 1070,
							Name:    "table3",
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "non-consecutive temporary IDs",
			initial: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: -5,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: -2,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: -8,
								101: -1,
							},
						},
						11: {
							TableID: -15,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: -3,
							},
						},
					},
				},
			},
			tempCounter: -15,
			genGlobalIDs: func(ctx context.Context, n int) ([]int64, error) {
				ids := make([]int64, n)
				for i := 0; i < n; i++ {
					ids[i] = int64(2000 + i*10)
				}
				return ids, nil
			},
			expected: map[UpstreamID]*DBReplace{
				1: {
					Name: "db1",
					DbID: 2030,
					TableMap: map[UpstreamID]*TableReplace{
						10: {
							TableID: 2010,
							Name:    "table1",
							PartitionMap: map[UpstreamID]DownstreamID{
								100: 2040,
								101: 2000,
							},
						},
						11: {
							TableID: 2050,
							Name:    "table2",
							PartitionMap: map[UpstreamID]DownstreamID{
								102: 2020,
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := NewTableMappingManager()
			tm.DBReplaceMap = tt.initial
			tm.tempIDCounter = tt.tempCounter

			err := tm.ReplaceTemporaryIDs(context.Background(), tt.genGlobalIDs)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr.Error())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, tm.DBReplaceMap)
			require.Equal(t, InitialTempId, tm.tempIDCounter)
		})
	}
}

func TestParseMetaKvAndUpdateIdMapping(t *testing.T) {
	var (
		dbID      int64  = 40
		dbName           = "test_db"
		tableID   int64  = 100
		tableName        = "test_table"
		pt1ID     int64  = 101
		pt2ID     int64  = 102
		pt1Name          = "pt1"
		pt2Name          = "pt2"
		ts        uint64 = 400036290571534337
	)

	tc := NewTableMappingManager()

	// Test DB key
	dbKey := meta.DBkey(dbID)
	dbInfo := &model.DBInfo{
		ID:   dbID,
		Name: pmodel.NewCIStr(dbName),
	}
	dbValue, err := json.Marshal(dbInfo)
	require.NoError(t, err)

	// Encode DB key in a transaction
	txnDBKey := utils.EncodeTxnMetaKey([]byte("DBs"), dbKey, ts)
	entry := &kv.Entry{
		Key:   txnDBKey,
		Value: dbValue,
	}

	// Test parsing DB key and value
	err = tc.ParseMetaKvAndUpdateIdMapping(entry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap, dbID)
	require.Equal(t, dbName, tc.DBReplaceMap[dbID].Name)

	// Test table key
	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions,
		model.PartitionDefinition{
			ID:   pt1ID,
			Name: pmodel.NewCIStr(pt1Name),
		},
		model.PartitionDefinition{
			ID:   pt2ID,
			Name: pmodel.NewCIStr(pt2Name),
		},
	)

	tableInfo := &model.TableInfo{
		ID:        tableID,
		Name:      pmodel.NewCIStr(tableName),
		Partition: &pi,
	}
	tableValue, err := json.Marshal(tableInfo)
	require.NoError(t, err)

	// Encode table key in a transaction
	txnTableKey := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)
	tableEntry := &kv.Entry{
		Key:   txnTableKey,
		Value: tableValue,
	}

	// Test parsing table key and value
	err = tc.ParseMetaKvAndUpdateIdMapping(tableEntry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap[dbID].TableMap, tableID)
	require.Equal(t, tableName, tc.DBReplaceMap[dbID].TableMap[tableID].Name)

	// Verify partition IDs are mapped
	require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt1ID)
	require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt2ID)

	// Test non-meta key
	nonMetaEntry := &kv.Entry{
		Key:   []byte("not_a_meta_key"),
		Value: []byte("some_value"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(nonMetaEntry, consts.DefaultCF)
	require.NoError(t, err)

	// Test auto increment key with different IDs
	autoIncrDBID := int64(50)
	autoIncrTableID := int64(200)
	autoIncrKey := utils.EncodeTxnMetaKey(meta.DBkey(autoIncrDBID), meta.AutoIncrementIDKey(autoIncrTableID), ts)
	autoIncrEntry := &kv.Entry{
		Key:   autoIncrKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoIncrEntry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap, autoIncrDBID)
	require.Contains(t, tc.DBReplaceMap[autoIncrDBID].TableMap, autoIncrTableID)

	// Test auto table ID key with different IDs
	autoTableDBID := int64(60)
	autoTableTableID := int64(300)
	autoTableKey := utils.EncodeTxnMetaKey(meta.DBkey(autoTableDBID), meta.AutoTableIDKey(autoTableTableID), ts)
	autoTableEntry := &kv.Entry{
		Key:   autoTableKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoTableEntry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap, autoTableDBID)
	require.Contains(t, tc.DBReplaceMap[autoTableDBID].TableMap, autoTableTableID)

	// Test sequence key with different IDs
	seqDBID := int64(70)
	seqTableID := int64(400)
	seqKey := utils.EncodeTxnMetaKey(meta.DBkey(seqDBID), meta.SequenceKey(seqTableID), ts)
	seqEntry := &kv.Entry{
		Key:   seqKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(seqEntry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap, seqDBID)
	require.Contains(t, tc.DBReplaceMap[seqDBID].TableMap, seqTableID)

	// Test auto random table ID key with different IDs
	autoRandomDBID := int64(80)
	autoRandomTableID := int64(500)
	autoRandomKey := utils.EncodeTxnMetaKey(meta.DBkey(autoRandomDBID), meta.AutoRandomTableIDKey(autoRandomTableID), ts)
	autoRandomEntry := &kv.Entry{
		Key:   autoRandomKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoRandomEntry, consts.DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DBReplaceMap, autoRandomDBID)
	require.Contains(t, tc.DBReplaceMap[autoRandomDBID].TableMap, autoRandomTableID)
}
