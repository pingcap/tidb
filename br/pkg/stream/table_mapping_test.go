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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

// MockMetaInfoCollector implements the MetaInfoCollector interface for testing
type MockMetaInfoCollector struct {
	dbInfos         map[int64]*model.DBInfo
	dbTimestamps    map[int64]uint64
	tableInfos      map[int64]map[int64]*model.TableInfo
	tableTimestamps map[int64]map[int64]uint64
}

func NewMockMetaInfoCollector() *MockMetaInfoCollector {
	return &MockMetaInfoCollector{
		dbInfos:         make(map[int64]*model.DBInfo),
		dbTimestamps:    make(map[int64]uint64),
		tableInfos:      make(map[int64]map[int64]*model.TableInfo),
		tableTimestamps: make(map[int64]map[int64]uint64),
	}
}

func (m *MockMetaInfoCollector) OnDatabaseInfo(dbId int64, dbName string, commitTs uint64) {
	// only update if this is a newer timestamp
	if existingTs, exists := m.dbTimestamps[dbId]; !exists || commitTs > existingTs {
		dbInfo := &model.DBInfo{
			ID:   dbId,
			Name: ast.NewCIStr(dbName),
		}
		m.dbInfos[dbInfo.ID] = dbInfo
		m.dbTimestamps[dbId] = commitTs
	}
}

func (m *MockMetaInfoCollector) OnTableInfo(dbID, tableId int64, tableSimpleInfo *tableSimpleInfo, commitTs uint64) {
	if _, ok := m.tableInfos[dbID]; !ok {
		m.tableInfos[dbID] = make(map[int64]*model.TableInfo)
	}
	if _, ok := m.tableTimestamps[dbID]; !ok {
		m.tableTimestamps[dbID] = make(map[int64]uint64)
	}

	// only update if this is a newer timestamp
	if existingTs, exists := m.tableTimestamps[dbID][tableId]; !exists || commitTs > existingTs {
		tableInfo := &model.TableInfo{
			ID:   tableId,
			Name: ast.NewCIStr(tableSimpleInfo.Name),
		}
		m.tableInfos[dbID][tableInfo.ID] = tableInfo
		m.tableTimestamps[dbID][tableId] = commitTs
	}
}

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
				DBIds:          map[int64]struct{}{},
				TableIdToDBIds: make(map[int64]map[int64]struct{}),
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
				DBIds: map[int64]struct{}{
					1: {},
				},
				TableIdToDBIds: map[int64]map[int64]struct{}{
					10: {1: {}},
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
				DBIds: map[int64]struct{}{
					1: {},
				},
				TableIdToDBIds: map[int64]map[int64]struct{}{
					10: {1: {}},
					12: {1: {}},
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
				DBIds: map[int64]struct{}{
					1: {},
				},
				TableIdToDBIds: map[int64]map[int64]struct{}{
					10: {1: {}},
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
				DBIds: map[int64]struct{}{
					1: {},
					2: {},
				},
				TableIdToDBIds: map[int64]map[int64]struct{}{
					10: {1: {}},
					20: {2: {}},
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
						21: {TableID: 2021, Name: "table4", FilteredOut: true},
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
				for i := range n {
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
				for i := range n {
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

	t.Run("DefaultCF and WriteCF flow", func(t *testing.T) {
		tc := NewTableMappingManager()
		collector := NewMockMetaInfoCollector()

		// Test DB key with DefaultCF
		dbKey := meta.DBkey(dbID)
		dbInfo := &model.DBInfo{
			ID:   dbID,
			Name: ast.NewCIStr(dbName),
		}
		dbValue, err := json.Marshal(dbInfo)
		require.NoError(t, err)

		// Encode DB key in a transaction for DefaultCF
		txnDBKey := utils.EncodeTxnMetaKey([]byte("DBs"), dbKey, ts)
		defaultCFEntry := &kv.Entry{
			Key:   txnDBKey,
			Value: dbValue,
		}

		// Test parsing DB key and value with DefaultCF
		err = tc.ParseMetaKvAndUpdateIdMapping(defaultCFEntry, consts.DefaultCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap, dbID)
		// With DefaultCF, the database name is stored in tempDefaultKVDbMap, not in DBReplace
		require.Equal(t, "", tc.DBReplaceMap[dbID].Name)
		// Collector is not called for DefaultCF
		require.NotContains(t, collector.dbInfos, dbID)

		// Now test with WriteCF - this should process the DefaultCF entry and call collector
		// Create a WriteCF value that references the DefaultCF entry by timestamp (no short value)
		writeCFData := []byte{WriteTypePut}                // Write type: Put
		writeCFData = codec.EncodeUvarint(writeCFData, ts) // Start timestamp (same as DefaultCF)
		writeCFEntry := &kv.Entry{
			Key:   txnDBKey,
			Value: writeCFData,
		}

		err = tc.ParseMetaKvAndUpdateIdMapping(writeCFEntry, consts.WriteCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap, dbID)
		// With WriteCF, the database name should now be set in DBReplace
		require.Equal(t, dbName, tc.DBReplaceMap[dbID].Name)
		// Collector should now be called
		require.Contains(t, collector.dbInfos, dbID)
		require.Equal(t, dbName, collector.dbInfos[dbID].Name.O)

		// Test write cf kvs are more than default cf kvs
		err = tc.ParseMetaKvAndUpdateIdMapping(writeCFEntry, consts.WriteCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap, dbID)
		// With WriteCF, the database name should now be set in DBReplace
		require.Equal(t, dbName, tc.DBReplaceMap[dbID].Name)
		// Collector should now be called
		require.Contains(t, collector.dbInfos, dbID)
		require.Equal(t, dbName, collector.dbInfos[dbID].Name.O)

		// Test table key with DefaultCF
		pi := model.PartitionInfo{
			Enable:      true,
			Definitions: make([]model.PartitionDefinition, 0),
		}
		pi.Definitions = append(pi.Definitions,
			model.PartitionDefinition{
				ID:   pt1ID,
				Name: ast.NewCIStr(pt1Name),
			},
			model.PartitionDefinition{
				ID:   pt2ID,
				Name: ast.NewCIStr(pt2Name),
			},
		)

		tableInfo := &model.TableInfo{
			ID:        tableID,
			Name:      ast.NewCIStr(tableName),
			Partition: &pi,
		}
		tableValue, err := json.Marshal(tableInfo)
		require.NoError(t, err)

		// Encode table key in a transaction for DefaultCF
		txnTableKey := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)
		tableDefaultCFEntry := &kv.Entry{
			Key:   txnTableKey,
			Value: tableValue,
		}

		// Test parsing table key and value with DefaultCF
		err = tc.ParseMetaKvAndUpdateIdMapping(tableDefaultCFEntry, consts.DefaultCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap, tableID)
		// With DefaultCF, the table name is stored in tempDefaultKVTableMap, not in TableReplace
		require.Equal(t, "", tc.DBReplaceMap[dbID].TableMap[tableID].Name)
		// Collector is not called for DefaultCF
		require.NotContains(t, collector.tableInfos, dbID)

		// Now test with WriteCF for table
		// Create a WriteCF value that references the DefaultCF entry by timestamp (no short value)
		tableWriteCFData := []byte{WriteTypePut}                     // Write type: Put
		tableWriteCFData = codec.EncodeUvarint(tableWriteCFData, ts) // Start timestamp (same as DefaultCF)
		tableWriteCFEntry := &kv.Entry{
			Key:   txnTableKey,
			Value: tableWriteCFData,
		}

		err = tc.ParseMetaKvAndUpdateIdMapping(tableWriteCFEntry, consts.WriteCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap, tableID)
		// With WriteCF, the table name should now be set in TableReplace
		require.Equal(t, tableName, tc.DBReplaceMap[dbID].TableMap[tableID].Name)
		// Collector should now be called
		require.Contains(t, collector.tableInfos, dbID)
		require.Contains(t, collector.tableInfos[dbID], tableID)
		require.Equal(t, tableName, collector.tableInfos[dbID][tableID].Name.O)

		// Verify partition IDs are mapped
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt1ID)
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt2ID)

		// Test write cf kvs are more than default cf kvs
		err = tc.ParseMetaKvAndUpdateIdMapping(tableWriteCFEntry, consts.WriteCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap, tableID)
		// With WriteCF, the table name should now be set in TableReplace
		require.Equal(t, tableName, tc.DBReplaceMap[dbID].TableMap[tableID].Name)
		// Collector should now be called
		require.Contains(t, collector.tableInfos, dbID)
		require.Contains(t, collector.tableInfos[dbID], tableID)
		require.Equal(t, tableName, collector.tableInfos[dbID][tableID].Name.O)

		// Verify partition IDs are mapped
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt1ID)
		require.Contains(t, tc.DBReplaceMap[dbID].TableMap[tableID].PartitionMap, pt2ID)
	})

	t.Run("Key-only entries", func(t *testing.T) {
		tc := NewTableMappingManager()
		collector := NewMockMetaInfoCollector()

		// Test non-meta key
		nonMetaEntry := &kv.Entry{
			Key:   []byte("not_a_meta_key"),
			Value: []byte("some_value"),
		}
		err := tc.ParseMetaKvAndUpdateIdMapping(nonMetaEntry, consts.DefaultCF, ts, collector)
		require.NoError(t, err)

		// Test auto increment key with different IDs
		autoIncrDBID := int64(50)
		autoIncrTableID := int64(200)
		autoIncrKey := utils.EncodeTxnMetaKey(meta.DBkey(autoIncrDBID), meta.AutoIncrementIDKey(autoIncrTableID), ts)
		autoIncrEntry := &kv.Entry{
			Key:   autoIncrKey,
			Value: []byte("1"),
		}
		err = tc.ParseMetaKvAndUpdateIdMapping(autoIncrEntry, consts.DefaultCF, ts, collector)
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
		err = tc.ParseMetaKvAndUpdateIdMapping(autoTableEntry, consts.DefaultCF, ts, collector)
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
		err = tc.ParseMetaKvAndUpdateIdMapping(seqEntry, consts.DefaultCF, ts, collector)
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
		err = tc.ParseMetaKvAndUpdateIdMapping(autoRandomEntry, consts.DefaultCF, ts, collector)
		require.NoError(t, err)
		require.Contains(t, tc.DBReplaceMap, autoRandomDBID)
		require.Contains(t, tc.DBReplaceMap[autoRandomDBID].TableMap, autoRandomTableID)
	})
}

func TestTableHistoryManagerOutOfOrderTS(t *testing.T) {
	const (
		dbID     int64 = 40
		tableID  int64 = 100
		partID   int64 = 101
		parentID int64 = 200
	)

	tests := []struct {
		name        string
		description string
		operations  []struct {
			ts        uint64
			operation string // "db", "table", "partition"
			name      string
			expected  bool // whether this should be the final state
		}
		expectedDBName    string
		expectedTableName string
	}{
		{
			name:        "database updates out of order",
			description: "database name updates processed out of chronological order",
			operations: []struct {
				ts        uint64
				operation string
				name      string
				expected  bool
			}{
				{ts: 100, operation: "db", name: "old_db", expected: false},
				{ts: 200, operation: "db", name: "new_db", expected: true},
				{ts: 50, operation: "db", name: "oldest_db", expected: false}, // should be ignored
			},
			expectedDBName: "new_db",
		},
		{
			name:        "table updates out of order",
			description: "table name updates processed out of chronological order",
			operations: []struct {
				ts        uint64
				operation string
				name      string
				expected  bool
			}{
				{ts: 100, operation: "db", name: "test_db", expected: true},
				{ts: 150, operation: "table", name: "old_table", expected: false},
				{ts: 200, operation: "table", name: "new_table", expected: true},
				{ts: 120, operation: "table", name: "intermediate_table", expected: false}, // should be ignored
			},
			expectedDBName:    "test_db",
			expectedTableName: "new_table",
		},
		{
			name:        "partition updates out of order",
			description: "partition name updates processed out of chronological order",
			operations: []struct {
				ts        uint64
				operation string
				name      string
				expected  bool
			}{
				{ts: 100, operation: "db", name: "test_db", expected: true},
				{ts: 150, operation: "partition", name: "old_partition", expected: false},
				{ts: 200, operation: "partition", name: "new_partition", expected: true},
				{ts: 120, operation: "partition", name: "intermediate_partition", expected: false}, // should be ignored
			},
			expectedDBName: "test_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewTableHistoryManager()

			// process operations in the specified order
			for _, op := range tt.operations {
				switch op.operation {
				case "db":
					manager.RecordDBIdToName(dbID, op.name, op.ts)
				case "table":
					manager.AddTableHistory(tableID, op.name, dbID, op.ts)
				case "partition":
					manager.AddPartitionHistory(partID, op.name, dbID, parentID, op.ts)
				default:
					t.Fatalf("unknown operation type: %s", op.operation)
				}
			}

			// verify final state
			if tt.expectedDBName != "" {
				dbName, exists := manager.GetDBNameByID(dbID)
				require.True(t, exists, "database should exist in history")
				require.Equal(t, tt.expectedDBName, dbName,
					"database name should match the entry with latest timestamp")
			}

			if tt.expectedTableName != "" {
				history := manager.GetTableHistory()
				require.Contains(t, history, tableID, "table should exist in history")
				require.Equal(t, tt.expectedTableName, history[tableID][1].TableName,
					"table name should match the entry with latest timestamp")
			}

			// verify partition history if applicable
			if len(tt.operations) > 0 && tt.operations[len(tt.operations)-1].operation == "partition" {
				history := manager.GetTableHistory()
				require.Contains(t, history, partID, "partition should exist in history")

				// find the expected partition name from the latest timestamp
				var expectedPartitionName string
				var latestTS uint64
				for _, op := range tt.operations {
					if op.operation == "partition" && op.ts >= latestTS {
						latestTS = op.ts
						expectedPartitionName = op.name
					}
				}

				require.Equal(t, expectedPartitionName, history[partID][1].TableName,
					"partition name should match the entry with latest timestamp")
			}
		})
	}
}

func TestReportError(t *testing.T) {
	manager := NewTableMappingManager()
	manager.noDefaultKVErrorMap[1] = errors.New("test")
	require.Error(t, manager.ReportIfError())
	manager.CleanError(2)
	require.Error(t, manager.ReportIfError())
	manager.CleanError(1)
	require.NoError(t, manager.ReportIfError())
}

// createMockInfoSchemaWithDBs creates a mock InfoSchema with specified databases
// This helper function creates an InfoSchema with multiple databases for testing
func createMockInfoSchemaWithDBs(dbNameToID map[string]int64) infoschema.InfoSchema {
	// Create DBInfos from the map
	dbInfos := make([]*model.DBInfo, 0, len(dbNameToID))
	for name, id := range dbNameToID {
		// Create a dummy table for this database to make MockInfoSchema work
		tableInfo := &model.TableInfo{
			ID:   id*1000 + 1, // Use a unique table ID
			Name: ast.NewCIStr("dummy_table"),
			Columns: []*model.ColumnInfo{
				{
					ID:     1,
					Name:   ast.NewCIStr("id"),
					Offset: 0,
					State:  model.StatePublic,
				},
			},
			State: model.StatePublic,
		}
		
		dbInfo := &model.DBInfo{
			ID:   id,
			Name: ast.NewCIStr(name),
		}
		dbInfo.Deprecated.Tables = []*model.TableInfo{tableInfo}
		tableInfo.DBID = id
		dbInfos = append(dbInfos, dbInfo)
	}
	
	// Create InfoSchema with all tables from all databases
	allTables := make([]*model.TableInfo, 0, len(dbInfos))
	for _, dbInfo := range dbInfos {
		allTables = append(allTables, dbInfo.Deprecated.Tables...)
	}
	
	// MockInfoSchema creates a single "test" database, so we need a wrapper
	// to override SchemaByName to return the correct database
	return &mockInfoSchemaWrapper{
		InfoSchema: infoschema.MockInfoSchema(allTables),
		dbNameToID: dbNameToID,
		dbInfos:    dbInfos,
	}
}

// mockInfoSchemaWrapper wraps an InfoSchema and overrides SchemaByName
// to support multiple databases for testing
type mockInfoSchemaWrapper struct {
	infoschema.InfoSchema
	dbNameToID map[string]int64
	dbInfos    []*model.DBInfo
}

func (m *mockInfoSchemaWrapper) SchemaByName(schema ast.CIStr) (val *model.DBInfo, ok bool) {
	// Check our custom database map first
	for _, dbInfo := range m.dbInfos {
		if dbInfo.Name.L == schema.L {
			return dbInfo, true
		}
	}
	// Fall back to the wrapped InfoSchema (which may have "test" and "mysql" databases)
	return m.InfoSchema.SchemaByName(schema)
}

func TestReuseExistingDatabaseIDs(t *testing.T) {
	tests := []struct {
		name           string
		initialMap     map[UpstreamID]*DBReplace
		infoSchemaDBs  map[string]int64 // db name -> db id
		expectedMap    map[UpstreamID]*DBReplace
		expectedReused map[UpstreamID]bool
	}{
		{
			name: "reuse existing database id when name matches",
			initialMap: map[UpstreamID]*DBReplace{
				1: NewDBReplace("db1", -1),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db1": 100,
				"db2": 200,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "db1",
					DbID:        100,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
				2: {
					Name:        "db2",
					DbID:        200,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: true,
				2: true,
			},
		},
		{
			name: "skip when database name not found in infoschema",
			initialMap: map[UpstreamID]*DBReplace{
				1: NewDBReplace("db1", -1),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db1": 100,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "db1",
					DbID:        100,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
				2: {
					Name:        "db2",
					DbID:        -2,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      false,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: true,
				2: false,
			},
		},
		{
			name: "skip when database is filtered out",
			initialMap: map[UpstreamID]*DBReplace{
				1: func() *DBReplace {
					dr := NewDBReplace("db1", -1)
					dr.FilteredOut = true
					return dr
				}(),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db1": 100,
				"db2": 200,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "db1",
					DbID:        -1,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: true,
					Reused:      false,
				},
				2: {
					Name:        "db2",
					DbID:        200,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: false,
				2: true,
			},
		},
		{
			name: "skip when database already has positive id",
			initialMap: map[UpstreamID]*DBReplace{
				1: NewDBReplace("db1", 100),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db1": 999,
				"db2": 200,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "db1",
					DbID:        100,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      false,
				},
				2: {
					Name:        "db2",
					DbID:        200,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: false,
				2: true,
			},
		},
		{
			name: "case insensitive database name matching",
			initialMap: map[UpstreamID]*DBReplace{
				1: NewDBReplace("DB1", -1),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db1": 100,
				"DB2": 200,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "DB1",
					DbID:        100,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
				2: {
					Name:        "db2",
					DbID:        200,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: true,
				2: true,
			},
		},
		{
			name: "empty database name should not match",
			initialMap: map[UpstreamID]*DBReplace{
				1: NewDBReplace("", -1),
				2: NewDBReplace("db2", -2),
			},
			infoSchemaDBs: map[string]int64{
				"db2": 200,
			},
			expectedMap: map[UpstreamID]*DBReplace{
				1: {
					Name:        "",
					DbID:        -1,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      false,
				},
				2: {
					Name:        "db2",
					DbID:        200,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: false,
					Reused:      true,
				},
			},
			expectedReused: map[UpstreamID]bool{
				1: false,
				2: true,
			},
		},
		{
			name:           "empty map should not cause issues",
			initialMap:     map[UpstreamID]*DBReplace{},
			infoSchemaDBs:  map[string]int64{"db1": 100},
			expectedMap:    map[UpstreamID]*DBReplace{},
			expectedReused: map[UpstreamID]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create manager with initial map
			tm := NewTableMappingManager()
			tm.DBReplaceMap = make(map[UpstreamID]*DBReplace)
			for k, v := range tt.initialMap {
				// Create a copy to avoid modifying the original
				dbReplace := &DBReplace{
					Name:        v.Name,
					DbID:        v.DbID,
					TableMap:    make(map[UpstreamID]*TableReplace),
					FilteredOut: v.FilteredOut,
					Reused:      v.Reused,
				}
				for tk, tv := range v.TableMap {
					dbReplace.TableMap[tk] = tv
				}
				tm.DBReplaceMap[k] = dbReplace
			}

			// Create mock infoschema
			mockIS := createMockInfoSchemaWithDBs(tt.infoSchemaDBs)

			// Call the method under test
			tm.ReuseExistingDatabaseIDs(mockIS)

			// Verify results
			require.Equal(t, len(tt.expectedMap), len(tm.DBReplaceMap), "number of databases should match")

			for upID, expected := range tt.expectedMap {
				actual, exists := tm.DBReplaceMap[upID]
				require.True(t, exists, "database %d should exist", upID)
				require.Equal(t, expected.Name, actual.Name, "database name should match")
				require.Equal(t, expected.DbID, actual.DbID, "database ID should match")
				require.Equal(t, expected.FilteredOut, actual.FilteredOut, "FilteredOut should match")
				if expectedReused, ok := tt.expectedReused[upID]; ok {
					require.Equal(t, expectedReused, actual.Reused, "Reused flag should match for db %d", upID)
				}
			}
		})
	}
}
