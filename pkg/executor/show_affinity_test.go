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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockPDCliForAffinity struct {
	pdhttp.Client
	mock.Mock
}

func (m *mockPDCliForAffinity) GetAllAffinityGroups(ctx context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*pdhttp.AffinityGroupState), args.Error(1)
}

func TestShowAffinity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Clean up
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("drop database if exists db2")

	// Test 1: Show affinity with no affinity tables - should return empty
	result := tk.MustQuery("show affinity")
	require.Equal(t, 0, len(result.Rows()))

	// Test 2: Create table with table-level affinity
	tk.MustExec("create table t1 (id int) affinity='table'")
	defer tk.MustExec("drop table if exists t1")

	result = tk.MustQuery("show affinity")
	rows := result.Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "test", rows[0][0])        // Db_name
	require.Equal(t, "t1", rows[0][1])          // Table_name
	require.Equal(t, "", rows[0][2])            // Partition_name (empty for table-level)
	require.Contains(t, rows[0][3], "_tidb_t_") // Affinity_group_id

	// Test 3: Create partitioned table with partition-level affinity
	tk.MustExec(`create table t2 (id int) affinity='partition'
		PARTITION BY RANGE (id) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (200),
			PARTITION p2 VALUES LESS THAN MAXVALUE
		)`)
	defer tk.MustExec("drop table if exists t2")

	result = tk.MustQuery("show affinity")
	rows = result.Rows()
	require.Greater(t, len(rows), 1) // Should have t1 + partitions of t2

	// Count rows for t2 (should be 3 partitions)
	t2Count := 0
	for _, row := range rows {
		if row[1] == "t2" {
			t2Count++
			require.NotEqual(t, "", row[2])          // Partition_name should not be empty
			require.Contains(t, row[3], "_tidb_pt_") // Affinity_group_id for partition
		}
	}
	require.Equal(t, 3, t2Count)

	// Test 4: Create table without affinity - should not appear
	tk.MustExec("create table t3 (id int)")
	defer tk.MustExec("drop table if exists t3")

	result = tk.MustQuery("show affinity")
	rows = result.Rows()
	for _, row := range rows {
		require.NotEqual(t, "t3", row[1]) // t3 should not appear
	}

	// Test 5: Test LIKE filter
	tk.MustExec("create database db2")
	defer tk.MustExec("drop database if exists db2")
	tk.MustExec("create table db2.t1 (id int) affinity='table'")
	defer tk.MustExec("drop table if exists db2.t1")

	// Filter by database name using LIKE
	result = tk.MustQuery("show affinity like 'db2'")
	rows = result.Rows()
	require.Greater(t, len(rows), 0)
	for _, row := range rows {
		require.Equal(t, "db2", row[0])
	}

	result = tk.MustQuery("show affinity like 'test'")
	rows = result.Rows()
	for _, row := range rows {
		require.Equal(t, "test", row[0])
	}
}

func TestShowAffinityColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	// Step 1: Create table first
	tk.MustExec("create table t1 (id int) affinity='table'")
	defer tk.MustExec("drop table if exists t1")

	// Step 2: Get actual table ID from session
	is := tk.Session().GetLatestInfoSchema()
	tbl, err := is.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(tbl.ID)

	// Step 3: Set up mock PD client
	mockCli := &mockPDCliForAffinity{}
	recoverFn := infosync.SetPDHttpCliForTest(mockCli)
	defer recoverFn()

	// Step 4: Mock the GetAllAffinityGroups call with actual group ID
	mockCli.On("GetAllAffinityGroups", mock.Anything).Return(
		map[string]*pdhttp.AffinityGroupState{
			groupID: {
				AffinityGroup: pdhttp.AffinityGroup{
					LeaderStoreID: 1,
					VoterStoreIDs: []uint64{1, 2, 3},
				},
				Phase:               "stable",
				RegionCount:         10,
				AffinityRegionCount: 9,
			},
		}, nil,
	).Once()

	// Step 5: Execute query and verify results
	result := tk.MustQuery("show affinity")
	require.Equal(t, 9, len(result.Rows()[0])) // Should have 9 columns

	rows := result.Rows()
	require.Equal(t, 1, len(rows))
	row := rows[0]

	// Db_name
	require.Equal(t, "test", row[0])
	// Table_name
	require.Equal(t, "t1", row[1])
	// Partition_name
	require.Equal(t, "", row[2])
	// Affinity_group_id
	require.Equal(t, groupID, row[3])
	// Leader_store_id
	require.Equal(t, "1", row[4])
	// Voter_store_ids
	require.Equal(t, "1,2,3", row[5])
	// Status
	require.Equal(t, "Stable", row[6])
	// Region_count
	require.Equal(t, "10", row[7])
	// Affinity_region_count
	require.Equal(t, "9", row[8])
}

func TestShowAffinityNullStatus(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	// Step 1: Create table first
	tk.MustExec("create table t1 (id int) affinity='table'")
	defer tk.MustExec("drop table if exists t1")

	// Step 2: Get actual table ID from session
	is := tk.Session().GetLatestInfoSchema()
	tbl, err := is.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(tbl.ID)

	// Step 3: Set up mock PD client
	mockCli := &mockPDCliForAffinity{}
	recoverFn := infosync.SetPDHttpCliForTest(mockCli)
	defer recoverFn()

	// Step 4: Mock PD response with empty map (group not found)
	mockCli.On("GetAllAffinityGroups", mock.Anything).Return(
		map[string]*pdhttp.AffinityGroupState{}, nil,
	).Once()

	// Step 5: Test that status shows as "NULL" when group not found in PD
	result := tk.MustQuery("show affinity")
	rows := result.Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "test", rows[0][0])  // Db_name
	require.Equal(t, "t1", rows[0][1])    // Table_name
	require.Equal(t, "", rows[0][2])      // Partition_name
	require.Equal(t, groupID, rows[0][3]) // Affinity_group_id
	require.Equal(t, "NULL", rows[0][6])  // Status should be NULL
}
