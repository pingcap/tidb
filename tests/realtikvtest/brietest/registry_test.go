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

package brietest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// initRegistryTest initializes the test environment for registry tests
func initRegistryTest(t *testing.T) (*testkit.TestKit, *domain.Domain, glue.Glue) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run BR SQL integration test with tikv store")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)

	cfg := config.GetGlobalConfig()
	cfg.Store = "tikv"
	cfg.Path = "127.0.0.1:2379"
	config.StoreGlobalConfig(cfg)

	tk := testkit.NewTestKit(t, store)

	dom, err := session.GetDomain(store)
	require.NoError(t, err)

	g := gluetidb.New()

	return tk, dom, g
}

// cleanupRegistryTable truncates the registry table to ensure clean state between tests
func cleanupRegistryTable(tk *testkit.TestKit) {
	// Clean up the registry table to ensure clean state for next test
	tk.MustExec(fmt.Sprintf("DELETE FROM %s.%s", registry.RestoreRegistryDBName, registry.RestoreRegistryTableName))
}

func TestRegistryBasicOperations(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(g, dom)
	require.NoError(t, err)
	defer r.Close()

	ctx := context.Background()

	// Test table should be created automatically
	tk.MustExec(fmt.Sprintf("SHOW DATABASES LIKE '%s'", registry.RestoreRegistryDBName))

	// Test creating a new registration
	info := registry.RegistrationInfo{
		FilterStrings:     []string{"db.table"},
		StartTS:           100,
		RestoredTS:        200,
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	restoreID, err := r.ResumeOrCreateRegistration(ctx, info)
	require.NoError(t, err)
	require.Greater(t, restoreID, uint64(0))

	// Verify registration exists in the database
	tk.MustExec(fmt.Sprintf("USE %s", registry.RestoreRegistryDBName))
	rows := tk.MustQuery(fmt.Sprintf("SELECT id, filter_strings, status FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))

	// Check first row has the ID and status "running"
	row := rows.Rows()[0]
	require.Equal(t, fmt.Sprintf("%d", restoreID), row[0])
	require.Equal(t, "db.table", row[1])
	require.Equal(t, "running", row[2])

	// Test pausing a task
	err = r.PauseTask(ctx, restoreID)
	require.NoError(t, err)

	// Verify task is now paused
	rows = tk.MustQuery(fmt.Sprintf("SELECT status FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))
	require.Equal(t, "paused", rows.Rows()[0][0])

	// Test resuming a paused task
	resumedID, err := r.ResumeOrCreateRegistration(ctx, info)
	require.NoError(t, err)
	require.Equal(t, restoreID, resumedID)

	// Verify task is running again
	rows = tk.MustQuery(fmt.Sprintf("SELECT status FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))
	require.Equal(t, "running", rows.Rows()[0][0])

	// Test conflict detection
	_, err = r.ResumeOrCreateRegistration(ctx, info)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists and is running")

	// Test unregistering
	err = r.Unregister(ctx, restoreID)
	require.NoError(t, err)

	// Verify the specific row is gone from the table
	rows = tk.MustQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id = %d",
		registry.RestoreRegistryDBName, registry.RestoreRegistryTableName, restoreID))
	require.Equal(t, "0", rows.Rows()[0][0])
}

func TestRegistryTableConflicts(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(g, dom)
	require.NoError(t, err)
	defer r.Close()

	ctx := context.Background()

	// Create two registrations with different filter patterns
	info1 := registry.RegistrationInfo{
		FilterStrings:     []string{"db1.table1"},
		StartTS:           100,
		RestoredTS:        200,
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	info2 := registry.RegistrationInfo{
		FilterStrings:     []string{"db2.table2"},
		StartTS:           100,
		RestoredTS:        200,
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	// Register the first task
	restoreID1, err := r.ResumeOrCreateRegistration(ctx, info1)
	require.NoError(t, err)

	// Create a PiTR tracker with tables that would conflict with info1
	tracker := utils.NewPiTRIdTracker()
	tracker.AddDB(1)                        // Add db with ID 1
	tracker.TrackTableId(1, 1)              // Add table with ID 1 in db 1
	tracker.TrackTableName("db1", "table1") // Also track by name

	// Try to create a second registration but CheckTablesWithRegisteredTasks should detect conflict
	err = r.CheckTablesWithRegisteredTasks(ctx, restoreID1+1, tracker, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be restored by current task")

	// But a different table should be allowed
	tracker = utils.NewPiTRIdTracker()
	tracker.AddDB(2)                        // Add db with ID 2
	tracker.TrackTableId(2, 1)              // Add table with ID 1 in db 2
	tracker.TrackTableName("db2", "table2") // Also track by name
	err = r.CheckTablesWithRegisteredTasks(ctx, restoreID1+1, tracker, nil)
	require.NoError(t, err)

	// Register the second task with non-conflicting table
	restoreID2, err := r.ResumeOrCreateRegistration(ctx, info2)
	require.NoError(t, err)

	// Clean up
	err = r.Unregister(ctx, restoreID1)
	require.NoError(t, err)
	err = r.Unregister(ctx, restoreID2)
	require.NoError(t, err)
}

func TestGetRegistrationsByMaxID(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(g, dom)
	require.NoError(t, err)
	defer r.Close()

	ctx := context.Background()

	// Create a few registrations
	info1 := registry.RegistrationInfo{
		FilterStrings:     []string{"db1.table1"},
		StartTS:           100,
		RestoredTS:        200,
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore task1",
	}

	info2 := registry.RegistrationInfo{
		FilterStrings:     []string{"db2.table2"},
		StartTS:           300,
		RestoredTS:        400,
		UpstreamClusterID: 2,
		WithSysTable:      false,
		Cmd:               "restore task2",
	}

	// Register the tasks
	restoreID1, err := r.ResumeOrCreateRegistration(ctx, info1)
	require.NoError(t, err)

	restoreID2, err := r.ResumeOrCreateRegistration(ctx, info2)
	require.NoError(t, err)

	// Get all registrations
	regs, err := r.GetRegistrationsByMaxID(ctx, restoreID2+1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(regs), 2)

	// Verify we have both our registrations
	foundTask1 := false
	foundTask2 := false

	for _, reg := range regs {
		if reg.Cmd == "restore task1" {
			foundTask1 = true
			require.Equal(t, uint64(100), reg.StartTS)
			require.Equal(t, uint64(200), reg.RestoredTS)
			require.Equal(t, []string{"db1.table1"}, reg.FilterStrings)
		}

		if reg.Cmd == "restore task2" {
			foundTask2 = true
			require.Equal(t, uint64(300), reg.StartTS)
			require.Equal(t, uint64(400), reg.RestoredTS)
			require.Equal(t, []string{"db2.table2"}, reg.FilterStrings)
		}
	}

	require.True(t, foundTask1, "Task 1 was not found in registrations")
	require.True(t, foundTask2, "Task 2 was not found in registrations")

	// Test getting registrations with lower max ID
	regs, err = r.GetRegistrationsByMaxID(ctx, restoreID1)
	require.NoError(t, err)

	// Since we're using a private field, just check that we get fewer results
	require.Less(t, len(regs), 2)

	// Clean up
	err = r.Unregister(ctx, restoreID1)
	require.NoError(t, err)
	err = r.Unregister(ctx, restoreID2)
	require.NoError(t, err)
}
