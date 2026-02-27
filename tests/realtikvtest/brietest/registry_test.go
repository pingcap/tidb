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
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	cfg.Store = config.StoreTypeTiKV
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

	testfailpoint.Enable(t, "github.com/pingcap/tidb/br/pkg/registry/is-task-stale-ticker-duration", "return(1)")
	// Create registry
	r, err := registry.NewRestoreRegistry(context.Background(), g, dom)
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

	// Test 1: User-specified RestoreTS should be preserved
	restoreID, resolvedRestoreTS, err := r.ResumeOrCreateRegistration(ctx, info, true) // isRestoredTSUserSpecified = true
	require.NoError(t, err)
	require.Greater(t, restoreID, uint64(0))
	require.Equal(t, uint64(200), resolvedRestoreTS, "User-specified RestoreTS should be preserved")

	// Verify registration exists in the database
	tk.MustExec(fmt.Sprintf("USE %s", registry.RestoreRegistryDBName))
	rows := tk.MustQuery(fmt.Sprintf("SELECT id, filter_strings, status, restored_ts FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))

	// Check first row has the ID, status "running", and correct RestoreTS
	row := rows.Rows()[0]
	require.Equal(t, fmt.Sprintf("%d", restoreID), row[0])
	require.Equal(t, "db.table", row[1])
	require.Equal(t, "running", row[2])
	require.Equal(t, "200", row[3])

	// Test pausing a task
	err = r.PauseTask(ctx, restoreID)
	require.NoError(t, err)

	// Verify task is now paused
	rows = tk.MustQuery(fmt.Sprintf("SELECT status FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))
	require.Equal(t, "paused", rows.Rows()[0][0])

	// Test 2: Auto-detected RestoreTS resolution from existing paused task
	// Create a new info with different RestoreTS but same other parameters
	infoWithDifferentTS := registry.RegistrationInfo{
		FilterStrings:     []string{"db.table"},
		StartTS:           100,
		RestoredTS:        999, // Different RestoreTS (simulating auto-detected value)
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	// Try to register with auto-detected RestoreTS (isRestoredTSUserSpecified = false)
	// This should find the existing paused task and reuse its RestoreTS (200)
	resumedID, resolvedRestoreTS2, err := r.ResumeOrCreateRegistration(ctx, infoWithDifferentTS, false)
	require.NoError(t, err)
	require.Equal(t, restoreID, resumedID, "Should resume the existing task")
	require.Equal(t, uint64(200), resolvedRestoreTS2, "Should resolve to existing task's RestoreTS, not the auto-detected value")
	require.NotEqual(t, uint64(999), resolvedRestoreTS2, "Should NOT use the auto-detected RestoreTS")

	// Verify task is running again
	rows = tk.MustQuery(fmt.Sprintf("SELECT status FROM %s WHERE id = %d",
		registry.RestoreRegistryTableName, restoreID))
	require.Equal(t, "running", rows.Rows()[0][0])

	err = r.PauseTask(ctx, restoreID)
	require.NoError(t, err)

	// Test 3: User explicitly specifies SAME restoredTS as existing paused task
	// Should reuse the existing task (new behavior test)
	infoWithSameUserSpecifiedTS := registry.RegistrationInfo{
		FilterStrings:     []string{"db.table"},
		StartTS:           100,
		RestoredTS:        200, // Same RestoreTS as existing task
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	resumedID2, resolvedRestoreTS3, err := r.ResumeOrCreateRegistration(ctx, infoWithSameUserSpecifiedTS, true)
	require.NoError(t, err)
	require.Equal(t, restoreID, resumedID2, "Should reuse existing task when user specifies same restoredTS")
	require.Equal(t, uint64(200), resolvedRestoreTS3, "Should use the same restoredTS")

	// Pause task again for next test
	err = r.PauseTask(ctx, restoreID)
	require.NoError(t, err)

	// Test 4: Auto-detected restoredTS is SAME as existing paused task
	// Should also reuse the existing task
	infoWithSameAutoDetectedTS := registry.RegistrationInfo{
		FilterStrings:     []string{"db.table"},
		StartTS:           100,
		RestoredTS:        200, // Same RestoreTS (auto-detected)
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	resumedID3, resolvedRestoreTS4, err := r.ResumeOrCreateRegistration(ctx, infoWithSameAutoDetectedTS, false) // false = auto-detected
	require.NoError(t, err)
	require.Equal(t, restoreID, resumedID3, "Should reuse existing task when auto-detected restoredTS is same")
	require.Equal(t, uint64(200), resolvedRestoreTS4, "Should use the same restoredTS")

	// Test 5: Conflict detection - same task already running, waiting until it is stale
	resumedID5, resolvedRestoreTS5, err := r.ResumeOrCreateRegistration(ctx, info, true)
	require.NoError(t, err)
	require.Equal(t, restoreID, resumedID5, "Should reuse existing task when auto-detected restoredTS is same")
	require.Equal(t, uint64(200), resolvedRestoreTS5, "Should use the same restoredTS")

	// Test 4: New task with different parameters should get its own RestoreTS
	infoNewTask := registry.RegistrationInfo{
		FilterStrings:     []string{"different.table"}, // Different filter
		StartTS:           100,
		RestoredTS:        888,
		UpstreamClusterID: 1,
		WithSysTable:      true,
		Cmd:               "restore",
	}

	newTaskID, resolvedRestoreTS3, err := r.ResumeOrCreateRegistration(ctx, infoNewTask, false)
	require.NoError(t, err)
	require.NotEqual(t, restoreID, newTaskID, "Should create a new task")
	require.Equal(t, uint64(888), resolvedRestoreTS3, "Should use the provided RestoreTS since no existing task matches")

	// Test unregistering
	err = r.Unregister(ctx, restoreID)
	require.NoError(t, err)
	err = r.Unregister(ctx, newTaskID)
	require.NoError(t, err)

	// Verify the rows are gone from the table
	rows = tk.MustQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id IN (%d, %d)",
		registry.RestoreRegistryDBName, registry.RestoreRegistryTableName, restoreID, newTaskID))
	require.Equal(t, "0", rows.Rows()[0][0])
}

func TestRegistryConfigurationOperations(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r1, err := registry.NewRestoreRegistry(context.Background(), g, dom)
	require.NoError(t, err)
	defer r1.Close()
	r2, err := registry.NewRestoreRegistry(context.Background(), g, dom)
	require.NoError(t, err)
	defer r2.Close()

	ctx := context.Background()

	putAndDrop := func() {
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
		restoreID1, _, err := r1.ResumeOrCreateRegistration(ctx, info1, false)
		require.NoError(t, err)
		var restoreID2 uint64
		var k int = 1
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sleeptime := time.Millisecond * time.Duration(100+rand.IntN(200))
			time.Sleep(sleeptime)

			restoreID2, _, err = r2.ResumeOrCreateRegistration(ctx, info2, false)
			require.NoError(t, err)
			r2.OperationAfterWaitIDs(ctx, func() error {
				k = -1
				return nil
			})
		}()

		go func() {
			defer wg.Done()
			sleeptime := time.Millisecond * time.Duration(100+rand.IntN(200))
			time.Sleep(sleeptime)

			r1.GlobalOperationAfterSetResettingStatus(ctx, restoreID1, func() error {
				k = 1
				return nil
			})
			r1.Unregister(ctx, restoreID1)
		}()
		wg.Wait()
		require.Equal(t, int(-1), k)
		// clean the registry
		err = r2.Unregister(ctx, restoreID2)
		require.NoError(t, err)
	}

	putAndDrop()

	dropAndDrop := func() {
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
		restoreID1, _, err := r1.ResumeOrCreateRegistration(ctx, info1, false)
		require.NoError(t, err)
		restoreID2, _, err := r2.ResumeOrCreateRegistration(ctx, info2, false)
		require.NoError(t, err)
		var k int = -1
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sleeptime := time.Millisecond * time.Duration(100+rand.IntN(200))
			time.Sleep(sleeptime)

			err = r2.GlobalOperationAfterSetResettingStatus(ctx, restoreID2, func() error {
				k = 1
				return nil
			})
			err = r2.Unregister(ctx, restoreID2)
			require.NoError(t, err)
		}()

		go func() {
			defer wg.Done()
			sleeptime := time.Millisecond * time.Duration(100+rand.IntN(200))
			time.Sleep(sleeptime)

			err := r1.GlobalOperationAfterSetResettingStatus(ctx, restoreID1, func() error {
				k = 1
				return nil
			})
			r1.Unregister(ctx, restoreID1)
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Equal(t, int(1), k)
	}

	dropAndDrop()
}

func TestRegistryTableConflicts(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(context.Background(), g, dom)
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
	restoreID1, _, err := r.ResumeOrCreateRegistration(ctx, info1, false)
	require.NoError(t, err)

	// Create a PiTR tracker with tables that would conflict with info1
	tracker := utils.NewPiTRIdTracker()
	tracker.AddDB(1)                        // Add db with ID 1
	tracker.TrackTableId(1, 1)              // Add table with ID 1 in db 1
	tracker.TrackTableName("db1", "table1") // Also track by name

	// Try to create a second registration but CheckTablesWithRegisteredTasks should detect conflict
	err = r.CheckTablesWithRegisteredTasks(ctx, restoreID1+1, tracker, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be restored concurrently by current task")

	// But a different table should be allowed
	tracker = utils.NewPiTRIdTracker()
	tracker.AddDB(2)                        // Add db with ID 2
	tracker.TrackTableId(2, 1)              // Add table with ID 1 in db 2
	tracker.TrackTableName("db2", "table2") // Also track by name
	err = r.CheckTablesWithRegisteredTasks(ctx, restoreID1+1, tracker, nil, nil)
	require.NoError(t, err)

	// Register the second task with non-conflicting table
	restoreID2, _, err := r.ResumeOrCreateRegistration(ctx, info2, false)
	require.NoError(t, err)

	// Clean up
	err = r.Unregister(ctx, restoreID1)
	require.NoError(t, err)
	err = r.Unregister(ctx, restoreID2)
	require.NoError(t, err)
}

func TestPreventConcurrentRestoreOfTheSameDatabase(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(context.Background(), g, dom)
	require.NoError(t, err)
	defer r.Close()

	ctx := context.Background()

	passedFn := func(err error) { require.NoError(t, err) }
	failedFn := func(err error) { require.Error(t, err) }

	cases := []struct {
		registeredFilterString []string
		registeredFilterCmd    string
		trackerDBNames         []string
		snapshotDBNames        []string
		expectedCheckError     func(err error)
	}{
		{
			// previous running task: full restore db1.table1
			// current task: full restore db1.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Full Restore",
			snapshotDBNames:        []string{"db1"},
			expectedCheckError:     passedFn,
		},
		{
			// previous running task: full restore db1.table1
			// current task: full restore db2.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Full Restore",
			snapshotDBNames:        []string{"db2"},
			expectedCheckError:     passedFn,
		},
		{
			// previous running task: full restore db1.table1
			// current task: point restore db1.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Full Restore",
			trackerDBNames:         []string{"db1"},
			expectedCheckError:     failedFn,
		},
		{
			// previous running task: full restore db1.table1
			// current task: point restore db1.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Full Restore",
			trackerDBNames:         []string{"db2"},
			expectedCheckError:     passedFn,
		},
		{
			// previous running task: point restore db1.table1
			// current task: point restore db2.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Point Restore",
			trackerDBNames:         []string{"db2"},
			expectedCheckError:     passedFn,
		},
		{
			// previous running task: point restore db1.table1
			// current task: point restore db1.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Point Restore",
			trackerDBNames:         []string{"db1"},
			expectedCheckError:     failedFn,
		},
		{
			// previous running task: point restore db1.table1
			// current task: full restore db1.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Point Restore",
			snapshotDBNames:        []string{"db1"},
			expectedCheckError:     failedFn,
		},
		{
			// previous running task: point restore db1.table1
			// current task: full restore db2.table2
			registeredFilterString: []string{"db1.table1"},
			registeredFilterCmd:    "Point Restore",
			snapshotDBNames:        []string{"db2"},
			expectedCheckError:     passedFn,
		},
	}

	for _, cs := range cases {
		info1 := registry.RegistrationInfo{
			FilterStrings:     cs.registeredFilterString,
			StartTS:           100,
			RestoredTS:        200,
			UpstreamClusterID: 1,
			WithSysTable:      true,
			Cmd:               cs.registeredFilterCmd,
		}

		// Register the first task
		restoreID1, _, err := r.ResumeOrCreateRegistration(ctx, info1, false)
		require.NoError(t, err)

		var tracker *utils.PiTRIdTracker
		if len(cs.trackerDBNames) > 0 {
			tracker = utils.NewPiTRIdTracker()
			for _, dbName := range cs.trackerDBNames {
				tracker.TrackTableName(dbName, "table2")
			}
		}

		var dbs []*metautil.Database
		if len(cs.snapshotDBNames) > 0 {
			for _, dbName := range cs.snapshotDBNames {
				dbs = append(dbs, &metautil.Database{
					Info: &model.DBInfo{
						Name: ast.NewCIStr(dbName),
					},
				})
			}
		}

		err = r.CheckTablesWithRegisteredTasks(ctx, restoreID1+1, tracker, dbs, nil)
		if cs.expectedCheckError != nil {
			cs.expectedCheckError(err)
		}

		// Clean up
		err = r.Unregister(ctx, restoreID1)
		require.NoError(t, err)
	}
}

func TestGetRegistrationsByMaxID(t *testing.T) {
	tk, dom, g := initRegistryTest(t)
	cleanupRegistryTable(tk)

	// Create registry
	r, err := registry.NewRestoreRegistry(context.Background(), g, dom)
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
	restoreID1, _, err := r.ResumeOrCreateRegistration(ctx, info1, false)
	require.NoError(t, err)

	restoreID2, _, err := r.ResumeOrCreateRegistration(ctx, info2, false)
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
