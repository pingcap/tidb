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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// SchedulerRule represents a PD scheduler rule
type SchedulerRule struct {
	GroupID   string              `json:"group_id"`
	ID        string              `json:"id"`
	StartKey  string              `json:"start_key"`
	EndKey    string              `json:"end_key"`
	Role      string              `json:"role"`
	Count     int                 `json:"count"`
	LabelKeys []string            `json:"label_keys"`
	Labels    []map[string]string `json:"labels"`
}

// getPDSchedulerRules fetches the current scheduler rules from PD
func getPDSchedulerRules(t *testing.T, pdAddr string) []SchedulerRule {
	url := fmt.Sprintf("http://%s/pd/api/v1/config/region-label/rules", pdAddr)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var rules []SchedulerRule
	err = json.Unmarshal(body, &rules)
	require.NoError(t, err)

	return rules
}

// countScheduleDenyRules counts how many rules have schedule=deny label
func countScheduleDenyRules(rules []SchedulerRule) int {
	count := 0
	for _, rule := range rules {
		for _, label := range rule.Labels {
			if label["key"] == "schedule" && label["value"] == "deny" {
				count++
				break
			}
		}
	}
	return count
}

// waitForSchedulerRules polls PD to verify scheduler rules are in effect and returns the count
func waitForSchedulerRules(t *testing.T, pdAddr string) int {
	// Wait a bit for scheduler rules to be applied by PD
	time.Sleep(1 * time.Second)

	// Poll PD to verify scheduler rules are in effect
	var ruleCount int
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		schedulerRules := getPDSchedulerRules(t, pdAddr)
		ruleCount = countScheduleDenyRules(schedulerRules)

		if ruleCount > 0 {
			t.Logf("Verified scheduler rules are active: %d rules found (attempt %d/%d)", ruleCount, i+1, maxRetries)
			break
		}

		if i < maxRetries-1 {
			t.Logf("No scheduler rules found yet, retrying... (attempt %d/%d)", i+1, maxRetries)
			time.Sleep(500 * time.Millisecond)
		}
	}

	return ruleCount
}

// TestLogRestoreFineGrainedSchedulerPausing tests that log restore uses fine-grained
// scheduler pausing when filters are specified and full pausing when no filters are used
func TestLogRestoreFineGrainedSchedulerPausing(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)

	taskName := "test-scheduler-pausing"

	// Create tables before log backup (these will be in snapshot range)
	kit.tk.MustExec("CREATE TABLE test.snapshot_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("CREATE TABLE test.snapshot_table2 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (1, 'snapshot1'), (2, 'snapshot2')")
	kit.tk.MustExec("INSERT INTO test.snapshot_table2 VALUES (1, 'snapshot1'), (2, 'snapshot2')")

	// Start log backup first, then take full backup
	kit.RunLogStart(taskName, func(cfg *task.StreamConfig) {})
	kit.RunFullBackup(func(cfg *task.BackupConfig) {})

	// Create tables during log backup (these will be in log backup range)
	kit.tk.MustExec("CREATE TABLE test.log_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("CREATE TABLE test.log_table2 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (1, 'log1'), (2, 'log2')")
	kit.tk.MustExec("INSERT INTO test.log_table2 VALUES (1, 'log1'), (2, 'log2')")

	// Add some incremental data to both snapshot and log tables
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (3, 'incremental')")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (3, 'incremental')")

	kit.forceFlushAndWait(taskName)
	kit.StopTaskIfExists(taskName)

	// Test case 1: Filtered restore with scheduler verification
	t.Run("FilteredRestoreWithSchedulerVerification", func(t *testing.T) {
		s.cleanSimpleData(kit)
		kit.tk.MustExec("DROP TABLE IF EXISTS test.snapshot_table1, test.snapshot_table2, test.log_table1, test.log_table2")

		// Channel to receive the captured rule count from failpoint
		ruleCountChan := make(chan int, 1)

		// Enable failpoint to capture scheduler state during restore
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused", func() {
			ruleCount := waitForSchedulerRules(t, "127.0.0.1:2379")

			// Send the result through channel
			select {
			case ruleCountChan <- ruleCount:
			default:
			}
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused")

		// Run filtered restore - failpoint will be hit during execution
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.FullBackupStorage = kit.LocalURI("full")
			// Set table filters to include both snapshot and log tables
			kit.SetFilter(&rc.Config, "test.snapshot_table1", "test.log_table1")
		})

		// Get the captured rule count from channel
		var capturedRuleCount int
		select {
		case capturedRuleCount = <-ruleCountChan:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for scheduler rule count from failpoint")
		}

		// Verify that scheduler rules were created and detected
		require.Greater(t, capturedRuleCount, 0, "Expected scheduler pausing rules during filtered restore")
		require.LessOrEqual(t, capturedRuleCount, 2, "Fine-grained pausing should create at most 2 rules (snapshot + log ranges)")

		// Verify tables were restored
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.snapshot_table1").Check(testkit.Rows("3"))
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.log_table1").Check(testkit.Rows("3"))

		t.Logf("Filtered restore created %d scheduler deny rules and completed successfully", capturedRuleCount)
	})

	// Test case 2: Full restore with scheduler verification
	t.Run("FullRestoreWithSchedulerVerification", func(t *testing.T) {
		s.cleanSimpleData(kit)
		kit.tk.MustExec("DROP TABLE IF EXISTS test.snapshot_table1, test.snapshot_table2, test.log_table1, test.log_table2")

		// Channel to receive the captured rule count from failpoint
		ruleCountChan := make(chan int, 1)

		// Enable failpoint to capture scheduler state during restore
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused", func() {
			ruleCount := waitForSchedulerRules(t, "127.0.0.1:2379")

			// Send the result through channel
			select {
			case ruleCountChan <- ruleCount:
			default:
			}
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused")

		// Run full restore - failpoint will be hit during execution
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.FullBackupStorage = kit.LocalURI("full")
			// No filters - this should trigger full pausing (1 rule for all schedulers)
		})

		// Get the captured rule count from channel
		var capturedRuleCount int
		select {
		case capturedRuleCount = <-ruleCountChan:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for scheduler rule count from failpoint")
		}

		// For full restore, should use traditional full scheduler pausing (1 rule)
		// vs fine-grained pausing (2 rules) for filtered restore
		require.Equal(t, capturedRuleCount, 1, "Full restore should create exactly 1 scheduler rule (full pausing)")

		// Verify all tables were restored
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.snapshot_table1").Check(testkit.Rows("3"))
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.snapshot_table2").Check(testkit.Rows("2"))
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.log_table1").Check(testkit.Rows("3"))
		kit.tk.MustQuery("SELECT COUNT(*) FROM test.log_table2").Check(testkit.Rows("2"))

		t.Logf("Full restore created %d scheduler deny rule and completed successfully", capturedRuleCount)
	})

	// Verify scheduler rules are cleaned up after both tests
	finalRules := getPDSchedulerRules(t, "127.0.0.1:2379")
	finalCount := countScheduleDenyRules(finalRules)
	require.Equal(t, finalCount, 0, "All scheduler rules should be cleaned up after restore completes")
	t.Logf("Scheduler rules properly cleaned up: final count = %d", finalCount)
}
