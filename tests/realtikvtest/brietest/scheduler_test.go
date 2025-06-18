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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/task"
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

// TestLogRestoreFineGrainedSchedulerPausing tests that log restore uses fine-grained
// scheduler pausing when filters are specified and full pausing when no filters are used
func TestLogRestoreFineGrainedSchedulerPausing(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)

	// Create tables before log backup (these will be in snapshot range)
	kit.tk.MustExec("CREATE TABLE test.snapshot_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("CREATE TABLE test.snapshot_table2 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (1, 'snapshot1'), (2, 'snapshot2')")
	kit.tk.MustExec("INSERT INTO test.snapshot_table2 VALUES (1, 'snapshot1'), (2, 'snapshot2')")

	taskName := t.Name()

	// Start log backup first, then take full backup to avoid timing gaps
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
	kit.RunFullBackup(func(bc *task.BackupConfig) {})

	// Create tables during log backup (these will be in log backup range)
	kit.tk.MustExec("CREATE TABLE test.log_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("CREATE TABLE test.log_table2 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (1, 'log1'), (2, 'log2')")
	kit.tk.MustExec("INSERT INTO test.log_table2 VALUES (1, 'log1'), (2, 'log2')")

	// Add some incremental data to existing tables
	s.insertSimpleIncreaseData(kit)
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (3, 'incremental')")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (3, 'incremental')")

	kit.forceFlushAndWait(taskName)
	kit.StopTaskIfExists(taskName)

	// Test case 1: Filtered restore (should use fine-grained pausing)
	t.Run("FilteredRestore", func(t *testing.T) {
		s.cleanSimpleData(kit)
		kit.tk.MustExec("DROP TABLE IF EXISTS test.snapshot_table1, test.snapshot_table2, test.log_table1, test.log_table2")

		var schedulerRulesDuringRestore []SchedulerRule
		var ruleCount int

		// Enable failpoint to capture scheduler state during restore
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused", func() {
			// Capture scheduler rules when the failpoint is hit
			schedulerRulesDuringRestore = getPDSchedulerRules(t, "127.0.0.1:2379")
			ruleCount = countScheduleDenyRules(schedulerRulesDuringRestore)
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused")

		// Run filtered restore - failpoint will be hit during execution
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.FullBackupStorage = kit.LocalURI("full")
			// Set table filters to include both snapshot and log tables
			kit.SetFilter(&rc.Config, "test.snapshot_table1", "test.log_table1")
		})

		// Verify that scheduler rules were created (fine-grained pausing)
		// We should have exactly 2 rules: one for snapshot range, one for log range
		// Or it could be fewer if ranges are merged or optimized
		require.Greater(t, ruleCount, 0, "Expected scheduler pausing rules during filtered restore")
		require.Equal(t, ruleCount, 2, "Fine-grained pausing should 2 rules (snapshot + log ranges)")

		t.Logf("Filtered restore created %d scheduler deny rules (expected <= 2)", ruleCount)
	})

	// Test case 2: Full restore (should use full pausing)
	t.Run("FullRestore", func(t *testing.T) {
		s.cleanSimpleData(kit)
		kit.tk.MustExec("DROP TABLE IF EXISTS test.snapshot_table1, test.snapshot_table2, test.log_table1, test.log_table2")

		var schedulerRulesDuringRestore []SchedulerRule
		var ruleCount int

		// Enable failpoint to capture scheduler state during restore
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused", func() {
			// Capture scheduler rules when the failpoint is hit
			schedulerRulesDuringRestore = getPDSchedulerRules(t, "127.0.0.1:2379")
			ruleCount = countScheduleDenyRules(schedulerRulesDuringRestore)
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused")

		// Run full restore - failpoint will be hit during execution
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.FullBackupStorage = kit.LocalURI("full")
			// No filters - this should trigger full pausing (1 rule for all schedulers)
		})

		require.Equal(t, ruleCount, 1, "Full restore should create exactly 1 scheduler rule (full pausing)")

		t.Logf("Full restore created %d scheduler deny rule (expected = 1)", ruleCount)
	})

	// Verify scheduler rules are cleaned up after both tests
	finalRules := getPDSchedulerRules(t, "127.0.0.1:2379")
	finalCount := countScheduleDenyRules(finalRules)
	require.Equal(t, finalCount, 0, "All scheduler rules should be cleaned up after restore completes")
	t.Logf("Scheduler rules properly cleaned up: final count = %d", finalCount)
}
