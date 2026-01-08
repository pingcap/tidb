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

	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	RuleType  string              `json:"rule_type"`
	Data      any                 `json:"data"`
}

// KeyRange represents a key range in the scheduler rule data
type KeyRange struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

const (
	EmptyRangeStart = ""
	EmptyRangeEnd   = ""
)

// getPDSchedulerRules fetches the current scheduler rules from PD
func getPDSchedulerRules(t *testing.T, pdAddr string) ([]SchedulerRule, error) {
	url := fmt.Sprintf("http://%s/pd/api/v1/config/region-label/rules", pdAddr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rules []SchedulerRule
	err = json.Unmarshal(body, &rules)
	if err != nil {
		return nil, err
	}

	return rules, nil
}

// extractKeyRangesFromRule extracts key ranges from a scheduler rule's Data field
func extractKeyRangesFromRule(rule SchedulerRule) ([]KeyRange, error) {
	if rule.Data == nil {
		return nil, nil
	}

	dataBytes, err := json.Marshal(rule.Data)
	if err != nil {
		return nil, err
	}

	var keyRanges []KeyRange
	err = json.Unmarshal(dataBytes, &keyRanges)
	if err != nil {
		return nil, err
	}

	return keyRanges, nil
}

// analyzeSchedulerRules provides detailed analysis of scheduler rules and their key ranges
func analyzeSchedulerRules(t *testing.T, rules []SchedulerRule, title string) map[string][]KeyRange {
	t.Logf("=== %s ===", title)
	t.Logf("Total rules found: %d", len(rules))

	allKeyRanges := make(map[string][]KeyRange)

	for i, rule := range rules {
		keyRanges, err := extractKeyRangesFromRule(rule)
		if err != nil {
			t.Logf("Failed to parse key ranges from rule %d: %v", i+1, err)
		}

		ruleKey := fmt.Sprintf("%s/%s", rule.GroupID, rule.ID)
		allKeyRanges[ruleKey] = keyRanges

		t.Logf("Rule %d: ID=%s, GroupID=%s", i+1, rule.ID, rule.GroupID)
		t.Logf("  StartKey=%s, EndKey=%s", rule.StartKey, rule.EndKey)
		t.Logf("  Role=%s, Count=%d, RuleType=%s", rule.Role, rule.Count, rule.RuleType)

		if len(rule.Labels) > 0 {
			t.Logf("  Labels:")
			for _, label := range rule.Labels {
				t.Logf("    - %s=%s", label["key"], label["value"])
			}
		}

		if len(keyRanges) > 0 {
			t.Logf("  Key Ranges in Data field (%d total):", len(keyRanges))
			for j, kr := range keyRanges {
				t.Logf("    Range %d: %s -> %s", j+1, kr.StartKey, kr.EndKey)
			}
		}
		t.Logf("")
	}

	return allKeyRanges
}

// compareKeyRanges compares key ranges between baseline and current state
func compareKeyRanges(t *testing.T, baselineRanges, currentRanges map[string][]KeyRange) bool {
	t.Log("Comparing key ranges...")

	hasChanges := false

	for ruleKey, currentKRs := range currentRanges {
		baselineKRs, existedInBaseline := baselineRanges[ruleKey]

		if !existedInBaseline {
			t.Logf("NEW RULE: %s with %d key ranges", ruleKey, len(currentKRs))
			for i, kr := range currentKRs {
				t.Logf("  Range %d: %s -> %s", i+1, kr.StartKey, kr.EndKey)
			}
			hasChanges = true
			continue
		}

		// Compare existing rule's key ranges
		if len(currentKRs) != len(baselineKRs) {
			t.Logf("RULE MODIFIED: %s", ruleKey)
			t.Logf("  Baseline had %d ranges, now has %d ranges", len(baselineKRs), len(currentKRs))

			// Find new ranges
			newRanges := findNewKeyRanges(baselineKRs, currentKRs)
			if len(newRanges) > 0 {
				t.Logf("  NEW key ranges added:")
				for i, kr := range newRanges {
					t.Logf("    Range %d: %s -> %s", i+1, kr.StartKey, kr.EndKey)

					// Check if this is a full range pause (empty start/end keys)
					if kr.StartKey == EmptyRangeStart && kr.EndKey == EmptyRangeEnd {
						t.Logf("    -> FULL RANGE PAUSE detected (empty start/end keys)")
					} else {
						t.Logf("    -> FINE-GRAINED PAUSE detected (specific key range)")
					}
				}
				t.Logf("  This indicates scheduler pausing is active!")
				hasChanges = true
			}

			// Find removed ranges
			removedRanges := findNewKeyRanges(currentKRs, baselineKRs)
			if len(removedRanges) > 0 {
				t.Logf("  Key ranges removed:")
				for i, kr := range removedRanges {
					t.Logf("    Range %d: %s -> %s", i+1, kr.StartKey, kr.EndKey)
				}
				hasChanges = true
			}
		} else {
			t.Logf("RULE UNCHANGED: %s (%d ranges)", ruleKey, len(currentKRs))
		}
	}

	return hasChanges
}

// findNewKeyRanges finds ranges in 'current' that are not in 'baseline'
func findNewKeyRanges(baseline, current []KeyRange) []KeyRange {
	var newRanges []KeyRange

	for _, currentRange := range current {
		found := false
		for _, baselineRange := range baseline {
			if currentRange.StartKey == baselineRange.StartKey && currentRange.EndKey == baselineRange.EndKey {
				found = true
				break
			}
		}
		if !found {
			newRanges = append(newRanges, currentRange)
		}
	}

	return newRanges
}

// setupTestData creates test tables and data for scheduler pausing tests
func setupTestData(kit *LogBackupKit, taskName string) {
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)

	// Create tables before log backup (these will be in snapshot range)
	kit.tk.MustExec("CREATE TABLE test.snapshot_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (1, 'snapshot1'), (2, 'snapshot2')")

	// Start log backup first, then take full backup
	kit.RunLogStart(taskName, func(cfg *task.StreamConfig) {})
	kit.RunFullBackup(func(cfg *task.BackupConfig) {})

	// Create tables during log backup (these will be in log backup range)
	kit.tk.MustExec("CREATE TABLE test.log_table1 (id INT PRIMARY KEY, data VARCHAR(100))")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (1, 'log1'), (2, 'log2')")

	// Add some incremental data
	kit.tk.MustExec("INSERT INTO test.snapshot_table1 VALUES (3, 'incremental')")
	kit.tk.MustExec("INSERT INTO test.log_table1 VALUES (3, 'incremental')")

	kit.forceFlushAndWait(taskName)
	kit.StopTaskIfExists(taskName)
}

// cleanupTestData removes test tables
func cleanupTestData(kit *LogBackupKit) {
	s := kit.simpleWorkload()
	s.cleanSimpleData(kit)
	kit.tk.MustExec("DROP TABLE IF EXISTS test.snapshot_table1, test.log_table1")
}

// checkSchedulerPausingBehavior monitors scheduler rules during restore
func checkSchedulerPausingBehavior(t *testing.T, baselineKeyRanges map[string][]KeyRange) []SchedulerRule {
	var finalRules []SchedulerRule
	maxRetries := 20

	for i := 0; i < maxRetries; i++ {
		time.Sleep(200 * time.Millisecond)

		rules, err := getPDSchedulerRules(t, "127.0.0.1:2379")
		if err != nil {
			t.Logf("Failed to get scheduler rules (attempt %d): %v", i+1, err)
			continue
		}

		t.Logf("Attempt %d: Got %d scheduler rules", i+1, len(rules))

		// Check if any rule has more key ranges than baseline
		hasNewRanges := false

		for _, rule := range rules {
			ruleKey := fmt.Sprintf("%s/%s", rule.GroupID, rule.ID)
			keyRanges, err := extractKeyRangesFromRule(rule)
			if err == nil {
				// Compare with baseline
				if baselineRanges, exists := baselineKeyRanges[ruleKey]; exists {
					if len(keyRanges) > len(baselineRanges) {
						hasNewRanges = true
						t.Logf("Detected new key ranges in rule %s! Baseline: %d, Current: %d",
							ruleKey, len(baselineRanges), len(keyRanges))
					}
				}
			}
		}

		if hasNewRanges {
			finalRules = rules
			t.Logf("Scheduler pausing detected - new key ranges added!")
			break
		}

		if i == maxRetries-1 {
			finalRules = rules
			t.Logf("Reached max retries. Using final rule set.")
		}
	}

	return finalRules
}

// TestLogRestoreFineGrainedSchedulerPausing tests that log restore uses fine-grained
// scheduler pausing when filters are specified
func TestLogRestoreFineGrainedSchedulerPausing(t *testing.T) {
	kit := NewLogBackupKit(t)
	taskName := "test-fine-grained-scheduler"

	setupTestData(kit, taskName)
	cleanupTestData(kit)

	// Get baseline scheduler rules
	t.Log("Getting baseline scheduler rules...")
	baselineRules, err := getPDSchedulerRules(t, "127.0.0.1:2379")
	require.NoError(t, err)
	baselineKeyRanges := analyzeSchedulerRules(t, baselineRules, "BASELINE SCHEDULER RULES (before restore)")

	// Create channel to capture results from the callback
	rulesChan := make(chan []SchedulerRule, 1)

	// Enable failpoint with callback that checks PD scheduler status
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/br/pkg/task/log-restore-scheduler-paused", func() {
		t.Log("Failpoint triggered - checking PD scheduler rules")
		finalRules := checkSchedulerPausingBehavior(t, baselineKeyRanges)
		rulesChan <- finalRules
	})

	// Run filtered restore
	t.Log("Starting filtered restore...")
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		kit.SetFilter(&rc.Config, "test.snapshot_table1", "test.log_table1")
	})

	// Wait for callback results
	select {
	case restoreRules := <-rulesChan:
		t.Log("Analyzing scheduler rules during filtered restore:")
		restoreKeyRanges := analyzeSchedulerRules(t, restoreRules, "SCHEDULER RULES DURING FILTERED RESTORE")
		hasChanges := compareKeyRanges(t, baselineKeyRanges, restoreKeyRanges)

		require.True(t, hasChanges, "Fine-grained scheduler pausing should be detected during filtered restore")

	case <-time.After(20 * time.Second):
		require.Fail(t, "Timeout waiting for failpoint callback - scheduler pausing may not have been triggered")
	}

	// Verify tables were restored correctly
	t.Log("verify tables")
	kit.tk.MustQuery("SELECT COUNT(*) FROM test.snapshot_table1").Check(testkit.Rows("3"))
	kit.tk.MustQuery("SELECT COUNT(*) FROM test.log_table1").Check(testkit.Rows("3"))

	// Get final scheduler rules after restore completes
	t.Log("Getting final scheduler rules after restore...")
	finalRules, err := getPDSchedulerRules(t, "127.0.0.1:2379")
	require.NoError(t, err)
	finalKeyRanges := analyzeSchedulerRules(t, finalRules, "FINAL SCHEDULER RULES (after filtered restore)")

	t.Log("Comparing final state with baseline:")
	finalHasChanges := compareKeyRanges(t, baselineKeyRanges, finalKeyRanges)
	require.False(t, finalHasChanges, "Scheduler rules should return to baseline state after restore completion")

	t.Logf("Filtered restore test completed")
}
