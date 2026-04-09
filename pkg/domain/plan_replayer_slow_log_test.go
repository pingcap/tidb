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

package domain_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

// TestPlanReplayerInternalQuery verifies that queries from plan replayer are marked as internal.
// This test checks both slow log (Is_internal field) and statement summary filtering.
func TestPlanReplayerInternalQuery(t *testing.T) {
	// Setup slow log file
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	slowLogFile := f.Name()
	require.NoError(t, f.Close())
	defer os.Remove(slowLogFile)

	// Configure slow log
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = slowLogFile
	config.StoreGlobalConfig(&newCfg)
	defer config.StoreGlobalConfig(originCfg)

	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Enable slow log with threshold = 0 to capture all queries
	tk.MustExec("set global tidb_slow_log_threshold = 0")
	tk.MustExec("set global tidb_enable_slow_log = 1")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", slowLogFile))

	// Configure statement summary to NOT record internal queries
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 3600")
	tk.MustExec("set global tidb_stmt_summary_internal_query = 0")

	// Setup test data
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('test_digest', 'test_plan')")

	// Trigger plan replayer collection
	prHandle := dom.GetPlanReplayerHandle()
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)

	// Wait for slow log to be written
	time.Sleep(500 * time.Millisecond)

	// Check slow log for Is_internal field
	slowLogContent, err := os.ReadFile(slowLogFile)
	if err == nil && len(slowLogContent) > 0 {
		slowLogStr := string(slowLogContent)
		lines := strings.Split(slowLogStr, "\n")

		for i, line := range lines {
			if strings.Contains(line, "select sql_digest, plan_digest from mysql.plan_replayer_task") && strings.Contains(strings.ToLower(line), "select") {
				t.Logf("Found matching line at index %d", i)
				// Found the SELECT query, look for Is_internal field
				// Find the start of this slow log entry: go backward until we find a line not starting with '#'
				startIdx := i - 1
				for startIdx > 0 && strings.HasPrefix(lines[startIdx], "#") {
					startIdx--
				}
				// Now startIdx points to the line before the first '#' line (the SQL query line)
				// Search for Is_internal field from startIdx+1 to i+10
				for j := startIdx + 1; j < len(lines) && j <= i; j++ {
					if strings.HasPrefix(lines[j], "# Is_internal:") {
						isInternal := strings.TrimSpace(strings.TrimPrefix(lines[j], "# Is_internal:"))
						require.Equal(t, "true", isInternal, "plan_replayer query should be marked as internal in slow log")
						// break
					}
				}
				break
			}
		}
	}

	// Check statement summary - internal queries should NOT appear
	result := tk.MustQuery(`select count(*) from information_schema.statements_summary
		where digest_text like '%plan_replayer_task%'
		and digest_text not like '%statements_summary%'
		and digest_text not like '%delete from%'
		and digest_text not like '%insert into%'`).Rows()
	count := result[0][0].(string)
	require.Equal(t, "0", count, "Internal plan_replayer queries should not appear in statement_summary when internal query recording is disabled")

	// Cleanup
	tk.MustExec("delete from mysql.plan_replayer_task")
}
