// Copyright 2026 PingCAP, Inc.
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

// Regression tests for information_schema.slow_query
// compatibility with old (no Binding_digest) and new (Binding_digest +
// arbitrary unknown keys) slow log files, on both reader scan paths — full
// parse and time-filtered locate. Pattern follows TestSlowQuery's crafted-file
// style (slow_query_sql_test.go).

package executor_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"

	"github.com/stretchr/testify/require"
)

// qaCraftedSlowLog writes content to a temp slow log file, points the global
// config and the session read path at it, and returns the testkit.
func qaCraftedSlowLog(t *testing.T, content string) *testkit.TestKit {
	f, err := os.CreateTemp("", "tidb-slow-regress-*.log")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(f.Name()))
	})
	return tk
}

// G1: old-format records (no Binding_digest key) parse cleanly with legacy
// columns intact — pre-upgrade slow log files stay queryable.
func TestQARegressSlowQueryOldFile(t *testing.T) {
	oldRecord := `# Time: 2026-10-09T12:00:00.000000+08:00
# Txn_start_ts: 4501
# Query_time: 0.5
# Digest: 077a87a576e4eb0c0c13b5d2c8b0d6e07d6bdcf0c0a6bd1bd0a19f9e76f75e0f
# Stats: t:1
select * from t where a = 1;
`
	tk := qaCraftedSlowLog(t, oldRecord)
	rows := tk.MustQuery("select query, digest, query_time from information_schema.slow_query").Rows()
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], "select * from t where a = 1")
	require.Contains(t, rows[0][1], "077a87a5")
}

// G2 + G3: new-format records carrying Binding_digest AND arbitrary unknown
// keys parse cleanly; tolerance holds on both scan directions — full parse and
// the time-filtered locate path.
func TestQARegressSlowQueryNewFileUnknownKeys(t *testing.T) {
	newRecord := `# Time: 2026-10-09T12:00:00.000000+08:00
# Txn_start_ts: 4502
# Query_time: 0.6
# Digest: 0809bc0859b70809bc0859b70809bc0859b70809bc0859b70809bc0859b70809
# Plan_digest: 1111111111111111111111111111111111111111111111111111111111111111
# Binding_digest: da38bf216db4da38bf216db4da38bf216db4da38bf216db4da38bf216db4
# Future_unknown_key: some value with spaces
# Keyspace: ks1
select * from t where a in (1);
`
	tk := qaCraftedSlowLog(t, newRecord)
	rows := tk.MustQuery("select query, digest from information_schema.slow_query").Rows()
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], "select * from t where a in (1)")
	// Time-filtered scan (locate path): bound chosen true in any session tz.
	rows = tk.MustQuery(
		"select query from information_schema.slow_query where time > '2026-01-01 00:00:00'").Rows()
	require.Len(t, rows, 1)
}

// G4: expensive query output must NOT carry Binding_digest (the exposure
// surface is the slow log only). Implement with a temp expensive-query file +
// tidb_enable_collect_execution_info once a deterministic expensive-query
// trigger is available in the suite.
func TestQARegressExpensiveQueryIsolated(t *testing.T) {
	t.Skip("P1: pending deterministic expensive-query trigger; asserts expensive query output has no Binding_digest")
}
