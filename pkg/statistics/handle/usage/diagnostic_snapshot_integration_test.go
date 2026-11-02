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

package usage_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/diagnosticlog"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticIndexUsageSQLSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table diagnostic_usage (id int primary key, a int, key idx_a(a))")
	tk.MustExec("insert into diagnostic_usage values (1,10),(2,20),(3,30)")
	tk.MustExec("analyze table diagnostic_usage")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("diagnostic_usage"))
	require.NoError(t, err)
	tableID, indexID := table.Meta().ID, table.Meta().Indices[0].ID
	tk.MustQuery("select a from diagnostic_usage force index(idx_a) where a >= 10 order by a").Check(testkit.Rows("10", "20", "30"))
	tk.MustQuery("select a from diagnostic_usage force index(idx_a) where a >= 20 order by a").Check(testkit.Rows("20", "30"))
	require.Eventually(t, func() bool {
		tk.Session().ReportUsageStats()
		return dom.StatsHandle().GetIndexUsage(tableID, indexID).QueryTotal == 2
	}, 5*time.Second, time.Millisecond)
	sample := dom.StatsHandle().GetIndexUsage(tableID, indexID)
	require.Equal(t, uint64(5), sample.RowAccessTotal)
	directory := t.TempDir()
	publisher := diagnosticlog.NewPublisher(directory)
	at := time.Date(2026, 10, 23, 2, 0, 0, 0, time.UTC)
	file, err := publisher.Publish(at, dom.StatsHandle().WriteIndexUsageSnapshot)
	require.NoError(t, err)
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	found := false
	for _, line := range lines[:len(lines)-1] {
		var row struct {
			TableID        int64  `json:"table_id"`
			IndexID        int64  `json:"index_id"`
			QueryTotal     uint64 `json:"query_total"`
			RowAccessTotal uint64 `json:"row_access_total"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &row))
		if row.TableID == tableID && row.IndexID == indexID {
			found = true
			require.Equal(t, uint64(2), row.QueryTotal)
			require.Equal(t, uint64(5), row.RowAccessTotal)
		}
	}
	require.True(t, found)
	require.Equal(t, sample, dom.StatsHandle().GetIndexUsage(tableID, indexID), "export must preserve cumulative counters")
	// Persist an optional SQL-produced artifact alongside synthetic edge cases.
	if root := os.Getenv("DIAGNOSTIC_FIXTURE_DIR"); root != "" {
		_, err = diagnosticlog.NewPublisher(filepath.Join(root, "sql")).Publish(at, dom.StatsHandle().WriteIndexUsageSnapshot)
		require.NoError(t, err)
	}
}
