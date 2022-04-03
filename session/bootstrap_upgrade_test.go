// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestUpgradeVersion83(t *testing.T) {
	ctx := context.Background()
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	ver, err := session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	statsHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"stats_data", "longblob"},
		{"seq_no", "bigint(64)"},
		{"version", "bigint(64)"},
		{"create_time", "datetime(6)"},
	}
	rStatsHistoryTbl, err := tk.Exec(`desc mysql.stats_history`)
	require.NoError(t, err)
	req := rStatsHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsHistoryTbl.Next(ctx, req))
	require.Equal(t, 5, req.NumRows())
	for i := 0; i < 5; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}
}

func TestUpgradeVersion84(t *testing.T) {
	ctx := context.Background()
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	ver, err := session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	statsHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"modify_count", "bigint(64)"},
		{"count", "bigint(64)"},
		{"version", "bigint(64)"},
		{"create_time", "datetime(6)"},
	}
	rStatsHistoryTbl, err := tk.Exec(`desc mysql.stats_meta_history`)
	require.NoError(t, err)
	req := rStatsHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsHistoryTbl.Next(ctx, req))
	require.Equal(t, 5, req.NumRows())
	for i := 0; i < 5; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}
}
