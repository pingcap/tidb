// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadef

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsReservedID(t *testing.T) {
	require.True(t, IsReservedID(ReservedGlobalIDUpperBound))
	require.True(t, IsReservedID(ReservedGlobalIDLowerBound+1))
	require.False(t, IsReservedID(ReservedGlobalIDLowerBound))
	require.False(t, IsReservedID(123))
}

func TestPrivilegeTableDefinitionsIncludeOperateView(t *testing.T) {
	require.Contains(t, CreateUserTable, "Operate_view_priv")
	require.Contains(t, CreateDBTable, "Operate_view_priv")
	require.Contains(t, CreateTablesPrivTable, "'Operate View'")
}

func TestMaterializedViewSystemTableDefinitions(t *testing.T) {
	tableDefinitions := []struct {
		id     int64
		offset int64
		name   string
		sql    string
	}{
		{TiDBMViewRefreshInfoTableID, 63, "tidb_mview_refresh_info", CreateTiDBMViewRefreshInfoTable},
		{TiDBMLogPurgeInfoTableID, 64, "tidb_mlog_purge_info", CreateTiDBMLogPurgeInfoTable},
		{TiDBMViewRefreshHistTableID, 65, "tidb_mview_refresh_hist", CreateTiDBMViewRefreshHistTable},
		{TiDBMViewRefreshAlertTableID, 66, "tidb_mview_refresh_alert", CreateTiDBMViewRefreshAlertTable},
		{TiDBMLogPurgeHistTableID, 67, "tidb_mlog_purge_hist", CreateTiDBMLogPurgeHistTable},
	}
	for _, table := range tableDefinitions {
		require.Equal(t, ReservedGlobalIDUpperBound-table.offset, table.id)
		require.Contains(t, table.sql, "CREATE TABLE IF NOT EXISTS mysql."+table.name+" (")
		require.Contains(t, table.sql, "PRIMARY KEY(")
	}
	require.Contains(t, CreateTiDBMViewRefreshHistTable, "REFRESH_COMMIT_TSO bigint unsigned")
	require.Contains(t, CreateTiDBMLogPurgeHistTable, "PURGE_CUTOFF_TSO bigint unsigned")
}
