// Copyright 2021 PingCAP, Inc.
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

package sem

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestInvisibleSchema(t *testing.T) {
	require.True(t, IsInvisibleSchema(metricsSchema))
	require.True(t, IsInvisibleSchema("METRICS_ScHEma"))
	require.False(t, IsInvisibleSchema("mysql"))
	require.False(t, IsInvisibleSchema(informationSchema))
	require.False(t, IsInvisibleSchema("Bogusname"))
}

func TestIsInvisibleTable(t *testing.T) {
	mysqlTbls := []string{exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables}
	infoSchemaTbls := []string{clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
		inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions}
	perfSChemaTbls := []string{pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
		pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
		tidbProfileMemory, tidbProfileMutex, tikvProfileCPU}

	for _, tbl := range mysqlTbls {
		require.True(t, IsInvisibleTable(mysql.SystemDB, tbl))
	}
	for _, tbl := range infoSchemaTbls {
		require.True(t, IsInvisibleTable(informationSchema, tbl))
	}
	for _, tbl := range perfSChemaTbls {
		require.True(t, IsInvisibleTable(performanceSchema, tbl))
	}

	require.True(t, IsInvisibleTable(metricsSchema, "acdc"))
	require.True(t, IsInvisibleTable(metricsSchema, "fdsgfd"))
	require.False(t, IsInvisibleTable("test", "t1"))
}

func TestIsRestrictedPrivilege(t *testing.T) {
	require.True(t, IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"))
	require.True(t, IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"))
	require.False(t, IsRestrictedPrivilege("CONNECTION_ADMIN"))
	require.False(t, IsRestrictedPrivilege("BACKUP_ADMIN"))
	require.False(t, IsRestrictedPrivilege("aa"))
}

func TestIsInvisibleStatusVar(t *testing.T) {
	require.True(t, IsInvisibleStatusVar(tidbGCLeaderDesc))
	require.False(t, IsInvisibleStatusVar("server_id"))
	require.False(t, IsInvisibleStatusVar("ddl_schema_version"))
	require.False(t, IsInvisibleStatusVar("Ssl_version"))
}

func TestIsInvisibleSysVar(t *testing.T) {
	require.False(t, IsInvisibleSysVar(variable.Hostname))                   // changes the value to default, but is not invisible
	require.False(t, IsInvisibleSysVar(variable.TiDBEnableEnhancedSecurity)) // should be able to see the mode is on.
	require.False(t, IsInvisibleSysVar(variable.TiDBAllowRemoveAutoInc))

	require.True(t, IsInvisibleSysVar(variable.TiDBCheckMb4ValueInUTF8))
	require.True(t, IsInvisibleSysVar(variable.TiDBConfig))
	require.True(t, IsInvisibleSysVar(variable.TiDBEnableSlowLog))
	require.True(t, IsInvisibleSysVar(variable.TiDBExpensiveQueryTimeThreshold))
	require.True(t, IsInvisibleSysVar(variable.TiDBForcePriority))
	require.True(t, IsInvisibleSysVar(variable.TiDBGeneralLog))
	require.True(t, IsInvisibleSysVar(variable.TiDBMetricSchemaRangeDuration))
	require.True(t, IsInvisibleSysVar(variable.TiDBMetricSchemaStep))
	require.True(t, IsInvisibleSysVar(variable.TiDBOptWriteRowID))
	require.True(t, IsInvisibleSysVar(variable.TiDBPProfSQLCPU))
	require.True(t, IsInvisibleSysVar(variable.TiDBRecordPlanInSlowLog))
	require.True(t, IsInvisibleSysVar(variable.TiDBSlowQueryFile))
	require.True(t, IsInvisibleSysVar(variable.TiDBSlowLogThreshold))
	require.True(t, IsInvisibleSysVar(variable.TiDBEnableCollectExecutionInfo))
	require.True(t, IsInvisibleSysVar(variable.TiDBMemoryUsageAlarmRatio))
	require.True(t, IsInvisibleSysVar(variable.TiDBEnableTelemetry))
	require.True(t, IsInvisibleSysVar(variable.TiDBRowFormatVersion))
	require.True(t, IsInvisibleSysVar(variable.TiDBRedactLog))
	require.True(t, IsInvisibleSysVar(variable.TiDBTopSQLMaxTimeSeriesCount))
	require.True(t, IsInvisibleSysVar(variable.TiDBTopSQLMaxTimeSeriesCount))
	require.True(t, IsInvisibleSysVar(tidbAuditRetractLog))
}
