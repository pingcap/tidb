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
	"github.com/stretchr/testify/assert"
)

func TestInvisibleSchema(t *testing.T) {
	assert := assert.New(t)

	require.True(IsInvisibleSchema(metricsSchema))
	require.True(IsInvisibleSchema("METRICS_ScHEma"))
	require.False(IsInvisibleSchema("mysql"))
	require.False(IsInvisibleSchema(informationSchema))
	require.False(IsInvisibleSchema("Bogusname"))
}

func TestIsInvisibleTable(t *testing.T) {
	assert := assert.New(t)

	mysqlTbls := []string{exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables}
	infoSchemaTbls := []string{clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
		inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions}
	perfSChemaTbls := []string{pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
		pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
		tidbProfileMemory, tidbProfileMutex, tikvProfileCPU}

	for _, tbl := range mysqlTbls {
		require.True(IsInvisibleTable(mysql.SystemDB, tbl))
	}
	for _, tbl := range infoSchemaTbls {
		require.True(IsInvisibleTable(informationSchema, tbl))
	}
	for _, tbl := range perfSChemaTbls {
		require.True(IsInvisibleTable(performanceSchema, tbl))
	}

	require.True(IsInvisibleTable(metricsSchema, "acdc"))
	require.True(IsInvisibleTable(metricsSchema, "fdsgfd"))
	require.False(IsInvisibleTable("test", "t1"))
}

func TestIsRestrictedPrivilege(t *testing.T) {
	assert := assert.New(t)

	require.True(IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"))
	require.True(IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"))
	require.False(IsRestrictedPrivilege("CONNECTION_ADMIN"))
	require.False(IsRestrictedPrivilege("BACKUP_ADMIN"))
	require.False(IsRestrictedPrivilege("aa"))
}

func TestIsInvisibleStatusVar(t *testing.T) {
	assert := assert.New(t)

	require.True(IsInvisibleStatusVar(tidbGCLeaderDesc))
	require.False(IsInvisibleStatusVar("server_id"))
	require.False(IsInvisibleStatusVar("ddl_schema_version"))
	require.False(IsInvisibleStatusVar("Ssl_version"))
}

func TestIsInvisibleSysVar(t *testing.T) {
	assert := assert.New(t)

	require.False(IsInvisibleSysVar(variable.Hostname))                   // changes the value to default, but is not invisible
	require.False(IsInvisibleSysVar(variable.TiDBEnableEnhancedSecurity)) // should be able to see the mode is on.
	require.False(IsInvisibleSysVar(variable.TiDBAllowRemoveAutoInc))

	require.True(IsInvisibleSysVar(variable.TiDBCheckMb4ValueInUTF8))
	require.True(IsInvisibleSysVar(variable.TiDBConfig))
	require.True(IsInvisibleSysVar(variable.TiDBEnableSlowLog))
	require.True(IsInvisibleSysVar(variable.TiDBExpensiveQueryTimeThreshold))
	require.True(IsInvisibleSysVar(variable.TiDBForcePriority))
	require.True(IsInvisibleSysVar(variable.TiDBGeneralLog))
	assert.True(IsInvisibleSysVar(variable.TiDBMetricSchemaRangeDuration))
	assert.True(IsInvisibleSysVar(variable.TiDBMetricSchemaStep))
	assert.True(IsInvisibleSysVar(variable.TiDBOptWriteRowID))
	assert.True(IsInvisibleSysVar(variable.TiDBPProfSQLCPU))
	assert.True(IsInvisibleSysVar(variable.TiDBRecordPlanInSlowLog))
	assert.True(IsInvisibleSysVar(variable.TiDBSlowQueryFile))
	assert.True(IsInvisibleSysVar(variable.TiDBSlowLogThreshold))
	assert.True(IsInvisibleSysVar(variable.TiDBEnableCollectExecutionInfo))
	assert.True(IsInvisibleSysVar(variable.TiDBMemoryUsageAlarmRatio))
	assert.True(IsInvisibleSysVar(variable.TiDBEnableTelemetry))
	assert.True(IsInvisibleSysVar(variable.TiDBRowFormatVersion))
	assert.True(IsInvisibleSysVar(variable.TiDBRedactLog))
	assert.True(IsInvisibleSysVar(variable.TiDBTopSQLMaxTimeSeriesCount))
	assert.True(IsInvisibleSysVar(variable.TiDBTopSQLMaxTimeSeriesCount))
	assert.True(IsInvisibleSysVar(tidbAuditRetractLog))
}
