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
	"github.com/pingcap/tidb/pkg/config"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/assert"
)

func TestInvisibleSchema(t *testing.T) {
	tidbCfg := config.NewConfig()
	tidbCfg.Security.SEM.RestrictedDatabases = []string{metricsSchema}
	config.StoreGlobalConfig(tidbCfg)

	assert := assert.New(t)

	assert.True(IsInvisibleSchema(metricsSchema))
	assert.True(IsInvisibleSchema("METRICS_ScHEma"))
	assert.False(IsInvisibleSchema("mysql"))
	assert.False(IsInvisibleSchema(informationSchema))
	assert.False(IsInvisibleSchema("Bogusname"))
}

func TestIsInvisibleTable(t *testing.T) {
	tidbCfg := config.NewConfig()
	tidbCfg.Security.SEM.RestrictedTables = []config.RestrictedTable{
		{
			Schema: mysql.SystemDB,
			Name:   exprPushdownBlacklist,
		},
		{
			Schema: mysql.SystemDB,
			Name:   gcDeleteRange,
		},
		{
			Schema: mysql.SystemDB,
			Name:   gcDeleteRangeDone,
		},
		{
			Schema: mysql.SystemDB,
			Name:   optRuleBlacklist,
		},
		{
			Schema: mysql.SystemDB,
			Name:   tidb,
		},
		{
			Schema: mysql.SystemDB,
			Name:   globalVariables,
		},
		{
			Schema: informationSchema,
			Name:   clusterConfig,
		},
		{
			Schema: informationSchema,
			Name:   clusterHardware,
		},
		{
			Schema: informationSchema,
			Name:   clusterLoad,
		},
		{
			Schema: informationSchema,
			Name:   clusterLog,
		},
		{
			Schema: informationSchema,
			Name:   clusterSystemInfo,
		},
		{
			Schema: informationSchema,
			Name:   inspectionResult,
		},
		{
			Schema: informationSchema,
			Name:   inspectionRules,
		},
		{
			Schema: informationSchema,
			Name:   inspectionSummary,
		},
		{
			Schema: informationSchema,
			Name:   metricsSummary,
		},
		{
			Schema: informationSchema,
			Name:   metricsSummaryByLabel,
		},
		{
			Schema: informationSchema,
			Name:   metricsTables,
		},
		{
			Schema: informationSchema,
			Name:   tidbHotRegions,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileAllocs,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileBlock,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileCPU,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileGoroutines,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileMemory,
		},
		{
			Schema: performanceSchema,
			Name:   pdProfileMutex,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileAllocs,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileBlock,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileCPU,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileGoroutines,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileMemory,
		},
		{
			Schema: performanceSchema,
			Name:   tidbProfileMutex,
		},
		{
			Schema: performanceSchema,
			Name:   tikvProfileCPU,
		},
	}
	tidbCfg.Security.SEM.RestrictedDatabases = []string{metricsSchema}
	config.StoreGlobalConfig(tidbCfg)

	assert := assert.New(t)

	mysqlTbls := []string{exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables}
	infoSchemaTbls := []string{clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
		inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions}
	perfSChemaTbls := []string{pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
		pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
		tidbProfileMemory, tidbProfileMutex, tikvProfileCPU}

	for _, tbl := range mysqlTbls {
		assert.True(IsInvisibleTable(mysql.SystemDB, tbl))
	}
	for _, tbl := range infoSchemaTbls {
		assert.True(IsInvisibleTable(informationSchema, tbl))
	}
	for _, tbl := range perfSChemaTbls {
		assert.True(IsInvisibleTable(performanceSchema, tbl))
	}

	assert.True(IsInvisibleTable(metricsSchema, "acdc"))
	assert.True(IsInvisibleTable(metricsSchema, "fdsgfd"))
	assert.False(IsInvisibleTable("test", "t1"))
}

func TestIsRestrictedPrivilege(t *testing.T) {
	assert := assert.New(t)

	assert.True(IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"))
	assert.True(IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"))
	assert.False(IsRestrictedPrivilege("CONNECTION_ADMIN"))
	assert.False(IsRestrictedPrivilege("BACKUP_ADMIN"))
	assert.False(IsRestrictedPrivilege("aa"))
}

func TestGetRestrictedStatusOfStateVariable(t *testing.T) {
	assert := assert.New(t)

	tidbCfg := config.NewConfig()

	tidbCfg.Security.SEM.RestrictedStatus = []config.RestrictedState{
		{
			Name:            "tidb_gc_leader_desc",
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            "server_id",
			RestrictionType: "replace",
			Value:           "xxxx",
		},
	}

	config.StoreGlobalConfig(tidbCfg)

	var restricted bool
	var info *config.RestrictedState

	restricted, info = GetRestrictedStatusOfStateVariable(tidbGCLeaderDesc)
	assert.True(restricted)
	assert.Equal("tidb_gc_leader_desc", info.Name)
	assert.Equal("hidden", info.RestrictionType)

	restricted, info = GetRestrictedStatusOfStateVariable("server_id")
	assert.True(restricted)
	assert.Equal("server_id", info.Name)
	assert.Equal("replace", info.RestrictionType)
	assert.Equal("xxxx", info.Value)

	restricted, info = GetRestrictedStatusOfStateVariable("ddl_schema_version")
	assert.False(restricted)

	restricted, info = GetRestrictedStatusOfStateVariable("Ssl_version")
	assert.False(restricted)
}

func TestIsInvisibleSysVar(t *testing.T) {
	tidbCfg := config.NewConfig()
	tidbCfg.Security.SEM.RestrictedVariables = []config.RestrictedVariable{
		{
			Name:            variable.Hostname,
			RestrictionType: "replace",
			Value:           "localhost",
		},
		{
			Name:            variable.TiDBEnableEnhancedSecurity,
			RestrictionType: "replace",
			Value:           "ON",
		},
		{
			Name:            variable.TiDBAllowRemoveAutoInc,
			RestrictionType: "replace",
			Value:           "True",
		},
		{
			Name:            variable.TiDBCheckMb4ValueInUTF8,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBConfig,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBEnableSlowLog,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBExpensiveQueryTimeThreshold,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBForcePriority,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBGeneralLog,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBMetricSchemaRangeDuration,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBMetricSchemaStep,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBOptWriteRowID,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBPProfSQLCPU,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBRecordPlanInSlowLog,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBSlowQueryFile,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBSlowLogThreshold,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBEnableCollectExecutionInfo,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBMemoryUsageAlarmRatio,
			RestrictionType: "hidden",
			Value:           "",
		},
		// This line is commented out, assuming variable.TiDBEnableTelemetry should be excluded
		{
			Name:            variable.TiDBEnableTelemetry,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBRowFormatVersion,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBRedactLog,
			RestrictionType: "hidden",
			Value:           "",
		},
		{
			Name:            variable.TiDBTopSQLMaxTimeSeriesCount,
			RestrictionType: "hidden",
			Value:           "",
		},
		// Assuming tidbAuditRetractLog is a variable, if it's not, you might need to adjust
		{
			Name:            tidbAuditRetractLog,
			RestrictionType: "hidden",
			Value:           "",
		},
	}

	config.StoreGlobalConfig(tidbCfg)
	assert := assert.New(t)

	assert.False(IsInvisibleSysVar(variable.Hostname))                   // changes the value to default, but is not invisible
	assert.False(IsInvisibleSysVar(variable.TiDBEnableEnhancedSecurity)) // should be able to see the mode is on.
	assert.False(IsInvisibleSysVar(variable.TiDBAllowRemoveAutoInc))

	assert.True(IsInvisibleSysVar(variable.TiDBCheckMb4ValueInUTF8))
	assert.True(IsInvisibleSysVar(variable.TiDBConfig))
	assert.True(IsInvisibleSysVar(variable.TiDBEnableSlowLog))
	assert.True(IsInvisibleSysVar(variable.TiDBExpensiveQueryTimeThreshold))
	assert.True(IsInvisibleSysVar(variable.TiDBForcePriority))
	assert.True(IsInvisibleSysVar(variable.TiDBGeneralLog))
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

func TestIsStaticPermissionRestricted(t *testing.T) {
	tidbCfg := config.NewConfig()
	p := make(map[mysql.PrivilegeType]struct{})
	p[mysql.ConfigPriv] = struct{}{}
	p[mysql.ShutdownPriv] = struct{}{}
	tidbCfg.Security.SEM.RestrictedStaticPrivileges = p
	config.StoreGlobalConfig(tidbCfg)
	assert := assert.New(t)
	assert.True(IsStaticPermissionRestricted(mysql.ConfigPriv))
	assert.False(IsStaticPermissionRestricted(mysql.AlterPriv))
	assert.True(IsStaticPermissionRestricted(mysql.ShutdownPriv))
	assert.False(IsStaticPermissionRestricted(mysql.CreatePriv))
	assert.False(IsStaticPermissionRestricted(mysql.AllPriv))
}
