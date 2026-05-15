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

package compat

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/assert"
)

const (
	metricsSchema         = "metrics_schema"
	exprPushdownBlacklist = "expr_pushdown_blacklist"
	gcDeleteRange         = "gc_delete_range"
	gcDeleteRangeDone     = "gc_delete_range_done"
	optRuleBlacklist      = "opt_rule_blacklist"
	tidb                  = "tidb"
	globalVariables       = "global_variables"
	informationSchema     = "information_schema"
	clusterConfig         = "cluster_config"
	clusterHardware       = "cluster_hardware"
	clusterLoad           = "cluster_load"
	clusterLog            = "cluster_log"
	clusterSystemInfo     = "cluster_systeminfo"
	inspectionResult      = "inspection_result"
	inspectionRules       = "inspection_rules"
	inspectionSummary     = "inspection_summary"
	metricsSummary        = "metrics_summary"
	metricsSummaryByLabel = "metrics_summary_by_label"
	metricsTables         = "metrics_tables"
	tidbHotRegions        = "tidb_hot_regions"
	performanceSchema     = "performance_schema"
	pdProfileAllocs       = "pd_profile_allocs"
	pdProfileBlock        = "pd_profile_block"
	pdProfileCPU          = "pd_profile_cpu"
	pdProfileGoroutines   = "pd_profile_goroutines"
	pdProfileMemory       = "pd_profile_memory"
	pdProfileMutex        = "pd_profile_mutex"
	tidbProfileAllocs     = "tidb_profile_allocs"
	tidbProfileBlock      = "tidb_profile_block"
	tidbProfileCPU        = "tidb_profile_cpu"
	tidbProfileGoroutines = "tidb_profile_goroutines"
	tidbProfileMemory     = "tidb_profile_memory"
	tidbProfileMutex      = "tidb_profile_mutex"
	tikvProfileCPU        = "tikv_profile_cpu"
	tidbGCLeaderDesc      = "tidb_gc_leader_desc"
	restrictedPriv        = "RESTRICTED_"
)

func TestInvisibleSchema(t *testing.T) {
	defer SwitchToSEMForTest(t, V2)()
	assert := assert.New(t)

	assert.True(IsInvisibleSchema(metricsSchema))
	assert.True(IsInvisibleSchema("METRICS_ScHEma"))
	assert.False(IsInvisibleSchema("mysql"))
	assert.False(IsInvisibleSchema(informationSchema))
	assert.False(IsInvisibleSchema("Bogusname"))
}

func TestIsInvisibleTable(t *testing.T) {
	defer SwitchToSEMForTest(t, V2)()
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
	defer SwitchToSEMForTest(t, V2)()
	assert := assert.New(t)

	assert.True(IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"))
	assert.True(IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"))
	assert.True(IsRestrictedPrivilege("BACKUP_ADMIN"))
	assert.False(IsRestrictedPrivilege("CONNECTION_ADMIN"))
	assert.False(IsRestrictedPrivilege("AA"))
}

func TestIsInvisibleStatusVar(t *testing.T) {
	defer SwitchToSEMForTest(t, V2)()
	assert := assert.New(t)

	assert.True(IsInvisibleStatusVar(tidbGCLeaderDesc))
	assert.False(IsInvisibleStatusVar("server_id"))
	assert.False(IsInvisibleStatusVar("ddl_schema_version"))
	assert.False(IsInvisibleStatusVar("Ssl_version"))
}

func TestIsInvisibleSysVar(t *testing.T) {
	defer SwitchToSEMForTest(t, V2)()
	assert := assert.New(t)

	assert.False(IsInvisibleSysVar(vardef.Hostname))                   // changes the value to default, but is not invisible
	assert.False(IsInvisibleSysVar(vardef.TiDBEnableEnhancedSecurity)) // should be able to see the mode is on.
	assert.False(IsInvisibleSysVar(vardef.TiDBAllowRemoveAutoInc))

	assert.True(IsInvisibleSysVar(vardef.TiDBCheckMb4ValueInUTF8))
	assert.True(IsInvisibleSysVar(vardef.TiDBConfig))
	assert.True(IsInvisibleSysVar(vardef.TiDBEnableSlowLog))
	assert.True(IsInvisibleSysVar(vardef.TiDBExpensiveQueryTimeThreshold))
	assert.True(IsInvisibleSysVar(vardef.TiDBForcePriority))
	assert.True(IsInvisibleSysVar(vardef.TiDBGeneralLog))
	assert.True(IsInvisibleSysVar(vardef.TiDBMetricSchemaRangeDuration))
	assert.True(IsInvisibleSysVar(vardef.TiDBMetricSchemaStep))
	assert.True(IsInvisibleSysVar(vardef.TiDBOptWriteRowID))
	assert.True(IsInvisibleSysVar(vardef.TiDBPProfSQLCPU))
	assert.True(IsInvisibleSysVar(vardef.TiDBRecordPlanInSlowLog))
	assert.True(IsInvisibleSysVar(vardef.TiDBSlowQueryFile))
	assert.True(IsInvisibleSysVar(vardef.TiDBSlowLogThreshold))
	assert.True(IsInvisibleSysVar(vardef.TiDBEnableCollectExecutionInfo))
	assert.True(IsInvisibleSysVar(vardef.TiDBMemoryUsageAlarmRatio))
	assert.True(IsInvisibleSysVar(vardef.TiDBEnableTelemetry))
	assert.True(IsInvisibleSysVar(vardef.TiDBRowFormatVersion))
	assert.True(IsInvisibleSysVar(vardef.TiDBRedactLog))
	assert.True(IsInvisibleSysVar(vardef.TiDBTopSQLMaxTimeSeriesCount))
	assert.True(IsInvisibleSysVar(vardef.TiDBTopSQLMaxTimeSeriesCount))
}
