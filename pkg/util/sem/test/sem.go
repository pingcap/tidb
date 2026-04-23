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

package sem_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/assert"
)

func RunInvisibleSchema(t *testing.T) {
	assert := assert.New(t)

	assert.True(sem.IsInvisibleSchema(metadef.MetricSchemaName.L))
	assert.True(sem.IsInvisibleSchema("METRICS_ScHEma"))
	assert.False(sem.IsInvisibleSchema("mysql"))
	assert.False(sem.IsInvisibleSchema(metadef.InformationSchemaName.L))
	assert.False(sem.IsInvisibleSchema("Bogusname"))
}

func RunIsInvisibleTable(t *testing.T) {
	assert := assert.New(t)

	mysqlTbls := []string{sem.ExportedExprPushdownBlacklist, sem.ExportedGCDeleteRange, sem.ExportedGCDeleteRangeDone, sem.ExportedOptRuleBlacklist, sem.ExportedTiDB, sem.ExportedGlobalVariables}
	infoSchemaTbls := []string{sem.ExportedClusterConfig, sem.ExportedClusterHardware, sem.ExportedClusterLoad, sem.ExportedClusterLog, sem.ExportedClusterSystemInfo, sem.ExportedInspectionResult,
		sem.ExportedInspectionRules, sem.ExportedInspectionSummary, sem.ExportedMetricsSummary, sem.ExportedMetricsSummaryByLabel, sem.ExportedMetricsTables, sem.ExportedTiDBHotRegions}
	perfSChemaTbls := []string{sem.ExportedPdProfileAllocs, sem.ExportedPdProfileBlock, sem.ExportedPdProfileCPU, sem.ExportedPdProfileGoroutines, sem.ExportedPdProfileMemory,
		sem.ExportedPdProfileMutex, sem.ExportedTiDBProfileAllocs, sem.ExportedTiDBProfileBlock, sem.ExportedTiDBProfileCPU, sem.ExportedTiDBProfileGoroutines,
		sem.ExportedTiDBProfileMemory, sem.ExportedTiDBProfileMutex, sem.ExportedTiKVProfileCPU}

	for _, tbl := range mysqlTbls {
		assert.True(sem.IsInvisibleTable(mysql.SystemDB, tbl))
	}
	for _, tbl := range infoSchemaTbls {
		assert.True(sem.IsInvisibleTable(metadef.InformationSchemaName.L, tbl))
	}
	for _, tbl := range perfSChemaTbls {
		assert.True(sem.IsInvisibleTable(metadef.PerformanceSchemaName.L, tbl))
	}

	assert.True(sem.IsInvisibleTable(metadef.MetricSchemaName.L, "acdc"))
	assert.True(sem.IsInvisibleTable(metadef.MetricSchemaName.L, "fdsgfd"))
	assert.False(sem.IsInvisibleTable("test", "t1"))
}

func RunIsRestrictedPrivilege(t *testing.T) {
	assert := assert.New(t)

	assert.True(sem.IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"))
	assert.True(sem.IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"))
	assert.False(sem.IsRestrictedPrivilege("CONNECTION_ADMIN"))
	assert.False(sem.IsRestrictedPrivilege("BACKUP_ADMIN"))
	assert.False(sem.IsRestrictedPrivilege("AA"))
}

func RunIsInvisibleStatusVar(t *testing.T) {
	assert := assert.New(t)

	assert.True(sem.IsInvisibleStatusVar(sem.ExportedTiDBGCLeaderDesc))
	assert.False(sem.IsInvisibleStatusVar("server_id"))
	assert.False(sem.IsInvisibleStatusVar("ddl_schema_version"))
	assert.False(sem.IsInvisibleStatusVar("Ssl_version"))
}

func RunIsInvisibleSysVar(t *testing.T) {
	assert := assert.New(t)

	assert.False(sem.IsInvisibleSysVar(vardef.Hostname))                   // changes the value to default, but is not invisible
	assert.False(sem.IsInvisibleSysVar(vardef.TiDBEnableEnhancedSecurity)) // should be able to see the mode is on.
	assert.False(sem.IsInvisibleSysVar(vardef.TiDBAllowRemoveAutoInc))

	assert.True(sem.IsInvisibleSysVar(vardef.TiDBCheckMb4ValueInUTF8))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBConfig))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBEnableSlowLog))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBExpensiveQueryTimeThreshold))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBForcePriority))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBGeneralLog))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBMetricSchemaRangeDuration))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBMetricSchemaStep))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBOptWriteRowID))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBPProfSQLCPU))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBRecordPlanInSlowLog))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBSlowQueryFile))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBSlowLogThreshold))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBEnableCollectExecutionInfo))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBMemoryUsageAlarmRatio))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBEnableTelemetry))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBRowFormatVersion))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBRedactLog))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBTopSQLMaxTimeSeriesCount))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBServiceScope))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBCloudStorageURI))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBStmtSummaryMaxStmtCount))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBServerMemoryLimit))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBServerMemoryLimitGCTrigger))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBInstancePlanCacheMaxMemSize))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBStatsCacheMemQuota))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBMemQuotaBindingCache))
	assert.True(sem.IsInvisibleSysVar(vardef.TiDBSchemaCacheSize))
	assert.True(sem.IsInvisibleSysVar(sem.ExportedTidbAuditRetractLog))
}
