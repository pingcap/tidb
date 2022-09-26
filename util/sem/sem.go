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
	"os"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
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
	tidbAuditRetractLog   = "tidb_audit_redact_log" // sysvar installed by a plugin
)

var (
	semEnabled int32
)

// Enable enables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Enable() {
	atomic.StoreInt32(&semEnabled, 1)
	variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.On)
	variable.SetSysVar(variable.Hostname, variable.DefHostname)
	// write to log so users understand why some operations are weird.
	logutil.BgLogger().Info("tidb-server is operating with security enhanced mode (SEM) enabled")
}

// Disable disables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Disable() {
	atomic.StoreInt32(&semEnabled, 0)
	variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.Off)
	if hostname, err := os.Hostname(); err == nil {
		variable.SetSysVar(variable.Hostname, hostname)
	}
}

// IsEnabled checks if Security Enhanced Mode (SEM) is enabled
func IsEnabled() bool {
	return atomic.LoadInt32(&semEnabled) == 1
}

// IsInvisibleSchema returns true if the dbName needs to be hidden
// when sem is enabled.
func IsInvisibleSchema(dbName string) bool {
	return strings.EqualFold(dbName, metricsSchema)
}

// IsInvisibleTable returns true if the  table needs to be hidden
// when sem is enabled.
func IsInvisibleTable(dbLowerName, tblLowerName string) bool {
	switch dbLowerName {
	case mysql.SystemDB:
		switch tblLowerName {
		case exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables:
			return true
		}
	case informationSchema:
		switch tblLowerName {
		case clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
			inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions:
			return true
		}
	case performanceSchema:
		switch tblLowerName {
		case pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
			pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
			tidbProfileMemory, tidbProfileMutex, tikvProfileCPU:
			return true
		}
	case metricsSchema:
		return true
	}
	return false
}

// IsInvisibleStatusVar returns true if the status var needs to be hidden
func IsInvisibleStatusVar(varName string) bool {
	return varName == tidbGCLeaderDesc
}

// IsInvisibleSysVar returns true if the sysvar needs to be hidden
func IsInvisibleSysVar(varNameInLower string) bool {
	switch varNameInLower {
	case variable.TiDBDDLSlowOprThreshold, // ddl_slow_threshold
		variable.TiDBAllowRemoveAutoInc,
		variable.TiDBCheckMb4ValueInUTF8,
		variable.TiDBConfig,
		variable.TiDBEnableSlowLog,
		variable.TiDBEnableTelemetry,
		variable.TiDBExpensiveQueryTimeThreshold,
		variable.TiDBForcePriority,
		variable.TiDBGeneralLog,
		variable.TiDBMetricSchemaRangeDuration,
		variable.TiDBMetricSchemaStep,
		variable.TiDBOptWriteRowID,
		variable.TiDBPProfSQLCPU,
		variable.TiDBRecordPlanInSlowLog,
		variable.TiDBRowFormatVersion,
		variable.TiDBSlowQueryFile,
		variable.TiDBSlowLogThreshold,
		variable.TiDBEnableCollectExecutionInfo,
		variable.TiDBMemoryUsageAlarmRatio,
		variable.TiDBRedactLog,
		variable.TiDBRestrictedReadOnly,
		variable.TiDBTopSQLMaxTimeSeriesCount,
		variable.TiDBTopSQLMaxMetaCount,
		tidbAuditRetractLog:
		return true
	}
	return false
}

// IsRestrictedPrivilege returns true if the privilege shuld not be satisfied by SUPER
// As most dynamic privileges are.
func IsRestrictedPrivilege(privNameInUpper string) bool {
	if len(privNameInUpper) < 12 {
		return false
	}
	return privNameInUpper[:11] == restrictedPriv
}
