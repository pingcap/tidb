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
// See the License for the specific language governing permissions and
// limitations under the License.

package security

import (
	"os"
	"strings"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
)

const (
	bindInfo              = "bind_info"
	columnsPriv           = "columns_priv"
	db                    = "db"
	defaultRoles          = "default_roles"
	globalPriv            = "global_priv"
	helpTopic             = "help_topic" // TODO: will this cause client problems?
	roleEdges             = "role_edges"
	schemaIndexUsage      = "schema_index_usage"
	statsBuckets          = "stats_buckets"
	statsExtended         = "stats_extended"
	statsFeedback         = "stats_feedback"
	statsHistograms       = "stats_histograms"
	statsMeta             = "stats_meta"
	statsTopN             = "stats_top_n"
	tablesPriv            = "tables_priv"
	user                  = "user"
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
)

var secureVarsList = []string{
	variable.TiDBDDLSlowOprThreshold, // ddl_slow_threshold
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
}

// Enable enables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Enable() {
	variable.SetSysVar(variable.Hostname, variable.DefHostname)
	for _, name := range secureVarsList {
		variable.UnregisterSysVar(name)
	}
	config.GetGlobalConfig().EnableEnhancedSecurity = true
}

// Disable disables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Disable() {
	if hostname, err := os.Hostname(); err != nil {
		variable.SetSysVar(variable.Hostname, hostname)
	}
	for _, name := range secureVarsList {
		variable.RegisterSysVarFromDefaults(name)
	}
	config.GetGlobalConfig().EnableEnhancedSecurity = false
}

// IsEnabled checks if Security Enhanced Mode (SEM) is enabled
func IsEnabled() bool {
	return config.GetGlobalConfig().EnableEnhancedSecurity
}

// IsReadOnlySystemTable returns true if SEM is enabled
// and the tblLowerName needs to be read-only
func IsReadOnlySystemTable(tblLowerName string) bool {
	if !IsEnabled() {
		return false
	}
	switch tblLowerName {
	case bindInfo, columnsPriv, db, defaultRoles, globalPriv, helpTopic,
		roleEdges, schemaIndexUsage, statsBuckets, statsExtended, statsFeedback,
		statsHistograms, statsMeta, statsTopN, tablesPriv, user:
		return true
	}
	return false
}

// IsInvisibleSchema returns true if SEM is enabled
// and the dbName needs to be hidden
func IsInvisibleSchema(dbName string) bool {
	if !IsEnabled() {
		return false
	}
	return strings.EqualFold(dbName, metricsSchema)
}

// IsInvisibleTable returns true if SEM is enabled and the
// table needs to be hidden
func IsInvisibleTable(dbLowerName, tblLowerName string) bool {
	if !IsEnabled() {
		return false
	}
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
