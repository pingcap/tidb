// Copyright 2023 PingCAP, Inc.
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

// ExportedExprPushdownBlacklist is exported version of exprPushdownBlacklist constant
const ExportedExprPushdownBlacklist = exprPushdownBlacklist

// ExportedGCDeleteRange is exported version of gcDeleteRange constant
const ExportedGCDeleteRange = gcDeleteRange

// ExportedGCDeleteRangeDone is exported version of gcDeleteRangeDone constant
const ExportedGCDeleteRangeDone = gcDeleteRangeDone

// ExportedOptRuleBlacklist is exported version of optRuleBlacklist constant
const ExportedOptRuleBlacklist = optRuleBlacklist

// ExportedTiDB is exported version of tidb constant
const ExportedTiDB = tidb

// ExportedGlobalVariables is exported version of globalVariables constant
const ExportedGlobalVariables = globalVariables

// ExportedClusterConfig is exported version of clusterConfig constant
const ExportedClusterConfig = clusterConfig

// ExportedClusterHardware is exported version of clusterHardware constant
const ExportedClusterHardware = clusterHardware

// ExportedClusterLoad is exported version of clusterLoad constant
const ExportedClusterLoad = clusterLoad

// ExportedClusterLog is exported version of clusterLog constant
const ExportedClusterLog = clusterLog

// ExportedClusterSystemInfo is exported version of clusterSystemInfo constant
const ExportedClusterSystemInfo = clusterSystemInfo

// ExportedInspectionResult is exported version of inspectionResult constant
const ExportedInspectionResult = inspectionResult

// ExportedInspectionRules is exported version of inspectionRules constant
const ExportedInspectionRules = inspectionRules

// ExportedInspectionSummary is exported version of inspectionSummary constant
const ExportedInspectionSummary = inspectionSummary

// ExportedMetricsSummary is exported version of metricsSummary constant
const ExportedMetricsSummary = metricsSummary

// ExportedMetricsSummaryByLabel is exported version of metricsSummaryByLabel constant
const ExportedMetricsSummaryByLabel = metricsSummaryByLabel

// ExportedMetricsTables is exported version of metricsTables constant
const ExportedMetricsTables = metricsTables

// ExportedTiDBHotRegions is exported version of tidbHotRegions constant
const ExportedTiDBHotRegions = tidbHotRegions

// ExportedPdProfileAllocs is exported version of pdProfileAllocs constant
const ExportedPdProfileAllocs = pdProfileAllocs

// ExportedPdProfileBlock is exported version of pdProfileBlock constant
const ExportedPdProfileBlock = pdProfileBlock

// ExportedPdProfileCPU is exported version of pdProfileCPU constant
const ExportedPdProfileCPU = pdProfileCPU

// ExportedPdProfileGoroutines is exported version of pdProfileGoroutines constant
const ExportedPdProfileGoroutines = pdProfileGoroutines

// ExportedPdProfileMemory is exported version of pdProfileMemory constant
const ExportedPdProfileMemory = pdProfileMemory

// ExportedPdProfileMutex is exported version of pdProfileMutex constant
const ExportedPdProfileMutex = pdProfileMutex

// ExportedTiDBProfileAllocs is exported version of tidbProfileAllocs constant
const ExportedTiDBProfileAllocs = tidbProfileAllocs

// ExportedTiDBProfileBlock is exported version of tidbProfileBlock constant
const ExportedTiDBProfileBlock = tidbProfileBlock

// ExportedTiDBProfileCPU is exported version of tidbProfileCPU constant
const ExportedTiDBProfileCPU = tidbProfileCPU

// ExportedTiDBProfileGoroutines is exported version of tidbProfileGoroutines constant
const ExportedTiDBProfileGoroutines = tidbProfileGoroutines

// ExportedTiDBProfileMemory is exported version of tidbProfileMemory constant
const ExportedTiDBProfileMemory = tidbProfileMemory

// ExportedTiDBProfileMutex is exported version of tidbProfileMutex constant
const ExportedTiDBProfileMutex = tidbProfileMutex

// ExportedTiKVProfileCPU is exported version of tikvProfileCPU constant
const ExportedTiKVProfileCPU = tikvProfileCPU

// ExportedTiDBGCLeaderDesc is exported version of tidbGCLeaderDesc constant
const ExportedTiDBGCLeaderDesc = tidbGCLeaderDesc

// ExportedTidbAuditRetractLog is exported version of tidbAuditRetractLog constant
const ExportedTidbAuditRetractLog = tidbAuditRetractLog

// ExportedIsInvisibleSchema is exported version of IsInvisibleSchema
func ExportedIsInvisibleSchema(dbName string) bool {
	return IsInvisibleSchema(dbName)
}
