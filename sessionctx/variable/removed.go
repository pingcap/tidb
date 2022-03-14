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

package variable

// Removed sysvars is a way of removing sysvars, while allowing limited
// Parse-but-ignore support in SET context, and a more specific error in
// SELECT @@varname context.
//
// This helps ensure some compatibility for applications while being
// careful not to return dummy data.

const (
	tiDBEnableAlterPlacement       = "tidb_enable_alter_placement"
	tiDBMemQuotaHashJoin           = "tidb_mem_quota_hashjoin"
	tiDBMemQuotaMergeJoin          = "tidb_mem_quota_mergejoin"
	tiDBMemQuotaSort               = "tidb_mem_quota_sort"
	tiDBMemQuotaTopn               = "tidb_mem_quota_topn"
	tiDBMemQuotaIndexLookupReader  = "tidb_mem_quota_indexlookupreader"
	tiDBMemQuotaIndexLookupJoin    = "tidb_mem_quota_indexlookupjoin"
	tiDBEnableGlobalTemporaryTable = "tidb_enable_global_temporary_table"
	tiDBSlowLogMasking             = "tidb_slow_log_masking"
	placementChecks                = "placement_checks"
	tiDBEnableStreaming            = "tidb_enable_streaming"
	tiDBOptBCJ                     = "tidb_opt_broadcast_join"
)

var removedSysVars = map[string]string{
	tiDBEnableAlterPlacement:       "alter placement is now always enabled",
	tiDBEnableGlobalTemporaryTable: "temporary table support is now always enabled",
	tiDBSlowLogMasking:             "use tidb_redact_log instead",
	placementChecks:                "placement_checks is removed and use tidb_placement_mode instead",
	tiDBMemQuotaHashJoin:           "use tidb_mem_quota_query instead",
	tiDBMemQuotaMergeJoin:          "use tidb_mem_quota_query instead",
	tiDBMemQuotaSort:               "use tidb_mem_quota_query instead",
	tiDBMemQuotaTopn:               "use tidb_mem_quota_query instead",
	tiDBMemQuotaIndexLookupReader:  "use tidb_mem_quota_query instead",
	tiDBMemQuotaIndexLookupJoin:    "use tidb_mem_quota_query instead",
	tiDBEnableStreaming:            "streaming is no longer supported",
	tiDBOptBCJ:                     "tidb_opt_broadcast_join is removed and use tidb_allow_mpp instead",
}

// IsRemovedSysVar returns true if the sysvar has been removed
func IsRemovedSysVar(varName string) bool {
	_, ok := removedSysVars[varName]
	return ok
}

// CheckSysVarIsRemoved returns an error if the sysvar has been removed
func CheckSysVarIsRemoved(varName string) error {
	if reason, ok := removedSysVars[varName]; ok {
		return ErrVariableNoLongerSupported.GenWithStackByArgs(varName, reason)
	}
	return nil
}
