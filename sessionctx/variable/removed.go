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

var removedSysVars = map[string]string{
	TiDBEnableAlterPlacement:       "alter placement is now always enabled",
	TiDBEnableGlobalTemporaryTable: "temporary table support is now always enabled",
	TiDBSlowLogMasking:             "use tidb_redact_log instead",
	PlacementChecks:                "placement_checks is removed and use tidb_placement_mode instead",
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
