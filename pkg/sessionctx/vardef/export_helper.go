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

package vardef

// Export helper for mega test framework
// This file exports internal symbols needed by test packages

// ExportIsReadOnlyVarInNextGen exports IsReadOnlyVarInNextGen for testing
func ExportIsReadOnlyVarInNextGen() any {
	return IsReadOnlyVarInNextGen
}

// ExportTiDBDDLDiskQuota exports TiDBDDLDiskQuota for testing
func ExportTiDBDDLDiskQuota() any {
	return TiDBDDLDiskQuota
}

// ExportTiDBDDLReorgMaxWriteSpeed exports TiDBDDLReorgMaxWriteSpeed for testing
func ExportTiDBDDLReorgMaxWriteSpeed() any {
	return TiDBDDLReorgMaxWriteSpeed
}

// ExportTiDBEnableMDL exports TiDBEnableMDL for testing
func ExportTiDBEnableMDL() any {
	return TiDBEnableMDL
}

// ExportTiDBMaxDistTaskNodes exports TiDBMaxDistTaskNodes for testing
func ExportTiDBMaxDistTaskNodes() any {
	return TiDBMaxDistTaskNodes
}
