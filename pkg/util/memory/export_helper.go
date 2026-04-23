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

package memory

// Export helper for mega test framework
// This file exports internal symbols needed by test packages

// ExportRootPoolEntry exports rootPoolEntry type for testing
type ExportRootPoolEntry = rootPoolEntry

// ExportArbitrationContext exports ArbitrationContext type for testing
type ExportArbitrationContext = ArbitrationContext

// ExportResourcePool exports ResourcePool type for testing
type ExportResourcePool = ResourcePool

// ExportRemoveRootPoolEntry exports removeRootPoolEntry method for testing
func ExportRemoveRootPoolEntry(m *MemArbitrator, entry *ExportRootPoolEntry) bool {
	return m.removeRootPoolEntry(entry)
}

// ExportRestartEntryByContext exports RestartEntryByContext method for testing
func ExportRestartEntryByContext(m *MemArbitrator, p rootPoolWrap, ctx *ExportArbitrationContext) bool {
	return m.RestartEntryByContext(p, ctx)
}

// ExportNewResourcePool exports NewResourcePool function for testing
func ExportNewResourcePool(
	uid uint64,
	name string,
	limit int64,
	allocAlignSize int64,
	maxUnusedBlocks int64,
	actions PoolActions,
) *ExportResourcePool {
	return NewResourcePool(uid, name, limit, allocAlignSize, maxUnusedBlocks, actions)
}

// ExportRootPoolWrap exports rootPoolWrap type for testing
type ExportRootPoolWrap = rootPoolWrap

// ExportMemArbitrator exports MemArbitrator type for testing
type ExportMemArbitrator = MemArbitrator
