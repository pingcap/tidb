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

package memory_test

import (
	"github.com/pingcap/tidb/pkg/util/memory"
)

// Exported type aliases for unexported types
// Note: These can only be used within this file to create wrapper functions

// removeEntryForTest provides access to MemArbitrator.removeRootPoolEntry
func (m *memory.MemArbitrator) removeEntryForTest(entry *memory.MemArbitrator) bool {
	// This won't work - we can't access unexported methods from outside the package
	// We need a different approach
	return false
}

// The actual solution: these helper files need to be in the same package
// as what they're testing. But since they're in a test subdirectory,
// we need to use reflection or expose the needed functions through the main package.

// FOR NOW: The proper fix is to make these files part of package memory
// but that conflicts with the generated test files.

// ALTERNATIVE: Create export_test.go in the main memory package that exposes
// the needed functions, then call those from here.
