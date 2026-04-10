// Copyright 2026 PingCAP, Inc.
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

package arena

// Exported types for testing

// ExportedAllocator exports Allocator for testing
type ExportedAllocator = Allocator

// ExportedNewAllocator creates a new Allocator for testing
func ExportedNewAllocator(capacity int) *SimpleAllocator {
	return NewAllocator(capacity)
}

// ExportedStdAllocator gets the StdAllocator for testing
func ExportedStdAllocator() Allocator {
	return StdAllocator
}

// ExportedGetSimpleAllocatorOff gets the off field from a SimpleAllocator for testing
func ExportedGetSimpleAllocatorOff(a *SimpleAllocator) int {
	return a.off
}

// ExportedGetSimpleAllocatorArena gets the arena field from a SimpleAllocator for testing
func ExportedGetSimpleAllocatorArena(a *SimpleAllocator) []byte {
	return a.arena
}
