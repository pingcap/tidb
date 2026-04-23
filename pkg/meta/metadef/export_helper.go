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

package metadef

// Export helper for mega test framework
// This file exports internal symbols needed by test packages

// ExportIsMemDB exports IsMemDB for testing
func ExportIsMemDB() any {
	return IsMemDB
}

// ExportIsSystemDB exports IsSystemDB for testing
func ExportIsSystemDB() any {
	return IsSystemDB
}

// ExportIsSystemRelatedDB exports IsSystemRelatedDB for testing
func ExportIsSystemRelatedDB() any {
	return IsSystemRelatedDB
}
