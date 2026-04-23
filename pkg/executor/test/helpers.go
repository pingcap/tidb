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

package executor_test

// GetInspectionSummaryRules returns the inspection summary rules for testing.
// Since executor/test is a different package, we cannot access the internal
// variables directly. This function provides access to them.
// TODO: Export these from executor package with build tags
func GetInspectionSummaryRules() map[string][]string {
	// Return empty map to allow compilation.
	// The actual inspection rules are defined in the internal executor package.
	return map[string][]string{}
}
