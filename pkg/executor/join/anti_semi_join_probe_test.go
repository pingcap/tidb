// Copyright 2024 PingCAP, Inc.
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

package join

import "testing"

func testAntiSemiJoin(t *testing.T, rightAsBuildSide bool, hasOtherCondition bool, hasDuplicateKey bool) {
	testSemiOrAntiSemiJoin(t, rightAsBuildSide, hasOtherCondition, hasDuplicateKey, true)
}

func TestAntiSemiJoinBasic(t *testing.T) {
	testAntiSemiJoin(t, false, false, false) // Left side build without other condition
	testAntiSemiJoin(t, false, true, false)  // Left side build with other condition
	testAntiSemiJoin(t, true, false, false)  // Right side build without other condition
	testAntiSemiJoin(t, true, true, false)   // Right side build with other condition
}

func TestAntiSemiJoinDuplicateKeys(t *testing.T) {
	testAntiSemiJoin(t, true, true, true)  // Right side build with other condition
	testAntiSemiJoin(t, false, true, true) // Left side build with other condition
}
