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

package aggfuncs_test

import "testing"

// _ AggFunc = (*countPartialWithDistinct4Int)(nil)
// _ AggFunc = (*countPartialWithDistinct4Real)(nil)
// _ AggFunc = (*countPartialWithDistinct4Decimal)(nil)
// _ AggFunc = (*countPartialWithDistinct4Duration)(nil)
// _ AggFunc = (*countPartialWithDistinct4String)(nil)
// _ AggFunc = (*countPartialWithDistinct)(nil)

// _ AggFunc = (*countOriginalWithDistinct4Int)(nil)
// _ AggFunc = (*countOriginalWithDistinct4Real)(nil)
// _ AggFunc = (*countOriginalWithDistinct4Decimal)(nil)
// _ AggFunc = (*countOriginalWithDistinct4Duration)(nil)
// _ AggFunc = (*countOriginalWithDistinct4String)(nil)
// _ AggFunc = (*countOriginalWithDistinct)(nil)
func TestParallelCountDistinct4Int(t *testing.T) {

}
