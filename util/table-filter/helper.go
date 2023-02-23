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

package filter

import "reflect"

// reverse replace the contents of a slice with the same elements but in reverse order.
func reverse(items interface{}) {
	n := reflect.ValueOf(items).Len()
	swap := reflect.Swapper(items)
	for i := n/2 - 1; i >= 0; i-- {
		opp := n - 1 - i
		swap(i, opp)
	}
}
