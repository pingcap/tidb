// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package t

import (
	"github.com/pingcap/tidb/pkg/parser/emptynil"
)

// TestAssertEmptyTest is a test function	for emptynil linter
func TestAssertEmptyTest() bool {
	var arr []int
	var m map[int]int

	if arr == nil { // want `should use len\(x\) == 0 to check whether a slice or map is empty. If you indeed need to check whether it is nil, use emptynil.IsNilSlice\(x\) or emptynil.IsNilMap\(x\).`
		return true
	}

	if nil == arr { // want `should use len\(x\) == 0 to check whether a slice or map is empty. If you indeed need to check whether it is nil, use emptynil.IsNilSlice\(x\) or emptynil.IsNilMap\(x\).`
		return true
	}

	if arr != nil { // want `should use len\(x\) > 0 to check whether a slice or map is not empty. If you indeed need to check whether it is nil, use !emptynil.IsNilSlice\(x\) or !emptynil.IsNilMap\(x\).`
		return true
	}

	if nil != arr { // want `should use len\(x\) > 0 to check whether a slice or map is not empty. If you indeed need to check whether it is nil, use !emptynil.IsNilSlice\(x\) or !emptynil.IsNilMap\(x\).`
		return true
	}

	if m == nil { // want `should use len\(x\) == 0 to check whether a slice or map is empty. If you indeed need to check whether it is nil, use emptynil.IsNilSlice\(x\) or emptynil.IsNilMap\(x\).`
		return true
	}

	if nil == m { // want `should use len\(x\) == 0 to check whether a slice or map is empty. If you indeed need to check whether it is nil, use emptynil.IsNilSlice\(x\) or emptynil.IsNilMap\(x\).`
		return true
	}

	if m != nil { // want `should use len\(x\) > 0 to check whether a slice or map is not empty. If you indeed need to check whether it is nil, use !emptynil.IsNilSlice\(x\) or !emptynil.IsNilMap\(x\).`
		return true
	}

	if nil != m { // want `should use len\(x\) > 0 to check whether a slice or map is not empty. If you indeed need to check whether it is nil, use !emptynil.IsNilSlice\(x\) or !emptynil.IsNilMap\(x\).`
		return true
	}

	// this is allowed
	if emptynil.IsNilSlice(arr) || emptynil.IsNilMap(m) {
		return true
	}

	return false
}
