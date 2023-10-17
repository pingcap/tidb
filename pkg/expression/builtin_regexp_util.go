// Copyright 2023 PingCAP, Inc.
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

package expression

import (
	"regexp"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

// memorized regexp means the constant pattern.
// Sometimes user may input a constant pattern, and it's unnecessary to compile
// the regexp.Regexp each time.
type regexpMemorizedSig struct {
	memorizedRegexp *regexp.Regexp
	memorizedErr    error
}

func (reg *regexpMemorizedSig) isMemorizedRegexpInitialized() bool {
	return !(reg.memorizedRegexp == nil && reg.memorizedErr == nil)
}

func (reg *regexpMemorizedSig) memorize(compile func(string) (*regexp.Regexp, error), pattern string) {
	re, err := compile(pattern)
	reg.memorizedRegexp = re
	reg.memorizedErr = err
}

func releaseBuffers(bf *baseBuiltinFunc, params []*funcParam) {
	for _, pa := range params {
		if pa.getCol() != nil {
			bf.bufAllocator.put(pa.getCol())
		}
	}
}

func getBuffers(params []*funcParam) []*chunk.Column {
	buffers := make([]*chunk.Column, 0, 6)
	for _, pa := range params {
		if pa.getCol() != nil {
			buffers = append(buffers, pa.getCol())
		}
	}
	return buffers
}

func isResultNull(columns []*chunk.Column, i int) bool {
	for _, col := range columns {
		if col.IsNull(i) {
			return true
		}
	}
	return false
}

func fillNullStringIntoResult(result *chunk.Column, num int) {
	result.ReserveString(num)
	for i := 0; i < num; i++ {
		result.AppendNull()
	}
}

// check if this is a valid position argument when position is out of range
func checkOutRangePos(strLen int, pos int64) bool {
	// false condition:
	//  1. non-empty string
	//  2. empty string and pos != 1
	return strLen != 0 || pos != 1
}
