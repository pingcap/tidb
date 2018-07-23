// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"github.com/pingcap/tidb/types"
)

type decimalSet map[types.MyDecimal]struct{}
type float64Set map[float64]struct{}
type stringSet map[string]struct{}

func newDecimalSet() decimalSet {
	return make(map[types.MyDecimal]struct{})
}

func (s decimalSet) exist(val *types.MyDecimal) bool {
	_, ok := s[*val]
	return ok
}

func (s decimalSet) insert(val *types.MyDecimal) {
	s[*val] = struct{}{}
}

func newFloat64Set() float64Set {
	return make(map[float64]struct{})
}

func (s float64Set) exist(val float64) bool {
	_, ok := s[val]
	return ok
}

func (s float64Set) insert(val float64) {
	s[val] = struct{}{}
}

func newStringSet() stringSet {
	return make(map[string]struct{})
}

func (s stringSet) exist(val string) bool {
	_, ok := s[val]
	return ok
}

func (s stringSet) insert(val string) {
	s[val] = struct{}{}
}
