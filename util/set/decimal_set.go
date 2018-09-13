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

package set

import (
	"github.com/pingcap/tidb/types"
)

// DecimalSet is a decimal set.
type DecimalSet map[types.MyDecimal]struct{}

// NewDecimalSet builds a decimal set.
func NewDecimalSet() DecimalSet {
	return make(map[types.MyDecimal]struct{})
}

// Exist checks whether `val` exists in `s`.
func (s DecimalSet) Exist(val *types.MyDecimal) bool {
	_, ok := s[*val]
	return ok
}

// Insert inserts `val` into `s`.
func (s DecimalSet) Insert(val *types.MyDecimal) {
	s[*val] = struct{}{}
}
