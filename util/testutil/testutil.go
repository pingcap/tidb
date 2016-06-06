// Copyright 2015 PingCAP, Inc.
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

package testutil

import (
	"fmt"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
)

// CompareUnorderedStringSlice compare two string slices.
// If a and b is exactly the same except the order, it returns true.
// In otherwise return false.
func CompareUnorderedStringSlice(a []string, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, i := range a {
		_, ok := m[i]
		if !ok {
			m[i] = 1
		} else {
			m[i]++
		}
	}

	for _, i := range b {
		_, ok := m[i]
		if !ok {
			return false
		}
		m[i]--
		if m[i] == 0 {
			delete(m, i)
		}
	}
	return len(m) == 0
}

// DatumEquals checker.
type datumEqualsChecker struct {
	*check.CheckerInfo
}

// DatumEquals checker verifies that the obtained value is equal to
// the expected value.
// For example:
//     c.Assert(value, DatumEquals, NewDatum(42))
var DatumEquals check.Checker = &datumEqualsChecker{
	&check.CheckerInfo{Name: "DatumEquals", Params: []string{"obtained", "expected"}},
}

func (checker *datumEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()
	paramFirst, ok := params[0].(types.Datum)
	if !ok {
		panic("the first param should be datum")
	}
	paramSecond, ok := params[1].(types.Datum)
	if !ok {
		panic("the second param should be datum")
	}

	res, err := paramFirst.CompareDatum(paramSecond)
	if err != nil {
		panic(err)
	}
	return res == 0, ""
}
