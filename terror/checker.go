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

package terror

import (
	"github.com/pingcap/check"
)

type isError struct{}

func (e *isError) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "IsError takes 2 error arguments"
	}

	// both nil returns true.
	if params[0] == nil && params[1] == nil {
		return true, ""
	}

	b1, ok1 := params[0].(error)
	b2, ok2 := params[1].(error)

	if !(ok1 && ok2) {
		return false, "Arguments to IsError must both be errors"
	}

	return ErrorEqual(b1, b2), ""
}

func (e *isError) Info() *check.CheckerInfo {
	return &check.CheckerInfo{
		Name:   "IsError",
		Params: []string{"error_one", "error_two"},
	}
}

// IsError checker checks whether err1 is the same as err2 in test using gocheck.
//
// For example:
//
//     c.Assert(err1, terror.IsError, err2)
//
var IsError = &isError{}

// IsNotError checker checks whether err1 is not the same as err2 in test using gocheck.
//
// For example:
//
//     c.Assert(err1, terror.IsNotError, err2)
//
var IsNotError = check.Not(IsError)
