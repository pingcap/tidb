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

package evaluator

import (
	"github.com/pingcap/tidb/terror"
)

const (
	zeroI64 int64 = 0
	oneI64  int64 = 1
)

// Error instances.
var (
	ErrInvalidOperation = terror.ClassEvaluator.New(CodeInvalidOperation, "invalid operation")
)

// Error codes.
const (
	CodeInvalidOperation terror.ErrCode = 1
)

func boolToInt64(v bool) int64 {
	if v {
		return int64(1)
	}
	return int64(0)
}
