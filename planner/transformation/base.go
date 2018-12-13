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

package transformation

import (
	"github.com/pingcap/tidb/planner/memo"
)

// baseTransform partially implements the cascades.Transformation interface.
// NOTE: All the rules should be stateless, there is only one rule object in
//		 the tidb-server process.
type baseTransform struct {
	pattern *memo.Pattern
}

// GetPattern implements the cascades.Transformation interface.
func (r *baseTransform) GetPattern() *memo.Pattern {
	return r.pattern
}

// Match implements the cascades.Transformation interface.
func (r *baseTransform) Match(expr *memo.ExprIter) bool {
	return true
}
