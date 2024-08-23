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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// PredicateSimplification consolidates different predcicates on a column and its equivalence classes.  Initial out is for
// in-list and not equal list intersection.
type PredicateSimplification struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PredicateSimplification) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PredicateSimplification(opt), planChanged, nil
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PredicateSimplification) Name() string {
	return "predicate_simplification"
}
