// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	JoinType     base.JoinType `hash64-equals:"true"`
	Reordered    bool
	StraightJoin bool

	// HintInfo stores the join algorithm hint information specified by client.
	HintInfo            *utilhint.PlanHints
	PreferJoinType      uint
	PreferJoinOrder     bool
	LeftPreferJoinType  uint
	RightPreferJoinType uint

	EqualConditions []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	// NAEQConditions means null aware equal conditions, which is used for null aware semi joins.
	NAEQConditions  []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	LeftConditions  expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	RightConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	OtherConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`

	LeftProperties  [][]*expression.Column
	RightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// FullSchema contains all the columns that the Join can output. It's ordered as [outer schema..., inner schema...].
	// This is useful for natural joins and "using" joins. In these cases, the join key columns from the
	// inner side (or the right side when it's an inner join) will not be in the schema of Join.
	// But upper operators should be able to find those "redundant" columns, and the user also can specifically select
	// those columns, so we put the "redundant" columns here to make them be able to be found.
	//
	// For example:
	// create table t1(a int, b int); create table t2(a int, b int);
	// select * from t1 join t2 using (b);
	// schema of the Join will be [t1.b, t1.a, t2.a]; FullSchema will be [t1.a, t1.b, t2.a, t2.b].
	//
	// We record all columns and keep them ordered is for correctly handling SQLs like
	// select t1.*, t2.* from t1 join t2 using (b);
	// (*PlanBuilder).unfoldWildStar() handles the schema for such case.
	FullSchema *expression.Schema
	FullNames  types.NameSlice

	// EqualCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	EqualCondOutCnt float64

	// allJoinLeaf is used to identify the table where the column is located during constant propagation.
	allJoinLeaf []*expression.Schema
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx base.PlanContext, offset int) *LogicalJoin {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}
