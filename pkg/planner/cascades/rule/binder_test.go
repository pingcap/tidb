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

package rule

import (
	"bufio"
	"bytes"
	"testing"

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/stretchr/testify/require"
)

func TestBinderSuccess(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	mm := memo.NewMemo(ctx)
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	// iter memo.groups to assert group ids.
	cnt := 1
	for e := mm.GetGroups().Front(); e != nil; e = e.Next() {
		group := e.Value.(*memo.Group)
		require.NotNil(t, group)
		require.Equal(t, memo.GroupID(cnt), group.GetGroupID())
		cnt++
	}

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll))

	// bind the pattern to the memo.
	rootGE := mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression)
	binder := NewBinder(pa, rootGE)
	require.True(t, binder.Next())
	require.True(t, binder.holder.Cur.GetLogicalPlan() == join)
	require.True(t, binder.holder.Subs[0].Cur.GetLogicalPlan() == t1)
	require.True(t, binder.holder.Subs[1].Cur.GetLogicalPlan() == t2)
}

func TestBinderFail(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	mm := memo.NewMemo(ctx)
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	// specify one child is from tiflash while the other is from tikv
	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll), pattern.NewPattern(pattern.OperandProjection, pattern.EngineAll))

	// bind the pattern to the memo.
	rootGE := mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression)
	binder := NewBinder(pa, rootGE)
	b := bytes.Buffer{}
	buf := bufio.NewWriter(&b)
	binder.w = buf
	require.False(t, binder.Next())
	buf.Flush()
	require.Equal(t, b.String(), "GE:DataSource_1{}\n")

	s1 := logicalop.LogicalLimit{}.Init(ctx, 0)
	s1.SetChildren(t1)
	p1 := logicalop.LogicalProjection{}.Init(ctx, 0)
	p1.SetChildren(s1)

	p2 := pattern.NewPattern(pattern.OperandLimit, pattern.EngineAll)
	p2.SetChildren(pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll))
	pa = pattern.NewPattern(pattern.OperandProjection, pattern.EngineAll)
	pa.SetChildren(p2)
	binder = NewBinder(pa, rootGE)
	b.Reset()
	buf = bufio.NewWriter(&b)
	binder.w = buf
	require.False(t, binder.Next())
	buf.Flush()
	require.Equal(t, b.String(), "")

	// renew memo
	mm = memo.NewMemo(ctx)
	mm.Init(p1)
	rootGE = mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression)
	binder = NewBinder(pa, rootGE)
	b.Reset()
	buf = bufio.NewWriter(&b)
	binder.w = buf
	require.False(t, binder.Next())
	buf.Flush()
	require.Equal(t, b.String(), "GE:Limit_4{inputs:1}\n")
}

func TestBinderTopNode(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	mm := memo.NewMemo(ctx)
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	// single level pattern, no children.
	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	binder := NewBinder(pa, mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression))
	require.True(t, binder.Next())
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
}

func TestBinderOneNode(t *testing.T) {
	ctx := plannercore.MockContext()
	join := logicalop.LogicalJoin{}.Init(ctx, 0)

	mm := memo.NewMemo(ctx)
	mm.Init(join)
	require.Equal(t, 1, mm.GetGroups().Len())
	require.Equal(t, 1, len(mm.GetGroupID2Group()))

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	binder := NewBinder(pa, mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression))
	require.True(t, binder.Next())
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
}

func TestBinderSubTreeMatch(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join1 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join1.SetChildren(t1, t2)

	t3 := logicalop.DataSource{}.Init(ctx, 0)
	t4 := logicalop.DataSource{}.Init(ctx, 0)
	join2 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join2.SetChildren(t3, t4)

	join3 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join3.SetChildren(join1, join2)

	mm := memo.NewMemo(ctx)
	mm.Init(join3)
	require.Equal(t, 7, mm.GetGroups().Len())
	require.Equal(t, 7, len(mm.GetGroupID2Group()))

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll), pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll))

	// bind the pattern to the memo.
	rootGE := mm.GetRootGroup().GetLogicalExpressions().Back().Value.(*memo.GroupExpression)
	binder := NewBinder(pa, rootGE)
	require.True(t, binder.Next())
	require.True(t, binder.holder.Cur.GetLogicalPlan() == join3)
	require.True(t, binder.holder.Subs[0].Cur.GetLogicalPlan() == join1)
	require.True(t, binder.holder.Subs[1].Cur.GetLogicalPlan() == join2)
	require.False(t, binder.Next())

	pa2 := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa2.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll))
	binder = NewBinder(pa2, rootGE)
	// we couldn't bind the pattern to the subtree of join3, because the root group expression is pinned.
	// the top-down iteration across all the tree nodes is the responsibility of the caller.
	require.False(t, binder.Next())
}

func TestBinderMultiNext(t *testing.T) {
	ctx := plannercore.MockContext()
	asT1 := pmodel.NewCIStr("t1")
	asT2 := pmodel.NewCIStr("t2")
	t1 := logicalop.DataSource{TableAsName: &asT1}.Init(ctx, 0)
	t2 := logicalop.DataSource{TableAsName: &asT2}.Init(ctx, 0)
	join1 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join1.SetChildren(t1, t2)

	asT3 := pmodel.NewCIStr("t3")
	asT4 := pmodel.NewCIStr("t4")
	t3 := logicalop.DataSource{TableAsName: &asT3}.Init(ctx, 0)
	t4 := logicalop.DataSource{TableAsName: &asT4}.Init(ctx, 0)

	mm := memo.NewMemo(ctx)
	gE := mm.Init(join1)

	// which means t1 and t3 are equivalent class.
	mm.CopyIn(gE.Inputs[0], t3)
	// which means t2 and t4 are equivalent class.
	mm.CopyIn(gE.Inputs[1], t4)
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll))
	binder := NewBinder(pa, gE)
	b := bytes.Buffer{}
	buf := bufio.NewWriter(&b)
	binder.w = buf

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴           ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t1", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t2", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴               ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t1", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t4", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//        ▴        ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t3", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t2", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//         ▴           ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t3", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t4", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	buf.Flush()
	// every time when next call done, the save stack info should be next iteration starting point.
	// the last element in the stack should call next element (outside control) before next round starting.
	//                 G1
	//               /    \
	//  G2{id(1),id(4)}   G3{id(2),id(5)}
	//        ▴               ▴
	// when G3 is exhausted, and next gE will be nil, and next() loop will enter next round with stack info popped as
	// G2(id(1)) which is what the third line comes from, and the next round will start from G2.next element starting
	// as G2(id(4)) which is the prefix of the fourth and fifth stack info.
	require.Equal(t, b.String(), "GE:DataSource_1{} -> GE:DataSource_2{}\n"+
		"GE:DataSource_1{} -> GE:DataSource_5{}\n"+
		"GE:DataSource_1{}\n"+
		"GE:DataSource_4{} -> GE:DataSource_2{}\n"+
		"GE:DataSource_4{} -> GE:DataSource_5{}\n")
}

func TestBinderAny(t *testing.T) {
	ctx := plannercore.MockContext()
	asT1 := pmodel.NewCIStr("t1")
	asT2 := pmodel.NewCIStr("t2")
	t1 := logicalop.DataSource{TableAsName: &asT1}.Init(ctx, 0)
	t2 := logicalop.DataSource{TableAsName: &asT2}.Init(ctx, 0)
	join1 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join1.SetChildren(t1, t2)

	asT3 := pmodel.NewCIStr("t3")
	asT4 := pmodel.NewCIStr("t4")
	t3 := logicalop.DataSource{TableAsName: &asT3}.Init(ctx, 0)
	t4 := logicalop.DataSource{TableAsName: &asT4}.Init(ctx, 0)

	mm := memo.NewMemo(ctx)
	gE := mm.Init(join1)

	// which means t1 and t3 are equivalent class.
	mm.CopyIn(gE.Inputs[0], t3)
	// which means t2 and t4 are equivalent class.
	mm.CopyIn(gE.Inputs[1], t4)
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineAll), pattern.NewPattern(pattern.OperandAny, pattern.EngineAll))
	binder := NewBinder(pa, gE)
	b := bytes.Buffer{}
	buf := bufio.NewWriter(&b)
	binder.w = buf

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴           ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t1", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t2", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//         ▴       ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t3", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t2", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.False(t, binder.Next())

	buf.Flush()
	// every time when next call done, the save stack info should be next iteration starting point.
	// the last element in the stack should call next element (outside control) before next round starting.
	//                 G1
	//               /    \
	//  G2{id(1),id(4)}   G3{id(2),id(5)}
	//        ▴               ▴
	// when G3 is matched from the Any pattern, and next gE will be nil and pop the stack info out, and next() loop
	// will enter next round with G2 pointing to id(4) which is table "t3".
	//                 G1
	//               /    \
	//  G2{id(1),id(4)}   G3{id(2),id(5)}
	//            ▴           ▴
	// when G3 is matched from the Any pattern again, and next gE will be nil and pop the stack info out, and next()
	// loop will enter next round with G2 pointing to next which is nil.
	// In a conclusion: the Group matched with Any pattern only generate the first group expression since we don't
	// care what the concrete group expression it is. Because the final generated group expression if any, will be
	// substituted ANY pattern with the referred group at last not a concrete one group expression inside.
	require.Equal(t, b.String(), "GE:DataSource_1{} -> GE:DataSource_2{}\n"+
		"GE:DataSource_1{}\n"+
		"GE:DataSource_4{} -> GE:DataSource_2{}\n"+
		"GE:DataSource_4{}\n")
}

func TestBinderMultiAny(t *testing.T) {
	ctx := plannercore.MockContext()
	asT1 := pmodel.NewCIStr("t1")
	asT2 := pmodel.NewCIStr("t2")
	t1 := logicalop.DataSource{TableAsName: &asT1}.Init(ctx, 0)
	t2 := logicalop.DataSource{TableAsName: &asT2}.Init(ctx, 0)
	join1 := logicalop.LogicalJoin{}.Init(ctx, 0)
	join1.SetChildren(t1, t2)

	asT3 := pmodel.NewCIStr("t3")
	asT4 := pmodel.NewCIStr("t4")
	t3 := logicalop.DataSource{TableAsName: &asT3}.Init(ctx, 0)
	t4 := logicalop.DataSource{TableAsName: &asT4}.Init(ctx, 0)

	mm := memo.NewMemo(ctx)
	gE := mm.Init(join1)

	// which means t1 and t3 are equivalent class.
	mm.CopyIn(gE.Inputs[0], t3)
	// which means t2 and t4 are equivalent class.
	mm.CopyIn(gE.Inputs[1], t4)

	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}

	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineAll)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineAll), pattern.NewPattern(pattern.OperandAny, pattern.EngineAll))
	binder := NewBinder(pa, gE)
	b := bytes.Buffer{}
	buf := bufio.NewWriter(&b)
	binder.w = buf

	require.True(t, binder.Next())
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴           ▴
	require.Equal(t, pattern.OperandJoin, pattern.GetOperand(binder.holder.Cur.GetLogicalPlan()))
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[0].Cur.GetLogicalPlan()))
	require.Equal(t, "t1", binder.holder.Subs[0].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)
	require.Equal(t, pattern.OperandDataSource, pattern.GetOperand(binder.holder.Subs[1].Cur.GetLogicalPlan()))
	require.Equal(t, "t2", binder.holder.Subs[1].Cur.GetLogicalPlan().(*logicalop.DataSource).TableAsName.L)

	require.False(t, binder.Next())

	buf.Flush()
	// state1:
	//           G1
	//         /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴           ▴    (matched: print stack: GE:DataSource_1{} -> GE:DataSource_2{})
	// state2:
	//		     G1
	//		   /    \
	//  G2{t1,t3}   G3{t2,t4}
	//     ▴               ▴ (already matched, pop stack, print stack: GE:DataSource_1{})
	// state3:
	//		     G1
	//		   /    \
	//  G2{t1,t3}   G3{t2,t4}
	//        ▴ (already matched, pop stack)
	// final state: empty stack
	require.Equal(t, b.String(), "GE:DataSource_1{} -> GE:DataSource_2{}\n"+
		"GE:DataSource_1{}\n")
}
