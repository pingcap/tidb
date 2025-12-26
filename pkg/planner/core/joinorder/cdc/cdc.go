package cdc

import (
	"github.com/pingcap/tidb/pkg/planner/base"
	"github.com/pingcap/tidb/pkg/expression"
	"golang.org/x/tools/go/ast/edge"
)

type BitSet uint64

type (b *BitSet) Union(s BitSet) {
	panic("union not impl")
}

type (b *BitSet) Intersect(s BitSet) {
	panic("intersect not impl")
}

type Node struct {
	bitSet BitSet
	p base.LogicalPlan
	leftVertexes []uint64
	rightVertexes []uint64
	leftEdges []*edge
	rightEdges []*edge
}

func newNode(bitSet BitSet, p base.LogicalPlan, leftVertexes, rightVertexes []uint64, leftEdges, rightEdges []*edge) *Node {
	return &Node {
		bitSet: bitSet,
		p: p,
		leftVertexes: leftVertexes,
		rightVertexes: rightVertexes,
		leftEdges: leftEdges,
		rightEdges: rightEdges,
	}
}

func (v *Node) GetIndex() uint64 {
	return uint64(v.bitSet)
}

func (v *Node) GetCost() float64 {
	p.StatsInfo().RowCount  // gjt todo real cost
}

type rule struct {
	t1 BitSet
	t2 BitSet
}

type edge struct {
	p *logicalop.LogicalJoin
	// index into vertexPlans
	tes BitSet
	rules []*rule

	leftVertexBitSet BitSet
	rightVertexBitSet BitSet
	leftEdges []*edge
	rightEdges []*edge
}

func newEdge(p *logicalop.LogicalJoin, leftVertexes, rightVertexes BitSet, leftEdges, rightEdges []*edge, vertexPlans []base.LogicalPlan) *edge {
	e := &edge{
		p: p,
		leftVertexes: leftVetexes,
		rightVertexes: rightVertexes,
		leftEdges: leftEdges,
		rightEdges: rightEdges,
	}
	e.calcTES(vertexPlans)
	e.calcRules()
}

func (e *edge) calcTES(vertexPlans []base.LogicalPlan) {
	// gjt todo: NAEQConditions, LeftConditions, RightConditions
	for _, cond := range p.EqualConditions {
		curBitSet := e.extractBitSetFromExpr(cond, vertexPlans)
		e.tes.union(curBitSet)
	}

	// gjt todo handle ohterconditions
}

func (e *edge) extractBitSetFromExpr(cond expression.Expression, vertexPlans []base.LogicalPlan) (BitSet, error) {
	switch x := cond.(type) {
	case *expression.Column:
		for i, curPlan := range vertexPlans {
			switch y := curPlan.(type) {
			case *logicalop.DataSource:
				schema := y.Schema()
				if expression.ExprReferenceSchema(x, schema) {
					// todo keep running and we can check sanity
					return newBitSet(i), nil
				}
			}
		}
		// gjt todo refine error
		return 0, errors.New("cannot find this column from all ds")
	case *expression.CorrelatedColumn:
		// gjt todo:
		panic("not impl for correlated col")
	case *expression.Constant:
		return 0, nil
	case *expression.ScalarFunction:
		var resBit BitSet
		for _, cond := x.GetArgs() {
			curBit, err := e.extractBitSetFromExpr(cond, vertexPlans)
			if err != nil {
				return resBit, err
			}
			resBit.union(curBit)
		}
		return resBit, nil
	default:
		// gjt todo refine error
		panic("unexpected expression type")
	}
}

func (e *edge) calcRules() {
	for i, childEdge := range e.leftEdges {
		if !assocTable[childEdge.p.JoinType, e.p.JoinType] {
			// gjt todo
			// T1{childEdge.rightVertexes} -> T2{leftEdge.leftVertexes}
			panic("impl asscomTable")
		}
		if !leftAsscomTable[childEdge.p.JoinType, e.p.JoinType] {
			panic("impl leftAsscomTable")
		}
	}
	for i, childEdge := range e.rightEdges {
		// gjt todo
		panic("gjt debug")
	}
}

type ConflictDetector struct {
	edges []*edge
	vertexPlans []base.LogicalPlan
	originalRootPlan base.LogicalPlan
}

type GroupChecker interface {
	check() bool
}

func NewConflictDetector(p base.LogicalPlan, checker GroupChecker) *ConflictDetector {
	if p == nil {
		return nil
	}
	c := &ConflictDetector{originalRootPlan: p}
	c.edges, _ = c.buildRecursive(c.originalRootPlan)
}

func (c *ConflictDetector) buildRecursive(p base.LogicalPlan, checker GroupChecker) ([]*edge, BitSet) {
	if p == nil {
		return nil, 0
	}

	makeVertex := func(p base.LogicalPlan) {
		curBit := NewBitSet(len(c.vertexPlans))
		c.vertexPlans[curBit] = p
		return nil, curBit
	}
	if !checker.check(p) {
		return makeVertex(p)
	}
	
	switch x := p.(type) {
	case *base.LogicalJoin:
		leftVertexes, leftEdges := c.buildRecursive(p.Children()[0])
		rightVertexes, rightEdges := c.buildRecursive(p.Children()[1])

		curBitSet := leftBitSet.union(rightBitSet)
		curEdge = newEdge(p, leftBitSet, rightBitSet, leftEdges, rightEdges, c.vertexPlans)

		resEdges := append(leftEdges, rightEdges...)
		resEdges = append(resEdges, curEdge)

		return resEdges, curBitSet
	default:
		return makeVertex(p)
	}

	panic("should not reach here")
}

func (c *ConflictDetector) CheckConnection(s1, s2 BitSet) bool {
	panic("gjt impl checkConnection")
}
