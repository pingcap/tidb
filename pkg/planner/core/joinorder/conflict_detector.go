package joinorder

import (
	"errors"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type ConflictDetector struct {
	groupRoot     base.LogicalPlan
	groupVertexes []*Node
	edges         []*edge
}

type edge struct {
	joinop *logicalop.LogicalJoin

	tes   BitSet
	rules []*rule

	leftEdges     []*edge
	rightEdges    []*edge
	leftVertexes  BitSet
	rightVertexes BitSet
}

type BitSet uint64

func newBitSet(idx int64) (BitSet, error) {
	if idx < 0 || idx >= 64 {
		return 0, errors.New("BitSet index out of range")
	}
	return 1 << idx, nil
}

func (b BitSet) Union(s BitSet) BitSet {
	return b | s
}

func (b BitSet) Intersect(s BitSet) bool {
	return (b & s) != 0
}

type rule struct {
	from BitSet
	to   BitSet
}

type Node struct {
	bitSet  BitSet
	p       base.LogicalPlan
	costSum float64
}

func (d *ConflictDetector) Build(group *joinGroup) []*Node {
	d.groupRoot = group.root
	d.buildRecursive(group.root, group.vertexes)
	return d.groupVertexes
}

func (d *ConflictDetector) buildRecursive(p base.LogicalPlan, vertexes []base.LogicalPlan) ([]*edge, BitSet, error) {
	for i, v := range vertexes {
		if p.ID() == v.ID() {
			bitSet, err := newBitSet(int64(i))
			if err != nil {
				return nil, 0, err
			}
			node := &Node{
				bitSet: bitSet,
				p:      v,
			}
			d.groupVertexes = append(d.groupVertexes, node)
			return nil, node.bitSet, nil
		}
	}

	// All internal nodes in the join group should be join operators.
	joinop, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return nil, 0, errors.New("unexpected plan type in conflict detector")
	}

	leftEdges, leftVertexes, err := d.buildRecursive(joinop.Children()[0], vertexes)
	if err != nil {
		return nil, 0, err
	}
	rightEdges, rightVertexes, err := d.buildRecursive(joinop.Children()[1], vertexes)
	if err != nil {
		return nil, 0, err
	}

	curEdge := d.makeEdge(joinop, leftVertexes, rightVertexes, leftEdges, rightEdges)
	if leftVertexes.Intersect(rightVertexes) {
		return nil, 0, errors.New("conflicting join edges detected")
	}
	curVertexes := leftVertexes.Union(rightVertexes)

	return append(leftEdges, append(rightEdges, curEdge)...), curVertexes, nil
}

func (d *ConflictDetector) makeEdge(joinop *logicalop.LogicalJoin, leftVertexes, rightVertexes BitSet, leftEdges, rightEdges []*edge) *edge {
	e := &edge{
		joinop:        joinop,
		leftVertexes:  leftVertexes,
		rightVertexes: rightVertexes,
		leftEdges:     leftEdges,
		rightEdges:    rightEdges,
	}

	// setup TES. Only consider EqualConditions and NAEQConditions.
	// OtherConditions are not edges.
	e.tes = d.calcTES(joinop.EqualConditions)
	e.tes = e.tes.Union(d.calcTES(joinop.NAEQConditions))

	// gjt todo: handle CrossProduct

	// setup conflict rules
	for _, left := range leftEdges {
		if !assoc(left, e) {
			// gjt todo
		}
		if !leftAsscom(left, e) {
			// gjt todo
		}
	}
	for _, right := range rightEdges {
		if !assoc(e, right) {
			// gjt todo
		}
		if !rightAsscom(e, right) {
			// gjt todo
		}
	}

	return e
}

func (d *ConflictDetector) calcTES(conds []base.Expression) BitSet {
	var res BitSet
	for _, cond := range conds {
		for _, node := range d.groupVertexes {
			if expression.ExprReferenceSchema(cond, node.p.Schema()) {
				res = res.Union(node.bitSet)
			}
		}
	}
	return res
}

func assoc(e1, e2 *edge) bool {
	// gjt todo: implement
	return true
}

func leftAsscom(e1, e2 *edge) bool {
	// gjt todo: implement
	return true
}

func rightAsscom(e1, e2 *edge) bool {
	// gjt todo: implement
	return true
}
