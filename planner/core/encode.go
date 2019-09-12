package core

import (
	"bytes"
	"sync"

	"github.com/pingcap/tidb/planner/codec"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &planEncoder{}
	},
}

type planEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
	compressBuf  bytes.Buffer
}

// EncodePlan is used to encodePlan the plan to the plan tree with compressing.
func EncodePlan(p PhysicalPlan) (string, error) {
	pn := encoderPool.Get().(*planEncoder)
	defer encoderPool.Put(pn)
	return pn.encodePlanTree(p)
}

func (pn *planEncoder) encodePlanTree(p PhysicalPlan) (string, error) {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.compressBuf.Reset()
	pn.encodePlan(p, true, 0)
	return codec.Compress(pn.buf.Bytes(), &pn.compressBuf)
}

func (pn *planEncoder) encodePlan(p PhysicalPlan, isRoot bool, depth int) {
	codec.EncodePlanNode(depth, p.ID(), p.TP(), isRoot, p.statsInfo().RowCount, p.ExplainInfo(), &pn.buf)
	pn.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encodePlan(child.(PhysicalPlan), isRoot, depth)
	}
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		pn.encodePlan(copPlan.tablePlan, false, depth)
	case *PhysicalIndexReader:
		pn.encodePlan(copPlan.indexPlan, false, depth)
	case *PhysicalIndexLookUpReader:
		pn.encodePlan(copPlan.indexPlan, false, depth)
		pn.encodePlan(copPlan.tablePlan, false, depth)
	}
}
