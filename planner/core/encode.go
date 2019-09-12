package core

import (
	"bytes"
	"sync"

	"github.com/pingcap/parser/terror"
	. "github.com/pingcap/tidb/util/plan"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &planEncoder{}
	},
}

type planEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
}

// EncodePlan is used to encodePlan the plan to the plan tree with compressing.
func EncodePlan(p PhysicalPlan) string {
	pn := encoderPool.Get().(*planEncoder)
	defer encoderPool.Put(pn)
	return pn.encodePlanTree(p)
}

func (pn *planEncoder) encodePlanTree(p PhysicalPlan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.encodePlan(p, true, 0)
	bs := pn.buf.Bytes()
	str, err := Compress(bs)
	if err != nil {
		terror.Log(err)
	}
	return str
}

func (pn *planEncoder) encodePlan(p PhysicalPlan, isRoot bool, depth int) {
	EncodePlanNode(depth, p.ID(), p.TP(), isRoot, p.statsInfo().RowCount, p.ExplainInfo(), &pn.buf)
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
