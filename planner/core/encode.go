package core

import (
	"bytes"
	"sync"

	"github.com/pingcap/parser/terror"
	. "github.com/pingcap/tidb/util/plan"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &PlanEncoder{}
	},
}

type PlanEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
}

func EncodePlan(p PhysicalPlan) string {
	pn := encoderPool.Get().(*PlanEncoder)
	defer encoderPool.Put(pn)
	return pn.Encode(p)
}

func (pn *PlanEncoder) Encode(p PhysicalPlan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.encode(p, RootTaskType, 0)
	bs := pn.buf.Bytes()
	if len(bs) > 0 && bs[len(bs)-1] == LineBreaker {
		bs = bs[:len(bs)-1]
	}
	str, err := Compress(bs)
	if err != nil {
		terror.Log(err)
	}
	return str
}

func (pn *PlanEncoder) encode(p PhysicalPlan, taskType string, depth int) {
	EncodePlanNode(depth, p.ID(), p.TP(), taskType, p.statsInfo().RowCount, p.ExplainInfo(), &pn.buf)
	pn.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encode(child.(PhysicalPlan), taskType, depth)
	}
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		pn.encode(copPlan.tablePlan, CopTaskType, depth)
	case *PhysicalIndexReader:
		pn.encode(copPlan.indexPlan, CopTaskType, depth)
	case *PhysicalIndexLookUpReader:
		pn.encode(copPlan.indexPlan, CopTaskType, depth)
		pn.encode(copPlan.tablePlan, CopTaskType, depth)
	}
}
