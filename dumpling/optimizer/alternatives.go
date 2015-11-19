package optimizer

import (
	"github.com/pingcap/tidb/optimizer/plan"
	"fmt"
)

// alternnatives returns multiple alternative plans that
// can be picked base on its cost.
func alternatives(p plan.Plan) []plan.Plan {
	switch x := p.(type) {
	case *plan.TableScan:
		return tableScanAlternatives(x)
	case plan.PlanWithSrc:
		return planWithSrcAlternatives(x)
	default:
		panic(fmt.Sprintf("unknown plan %T", p))
	}
}

func tableScanAlternatives(p *plan.TableScan) []plan.Plan {
	var alts []plan.Plan
	for _, v := range p.Table.Indices {
		fullRange := &plan.IndexRange{
			HighVal: []interface{}{plan.MaxVal},
		}
		ip := &plan.IndexScan{
			Index: v,
			Table: p.Table,
			Ranges: []*plan.IndexRange{fullRange},
		}
		alts = append(alts, ip)
	}
	return alts
}

func planWithSrcAlternatives(p plan.PlanWithSrc) []plan.Plan {
	srcs := alternatives(p.Src())
	for i, val := range srcs {
		alt := shallowCopy(p)
		alt.SetSrc(val)
		srcs[i] = alt
	}
	return srcs
}

func shallowCopy(p plan.PlanWithSrc) plan.PlanWithSrc {
	var copied plan.PlanWithSrc
	switch x := p.(type) {
	case *plan.Filter:
		n := *x
		copied = &n
	case *plan.SelectLock:
		n := *x
		copied = &n
	case *plan.SelectFields:
		n := *x
		copied = &n
	case *plan.Sort:
		n := *x
		copied = &n
	case *plan.Limit:
		n := *x
		copied = &n
	}
	return copied
}
