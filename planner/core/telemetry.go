package core

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/plancodec"
)

// IsTiFlashContained returns whether the plan contains TiFlash related executors.
func IsTiFlashContained(plan Plan) (tiFlashPushDown, tiFlashExchangePushDown bool) {
	if plan == nil {
		return
	}
	var tiflashProcess func(p Plan)
	tiflashProcess = func(p Plan) {
		if exp, isExplain := p.(*Explain); isExplain {
			p = exp.TargetPlan
			if p == nil {
				return
			}
		}
		pp, isPhysical := p.(PhysicalPlan)
		if !isPhysical {
			return
		}
		if tableReader, ok := pp.(*PhysicalTableReader); ok {
			tiFlashPushDown = tableReader.StoreType == kv.TiFlash
			if tiFlashPushDown && tableReader.GetTablePlan().TP() == plancodec.TypeExchangeSender {
				tiFlashExchangePushDown = true
			}
			return
		}
		for _, child := range pp.Children() {
			tiflashProcess(child)
			if tiFlashPushDown {
				return
			}
		}
	}
	tiflashProcess(plan)
	return
}
