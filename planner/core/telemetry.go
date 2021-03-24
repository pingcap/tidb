package core

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/plancodec"
)

// GetTiFlashTelemetry execute telemetry for a plan.
func GetTiFlashTelemetry(plan Plan) (tiFlashPushDown, tiFlashExchangePushDown bool) {
	if plan == nil {
		return
	}
	var tiflashProcess func(p Plan)
	tiflashProcess = func(p Plan) {
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
