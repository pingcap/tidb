// Copyright 2021 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// IsTiFlashContained returns whether the plan contains TiFlash related executors.
func IsTiFlashContained(plan base.Plan) (tiFlashPushDown, tiFlashExchangePushDown bool) {
	if plan == nil {
		return
	}
	var tiflashProcess func(p base.Plan)
	tiflashProcess = func(p base.Plan) {
		if exp, isExplain := p.(*Explain); isExplain {
			p = exp.TargetPlan
			if p == nil {
				return
			}
		}
		pp, isPhysical := p.(base.PhysicalPlan)
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
