// Copyright 2022 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/tidb/util/tracing"
)

const (
	// RowSizeLbl indicates rowSize
	RowSizeLbl = "rowSize"
	// NetworkFactorLbl indicates networkFactor
	NetworkFactorLbl = "networkFactor"
	// SeekFactorLbl indicates seekFactor
	SeekFactorLbl = "seekFactor"
)

func setPointGetPlanCostDetail(p *PointGetPlan, opt *physicalOptimizeOp,
	rowSize, networkFactor, seekFactor float64) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(SeekFactorLbl, seekFactor).
		SetDesc(fmt.Sprintf("%s*%s+%s", RowSizeLbl, NetworkFactorLbl, SeekFactorLbl))
	opt.appendPlanCostDetail(detail)
}
