// Copyright 2026 PingCAP, Inc.
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

package physicalop

import (
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
)

// PKRangeProbe lets the executor bound the handle range of a table scan.
// Every row the scan returns satisfies EqConds, which fix all columns of
// Index, so the row's handle lies between the smallest and the largest handle
// stored in Index under the key EqConds select. The executor reads those two
// index entries when the scan starts and skips the parts of the table outside
// them. The probe never changes which plan is chosen.
type PKRangeProbe struct {
	Index *model.IndexInfo
	// EqConds hold one `column = constant` condition per index column, in
	// index column order. The constants may be parameter markers.
	EqConds []expression.Expression
	// IdxCols and IdxColLens describe the index columns for the ranger.
	IdxCols    []*expression.Column
	IdxColLens []int
}

func (p *PKRangeProbe) cloneForPlanCache() *PKRangeProbe {
	cloned := *p
	cloned.EqConds = utilfuncp.CloneExpressionsForPlanCache(p.EqConds, nil)
	cloned.IdxCols = utilfuncp.CloneColumnsForPlanCache(p.IdxCols, nil)
	cloned.IdxColLens = make([]int, len(p.IdxColLens))
	copy(cloned.IdxColLens, p.IdxColLens)
	return &cloned
}

func clonePKRangeProbesForPlanCache(probes []*PKRangeProbe) []*PKRangeProbe {
	if probes == nil {
		return nil
	}
	cloned := make([]*PKRangeProbe, 0, len(probes))
	for _, p := range probes {
		cloned = append(cloned, p.cloneForPlanCache())
	}
	return cloned
}

// explainPKRangeProbes returns the EXPLAIN text for the probes of a reader.
func explainPKRangeProbes(probes []*PKRangeProbe) string {
	if len(probes) == 0 {
		return ""
	}
	names := make([]string, 0, len(probes))
	for _, p := range probes {
		names = append(names, p.Index.Name.O)
	}
	return ", pk_range_probe(" + strings.Join(names, ", ") + ")"
}
