// Copyright 2025 PingCAP, Inc.
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

package bindinfo

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type BindingPlanInfo struct {
	*Binding

	// Info from StmtStats
	Plan                 string
	AvgLatency           float64
	ExecTimes            int64
	AvgScanRows          float64
	AvgReturnedRows      float64
	LatencyPerReturnRow  float64
	ScanRowsPerReturnRow float64

	// Recommendation
	Recommend string
	Reason    string
}

// BindingAuto represents a series of APIs that help manage bindings automatically.
type BindingAuto interface {
	// TODO: RecordHistPlansAsBindings records the history plans as bindings for qualified queries.

	// ShowPlansForSQL shows historical plans for a specific SQL.
	ShowPlansForSQL(sqlOrDigest string) ([]*BindingPlanInfo, error)
}

type bindingAuto struct {
	sPool util.DestroyableSessionPool
}

func newBindingAuto(sPool util.DestroyableSessionPool) BindingAuto {
	return &bindingAuto{
		sPool: sPool,
	}
}

// ShowPlansForSQL shows historical plans for a specific SQL.
func (ba *bindingAuto) ShowPlansForSQL(sqlOrDigest string) ([]*BindingPlanInfo, error) {
	whereCond := fmt.Sprintf("where sql_digest='%s'", sqlOrDigest)
	bindings, err := readBindingsFromStorage(ba.sPool, whereCond)
	if err != nil {
		return nil, err
	}
	var autoBindings []*BindingPlanInfo
	for _, binding := range bindings {
		pInfo, err := ba.getStmtStatsByPlanDigestInCluster(binding.PlanDigest)
		if err != nil {
			logutil.BgLogger().Error("getStmtStatsByDigestInCluster", zap.String("plan_digest", binding.PlanDigest), zap.Error(err))
			continue
		}
		autoBinding := &BindingPlanInfo{Binding: binding}
		if pInfo != nil && pInfo.ExecCount > 0 {
			autoBinding.Plan = pInfo.Plan
			autoBinding.ExecTimes = pInfo.ExecCount
			autoBinding.AvgLatency = float64(pInfo.TotalTime) / float64(pInfo.ExecCount)
			autoBinding.AvgScanRows = float64(pInfo.ProcessedKeys) / float64(pInfo.ExecCount)
			autoBinding.AvgReturnedRows = float64(pInfo.ResultRows) / float64(pInfo.ExecCount)
			autoBinding.LatencyPerReturnRow = autoBinding.AvgLatency / autoBinding.AvgReturnedRows
			autoBinding.ScanRowsPerReturnRow = autoBinding.AvgScanRows / autoBinding.AvgReturnedRows
		}
		autoBindings = append(autoBindings, autoBinding)
	}
	return nil, nil
}

func (ba *bindingAuto) getStmtStatsByPlanDigestInCluster(planDigest string) (plan *planInfo, err error) {
	if planDigest == "" {
		return nil, nil
	}
	stmtQuery := fmt.Sprintf(`
				select digest, query_sample_text, charset,
				collation, plan_hint, plan_digest, schema_name,
				result_rows, exec_count, processed_keys, total_time, plan
				from information_schema.tidb_statements_stats
				where plan_digest = '%v'`, planDigest)
	var rows []chunk.Row
	err = callWithSCtx(ba.sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > 1 {
		// TODO: accumulate them
	}

	row := rows[0]
	return &planInfo{
		Digest:          row.GetString(0),
		QuerySampleText: row.GetString(1),
		Charset:         row.GetString(2),
		Collation:       row.GetString(3),
		PlanHint:        row.GetString(4),
		PlanDigest:      row.GetString(5),
		SchemaName:      row.GetString(6),
		ResultRows:      row.GetInt64(7),
		ExecCount:       row.GetInt64(8),
		ProcessedKeys:   row.GetInt64(9),
		TotalTime:       row.GetInt64(10),
		Plan:            row.GetString(11),
	}, nil
}

// planInfo represents the plan info from information_schema.tidb_statements_stats table.
type planInfo struct {
	// meta info
	Digest          string
	QuerySampleText string
	Charset         string
	Collation       string
	PlanHint        string
	PlanDigest      string
	SchemaName      string

	// exec info
	Plan          string
	ResultRows    int64
	ExecCount     int64
	ProcessedKeys int64
	TotalTime     int64
}
