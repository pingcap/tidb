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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
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
	ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error)
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
func (ba *bindingAuto) ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error) {
	p := parser.New()
	var normalizedSQL, whereCond string
	stmtNode, err := p.ParseOneStmt(sqlOrDigest, charset, collation)
	if err != nil {
		db := utilparser.GetDefaultDB(stmtNode, currentDB)
		normalizedSQL, _ = NormalizeStmtForBinding(stmtNode, db, false)
	}
	if normalizedSQL != "" {
		whereCond = fmt.Sprintf("where original_sql='%s'", normalizedSQL)
	} else { // treat sqlOrDigest as a digest
		whereCond = fmt.Sprintf("where sql_digest='%s'", sqlOrDigest)
	}
	bindings, err := readBindingsFromStorage(ba.sPool, whereCond)
	if err != nil {
		return nil, err
	}
	var autoBindings []*BindingPlanInfo
	for _, binding := range bindings {
		pInfo, err := ba.getStmtStatsByPlanDigest(binding.PlanDigest)
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

// getStmtStatsByPlanDigest gets the plan info from information_schema.tidb_statements_stats table.
func (ba *bindingAuto) getStmtStatsByPlanDigest(planDigest string) (plan *planInfo, err error) {
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
		// TODO: read data from workload_schema.hist_stmt_stats in this case if it's enabled.
		return nil, nil
	}

	pi := new(planInfo)
	for _, row := range rows {
		// Merge them into one record if there are multiple rows.
		// Assume they have the same meta info and accumulate result-rows, exec-count, pro-keys, total-time.
		pi.Digest = row.GetString(0)
		pi.QuerySampleText = row.GetString(1)
		pi.Charset = row.GetString(2)
		pi.Collation = row.GetString(3)
		pi.PlanHint = row.GetString(4)
		pi.PlanDigest = row.GetString(5)
		pi.SchemaName = row.GetString(6)
		pi.ResultRows += row.GetInt64(7)
		pi.ExecCount += row.GetInt64(8)
		pi.ProcessedKeys += row.GetInt64(9)
		pi.TotalTime += row.GetInt64(10)
		pi.Plan = row.GetString(11)
	}
	return pi, nil
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
