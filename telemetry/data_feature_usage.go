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
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/util/sqlexec"
)

type featureUsage struct {
	Txn          *TxnUsage       `json:"txn"`
	ClusterIndex map[string]bool `json:"clusterIndex"`
}

func getFeatureUsage(ctx sessionctx.Context) (*featureUsage, error) {
	// init
	usageInfo := featureUsage{
		ClusterIndex: make(map[string]bool),
	}

	// cluster index
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.TODO(), `
		SELECT left(sha2(TABLE_NAME, 256), 6) name, TIDB_PK_TYPE
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')
		ORDER BY name
		limit 10000`)
	if err != nil {
		return nil, err
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.Len() < 2 {
			continue
		}
		isClustered := false
		if row.GetString(1) == "CLUSTERED" {
			isClustered = true
		}
		usageInfo.ClusterIndex[row.GetString(0)] = isClustered
	}

	// transaction related feature
	usageInfo.Txn = GetTxnUsageInfo(ctx)

	return &usageInfo, nil
}

// TxnUsage records the usage info of transaction related features, including
// async-commit, 1PC and counters of transactions committed with different protocols.
type TxnUsage struct {
	AsyncCommitUsed  bool                     `json:"asyncCommitUsed"`
	OnePCUsed        bool                     `json:"onePCUsed"`
	TxnCommitCounter metrics.TxnCommitCounter `json:"txnCommitCounter"`
}

var initialTxnCommitCounter metrics.TxnCommitCounter

// GetTxnUsageInfo gets the usage info of transaction related features. It's exported for tests.
func GetTxnUsageInfo(ctx sessionctx.Context) *TxnUsage {
	asyncCommitUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBEnableAsyncCommit); err == nil {
		asyncCommitUsed = val == variable.BoolOn
	}
	onePCUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBEnable1PC); err == nil {
		onePCUsed = val == variable.BoolOn
	}
	curr := metrics.GetTxnCommitCounter()
	diff := curr.Sub(initialTxnCommitCounter)
	return &TxnUsage{asyncCommitUsed, onePCUsed, diff}
}

func postReportTxnUsage() {
	initialTxnCommitCounter = metrics.GetTxnCommitCounter()
}
