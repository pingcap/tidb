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

package telemetry

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/infoschema"
	m "github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/metrics"
)

type featureUsage struct {
	// transaction usage information
	Txn *TxnUsage `json:"txn"`
	// cluster index usage information
	// key is the first 6 characters of sha2(TABLE_NAME, 256)
	ClusterIndex         *ClusterIndexUsage    `json:"clusterIndex"`
	TemporaryTable       bool                  `json:"temporaryTable"`
	CTE                  *m.CTEUsageCounter    `json:"cte"`
	CachedTable          bool                  `json:"cachedTable"`
	AutoCapture          bool                  `json:"autoCapture"`
	PlacementPolicyUsage *placementPolicyUsage `json:"placementPolicy"`
}

type placementPolicyUsage struct {
	NumPlacementPolicies uint64 `json:"numPlacementPolicies"`
	NumDBWithPolicies    uint64 `json:"numDBWithPolicies"`
	NumTableWithPolicies uint64 `json:"numTableWithPolicies"`
	// The number of partitions that policies are explicitly specified.
	NumPartitionWithExplicitPolicies uint64 `json:"numPartitionWithExplicitPolicies"`
}

func getFeatureUsage(ctx sessionctx.Context) (*featureUsage, error) {
	var usage featureUsage
	var err error
	usage.ClusterIndex, err = getClusterIndexUsageInfo(ctx)
	if err != nil {
		logutil.BgLogger().Info(err.Error())
		return nil, err
	}

	// transaction related feature
	usage.Txn = getTxnUsageInfo(ctx)

	usage.CTE = getCTEUsageInfo()

	usage.AutoCapture = getAutoCaptureUsageInfo(ctx)

	collectFeatureUsageFromInfoschema(ctx, &usage)
	return &usage, nil
}

// collectFeatureUsageFromInfoschema updates the usage for temporary table, cached table and placement policies.
func collectFeatureUsageFromInfoschema(ctx sessionctx.Context, usage *featureUsage) {
	if usage.PlacementPolicyUsage == nil {
		usage.PlacementPolicyUsage = &placementPolicyUsage{}
	}
	is := GetDomainInfoSchema(ctx)
	for _, dbInfo := range is.AllSchemas() {
		if dbInfo.PlacementPolicyRef != nil {
			usage.PlacementPolicyUsage.NumDBWithPolicies++
		}

		for _, tbInfo := range is.SchemaTables(dbInfo.Name) {
			if tbInfo.Meta().TempTableType != model.TempTableNone {
				usage.TemporaryTable = true
			}
			if tbInfo.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
				usage.CachedTable = true
			}
			if tbInfo.Meta().PlacementPolicyRef != nil {
				usage.PlacementPolicyUsage.NumTableWithPolicies++
			}
			partitions := tbInfo.Meta().GetPartitionInfo()
			if partitions == nil {
				continue
			}
			for _, partitionInfo := range partitions.Definitions {
				if partitionInfo.PlacementPolicyRef != nil {
					usage.PlacementPolicyUsage.NumPartitionWithExplicitPolicies++
				}
			}
		}
	}

	usage.PlacementPolicyUsage.NumPlacementPolicies += uint64(len(is.AllPlacementPolicies()))
}

// GetDomainInfoSchema is used by the telemetry package to get the latest schema information
// while avoiding circle dependency with domain package.
var GetDomainInfoSchema func(sessionctx.Context) infoschema.InfoSchema

// ClusterIndexUsage records the usage info of all the tables, no more than 10k tables
type ClusterIndexUsage map[string]TableClusteredInfo

// TableClusteredInfo records the usage info of clusterindex of each table
// CLUSTERED, NON_CLUSTERED, NA
type TableClusteredInfo struct {
	IsClustered   bool   `json:"isClustered"`   // True means CLUSTERED, False means NON_CLUSTERED
	ClusterPKType string `json:"clusterPKType"` // INT means clustered PK type is int
	// NON_INT means clustered PK type is not int
	// NA means this field is no meaningful information
}

// getClusterIndexUsageInfo gets the ClusterIndex usage information. It's exported for future test.
func getClusterIndexUsageInfo(ctx sessionctx.Context) (cu *ClusterIndexUsage, err error) {
	usage := make(ClusterIndexUsage)
	exec := ctx.(sqlexec.RestrictedSQLExecutor)

	// query INFORMATION_SCHEMA.tables to get the latest table information about ClusterIndex
	rows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, `
		SELECT left(sha2(TABLE_NAME, 256), 6) table_name_hash, TIDB_PK_TYPE, TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')
		ORDER BY table_name_hash
		limit 10000`)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown failure")
			}
		}
	}()

	err = ctx.RefreshTxnCtx(context.TODO())
	if err != nil {
		return nil, err
	}
	infoSchema := ctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)

	// check ClusterIndex information for each table
	// row: 0 = table_name_hash, 1 = TIDB_PK_TYPE, 2 = TABLE_SCHEMA (db), 3 = TABLE_NAME

	for _, row := range rows {
		if row.Len() < 4 {
			continue
		}
		tblClusteredInfo := TableClusteredInfo{false, "NA"}
		if row.GetString(1) == "CLUSTERED" {
			tblClusteredInfo.IsClustered = true
			table, err := infoSchema.TableByName(model.NewCIStr(row.GetString(2)), model.NewCIStr(row.GetString(3)))
			if err != nil {
				continue
			}
			tableInfo := table.Meta()
			if tableInfo.PKIsHandle {
				tblClusteredInfo.ClusterPKType = "INT"
			} else if tableInfo.IsCommonHandle {
				tblClusteredInfo.ClusterPKType = "NON_INT"
			} else {
				// if both CLUSTERED IS TURE and CLUSTERPKTYPE IS NA met, this else is hit
				// it means the status of INFORMATION_SCHEMA.tables if not consistent with session.Context
				// WE SHOULD treat this issue SERIOUSLY
			}
		}
		usage[row.GetString(0)] = tblClusteredInfo
	}

	return &usage, nil
}

// TxnUsage records the usage info of transaction related features, including
// async-commit, 1PC and counters of transactions committed with different protocols.
type TxnUsage struct {
	AsyncCommitUsed     bool                     `json:"asyncCommitUsed"`
	OnePCUsed           bool                     `json:"onePCUsed"`
	TxnCommitCounter    metrics.TxnCommitCounter `json:"txnCommitCounter"`
	MutationCheckerUsed bool                     `json:"mutationCheckerUsed"`
	AssertionLevel      string                   `json:"assertionLevel"`
	RcCheckTS           bool                     `json:"rcCheckTS"`
}

var initialTxnCommitCounter metrics.TxnCommitCounter
var initialCTECounter m.CTEUsageCounter

// getTxnUsageInfo gets the usage info of transaction related features. It's exported for tests.
func getTxnUsageInfo(ctx sessionctx.Context) *TxnUsage {
	asyncCommitUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBEnableAsyncCommit); err == nil {
		asyncCommitUsed = val == variable.On
	}
	onePCUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBEnable1PC); err == nil {
		onePCUsed = val == variable.On
	}
	curr := metrics.GetTxnCommitCounter()
	diff := curr.Sub(initialTxnCommitCounter)
	mutationCheckerUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBEnableMutationChecker); err == nil {
		mutationCheckerUsed = val == variable.On
	}
	assertionUsed := ""
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBTxnAssertionLevel); err == nil {
		assertionUsed = val
	}
	rcCheckTSUsed := false
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBRCReadCheckTS); err == nil {
		rcCheckTSUsed = val == variable.On
	}
	return &TxnUsage{asyncCommitUsed, onePCUsed, diff, mutationCheckerUsed, assertionUsed, rcCheckTSUsed}
}

func postReportTxnUsage() {
	initialTxnCommitCounter = metrics.GetTxnCommitCounter()
}

// ResetCTEUsage resets CTE usages.
func postReportCTEUsage() {
	initialCTECounter = m.GetCTECounter()
}

// getCTEUsageInfo gets the CTE usages.
func getCTEUsageInfo() *m.CTEUsageCounter {
	curr := m.GetCTECounter()
	diff := curr.Sub(initialCTECounter)
	return &diff
}

// getAutoCaptureUsageInfo gets the 'Auto Capture' usage
func getAutoCaptureUsageInfo(ctx sessionctx.Context) bool {
	if val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBCapturePlanBaseline); err == nil {
		return val == variable.On
	}
	return false
}
