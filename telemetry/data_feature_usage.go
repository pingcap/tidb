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

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	m "github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/metrics"
)

// emptyClusterIndexUsage is empty ClusterIndexUsage, deprecated.
var emptyClusterIndexUsage = ClusterIndexUsage{}

type featureUsage struct {
	// transaction usage information
	Txn *TxnUsage `json:"txn"`
	// cluster index usage information
	// key is the first 6 characters of sha2(TABLE_NAME, 256)
	ClusterIndex          *ClusterIndexUsage             `json:"clusterIndex"`
	NewClusterIndex       *NewClusterIndexUsage          `json:"newClusterIndex"`
	TemporaryTable        bool                           `json:"temporaryTable"`
	CTE                   *m.CTEUsageCounter             `json:"cte"`
	CachedTable           bool                           `json:"cachedTable"`
	AutoCapture           bool                           `json:"autoCapture"`
	PlacementPolicyUsage  *placementPolicyUsage          `json:"placementPolicy"`
	NonTransactionalUsage *m.NonTransactionalStmtCounter `json:"nonTransactional"`
	GlobalKill            bool                           `json:"globalKill"`
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
	usage.NewClusterIndex, usage.ClusterIndex, err = getClusterIndexUsageInfo(ctx)
	if err != nil {
		logutil.BgLogger().Info(err.Error())
		return nil, err
	}

	// transaction related feature
	usage.Txn = getTxnUsageInfo(ctx)

	usage.CTE = getCTEUsageInfo()

	usage.AutoCapture = getAutoCaptureUsageInfo(ctx)

	collectFeatureUsageFromInfoschema(ctx, &usage)

	usage.NonTransactionalUsage = getNonTransactionalUsage()

	usage.GlobalKill = getGlobalKillUsageInfo()

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

// ClusterIndexUsage records the usage info of all the tables, no more than 10k tables, deprecated.
type ClusterIndexUsage map[string]TableClusteredInfo

// TableClusteredInfo records the usage info of clusterindex of each table
// CLUSTERED, NON_CLUSTERED, NA
type TableClusteredInfo struct {
	IsClustered   bool   `json:"isClustered"`   // True means CLUSTERED, False means NON_CLUSTERED
	ClusterPKType string `json:"clusterPKType"` // INT means clustered PK type is int
	// NON_INT means clustered PK type is not int
	// NA means this field is no meaningful information
}

// NewClusterIndexUsage records the clustered index usage info of all the tables.
type NewClusterIndexUsage struct {
	// The number of user's tables with clustered index enabled.
	NumClusteredTables uint64 `json:"numClusteredTables"`
	// The number of user's tables.
	NumTotalTables uint64 `json:"numTotalTables"`
}

// getClusterIndexUsageInfo gets the ClusterIndex usage information. It's exported for future test.
func getClusterIndexUsageInfo(ctx sessionctx.Context) (ncu *NewClusterIndexUsage, cu *ClusterIndexUsage, err error) {
	var newUsage NewClusterIndexUsage
	exec := ctx.(sqlexec.RestrictedSQLExecutor)

	// query INFORMATION_SCHEMA.tables to get the latest table information about ClusterIndex
	rows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, `
		SELECT TIDB_PK_TYPE
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')`)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	// check ClusterIndex information for each table
	// row: 0 = TIDB_PK_TYPE
	for _, row := range rows {
		if row.Len() < 1 {
			continue
		}
		if row.GetString(0) == "CLUSTERED" {
			newUsage.NumClusteredTables++
		}
	}
	newUsage.NumTotalTables = uint64(len(rows))
	return &newUsage, &emptyClusterIndexUsage, nil
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
var initialNonTransactionalCounter m.NonTransactionalStmtCounter

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

func getNonTransactionalUsage() *m.NonTransactionalStmtCounter {
	curr := m.GetNonTransactionalStmtCounter()
	diff := curr.Sub(initialNonTransactionalCounter)
	return &diff
}

func postReportNonTransactionalCounter() {
	initialNonTransactionalCounter = m.GetNonTransactionalStmtCounter()
}

func getGlobalKillUsageInfo() bool {
	return config.GetGlobalConfig().EnableGlobalKill
}
