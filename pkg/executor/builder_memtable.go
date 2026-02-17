// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
)

func (b *executorBuilder) buildMemTable(v *physicalop.PhysicalMemTable) exec.Executor {
	switch v.DBName.L {
	case metadef.MetricSchemaName.L:
		return &MemTableReaderExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			table:        v.Table,
			retriever: &MetricRetriever{
				table:     v.Table,
				extractor: v.Extractor.(*plannercore.MetricTableExtractor),
			},
		}
	case metadef.InformationSchemaName.L:
		switch v.Table.Name.L {
		case strings.ToLower(infoschema.TableClusterConfig):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterConfigRetriever{
					extractor: v.Extractor.(*plannercore.ClusterTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableClusterLoad):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_LoadInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterHardware):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_HardwareInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterSystemInfo):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_SystemInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterLog):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterLogRetriever{
					extractor: v.Extractor.(*plannercore.ClusterLogTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableTiDBHotRegionsHistory):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hotRegionsHistoryRetriver{
					extractor: v.Extractor.(*plannercore.HotRegionsHistoryTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableInspectionResult):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionResultRetriever{
					extractor: v.Extractor.(*plannercore.InspectionResultTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionSummary):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.InspectionSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionRules):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionRuleRetriever{
					extractor: v.Extractor.(*plannercore.InspectionRuleTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableMetricSummary):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableMetricSummaryByLabel):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryByLabelRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableTiKVRegionPeers):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tikvRegionPeersRetriever{
					extractor: v.Extractor.(*plannercore.TikvRegionPeersExtractor),
				},
			}
		case strings.ToLower(infoschema.TableSchemata),
			strings.ToLower(infoschema.TableStatistics),
			strings.ToLower(infoschema.TableTiDBIndexes),
			strings.ToLower(infoschema.TableViews),
			strings.ToLower(infoschema.TableTables),
			strings.ToLower(infoschema.TableReferConst),
			strings.ToLower(infoschema.TableSequences),
			strings.ToLower(infoschema.TablePartitions),
			strings.ToLower(infoschema.TableEngines),
			strings.ToLower(infoschema.TableCollations),
			strings.ToLower(infoschema.TableAnalyzeStatus),
			strings.ToLower(infoschema.TableClusterInfo),
			strings.ToLower(infoschema.TableProfiling),
			strings.ToLower(infoschema.TableCharacterSets),
			strings.ToLower(infoschema.TableKeyColumn),
			strings.ToLower(infoschema.TableUserPrivileges),
			strings.ToLower(infoschema.TableMetricTables),
			strings.ToLower(infoschema.TableCollationCharacterSetApplicability),
			strings.ToLower(infoschema.TableProcesslist),
			strings.ToLower(infoschema.ClusterTableProcesslist),
			strings.ToLower(infoschema.TableTiKVRegionStatus),
			strings.ToLower(infoschema.TableTiDBHotRegions),
			strings.ToLower(infoschema.TableConstraints),
			strings.ToLower(infoschema.TableTiFlashReplica),
			strings.ToLower(infoschema.TableTiDBServersInfo),
			strings.ToLower(infoschema.TableTiKVStoreStatus),
			strings.ToLower(infoschema.TableClientErrorsSummaryGlobal),
			strings.ToLower(infoschema.TableClientErrorsSummaryByUser),
			strings.ToLower(infoschema.TableClientErrorsSummaryByHost),
			strings.ToLower(infoschema.TableAttributes),
			strings.ToLower(infoschema.TablePlacementPolicies),
			strings.ToLower(infoschema.TableTrxSummary),
			strings.ToLower(infoschema.TableVariablesInfo),
			strings.ToLower(infoschema.TableUserAttributes),
			strings.ToLower(infoschema.ClusterTableTrxSummary),
			strings.ToLower(infoschema.TableMemoryUsage),
			strings.ToLower(infoschema.TableMemoryUsageOpsHistory),
			strings.ToLower(infoschema.ClusterTableMemoryUsage),
			strings.ToLower(infoschema.ClusterTableMemoryUsageOpsHistory),
			strings.ToLower(infoschema.TableResourceGroups),
			strings.ToLower(infoschema.TableRunawayWatches),
			strings.ToLower(infoschema.TableCheckConstraints),
			strings.ToLower(infoschema.TableTiDBCheckConstraints),
			strings.ToLower(infoschema.TableKeywords),
			strings.ToLower(infoschema.TableTiDBIndexUsage),
			strings.ToLower(infoschema.TableTiDBPlanCache),
			strings.ToLower(infoschema.ClusterTableTiDBPlanCache),
			strings.ToLower(infoschema.ClusterTableTiDBIndexUsage),
			strings.ToLower(infoschema.TableKeyspaceMeta):
			memTracker := memory.NewTracker(v.ID(), -1)
			memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &memtableRetriever{
					table:      v.Table,
					columns:    v.Columns,
					extractor:  v.Extractor,
					memTracker: memTracker,
				},
			}
		case strings.ToLower(infoschema.TableTiDBTrx),
			strings.ToLower(infoschema.ClusterTableTiDBTrx):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tidbTrxTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDataLockWaits):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &dataLockWaitsTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDeadlocks),
			strings.ToLower(infoschema.ClusterTableDeadlocks):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &deadlocksTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableStatementsSummary),
			strings.ToLower(infoschema.TableStatementsSummaryHistory),
			strings.ToLower(infoschema.TableStatementsSummaryEvicted),
			strings.ToLower(infoschema.TableTiDBStatementsStats),
			strings.ToLower(infoschema.ClusterTableStatementsSummary),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryHistory),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryEvicted),
			strings.ToLower(infoschema.ClusterTableTiDBStatementsStats):
			var extractor *plannercore.StatementsSummaryExtractor
			if v.Extractor != nil {
				extractor = v.Extractor.(*plannercore.StatementsSummaryExtractor)
			}
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever:    buildStmtSummaryRetriever(v.Table, v.Columns, extractor),
			}
		case strings.ToLower(infoschema.TableColumns):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hugeMemTableRetriever{
					table:              v.Table,
					columns:            v.Columns,
					extractor:          v.Extractor.(*plannercore.InfoSchemaColumnsExtractor),
					viewSchemaMap:      make(map[int64]*expression.Schema),
					viewOutputNamesMap: make(map[int64]types.NameSlice),
				},
			}
		case strings.ToLower(infoschema.TableSlowQuery), strings.ToLower(infoschema.ClusterTableSlowLog):
			extractor := v.Extractor.(*plannercore.SlowQueryExtractor)
			memTracker := memory.NewTracker(v.ID(), -1)
			memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &slowQueryRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  extractor,
					limit:      extractor.Limit,
					memTracker: memTracker,
				},
			}
		case strings.ToLower(infoschema.TableStorageStats):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tableStorageStatsRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.TableStorageStatsExtractor),
				},
			}
		case strings.ToLower(infoschema.TableDDLJobs):
			loc := b.ctx.GetSessionVars().Location()
			ddlJobRetriever := DDLJobRetriever{TZLoc: loc, extractor: v.Extractor}
			return &DDLJobsReaderExec{
				BaseExecutor:    exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				is:              b.is,
				DDLJobRetriever: ddlJobRetriever,
			}
		case strings.ToLower(infoschema.TableTiFlashTables),
			strings.ToLower(infoschema.TableTiFlashSegments),
			strings.ToLower(infoschema.TableTiFlashIndexes):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &TiFlashSystemTableRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.TiFlashSystemTableExtractor),
				},
			}
		}
	}
	tb, _ := b.is.TableByID(context.Background(), v.Table.ID)
	return &TableScanExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		t:            tb,
		columns:      v.Columns,
	}
}

