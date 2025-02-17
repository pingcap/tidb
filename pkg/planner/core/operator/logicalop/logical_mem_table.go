// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalMemTable represents a memory table or virtual table
// Some memory tables wants to take the ownership of some predications
// e.g
// SELECT * FROM cluster_log WHERE type='tikv' AND address='192.16.5.32'
// Assume that the table `cluster_log` is a memory table, which is used
// to retrieve logs from remote components. In the above situation we should
// send log search request to the target TiKV (192.16.5.32) directly instead of
// requesting all cluster components log search gRPC interface to retrieve
// log message and filtering them in TiDB node.
type LogicalMemTable struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	Extractor base.MemTablePredicateExtractor
	DBName    ast.CIStr        `hash64-equals:"true"`
	TableInfo *model.TableInfo `hash64-equals:"true"`
	Columns   []*model.ColumnInfo
	// QueryTimeRange is used to specify the time range for metrics summary tables and inspection tables
	// e.g: select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary_by_label;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_result;
	QueryTimeRange util.QueryTimeRange
}

// Init initializes LogicalMemTable.
func (p LogicalMemTable) Init(ctx base.PlanContext, offset int) *LogicalMemTable {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeMemTableScan, &p, offset)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalMemTable) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	if p.Extractor != nil {
		failpoint.Inject("skipExtractor", func(_ failpoint.Value) {
			failpoint.Return(predicates, p.Self())
		})
		predicates = p.Extractor.Extract(p.SCtx(), p.Schema(), p.OutputNames(), predicates)
	}
	return predicates, p.Self()
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalMemTable) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	switch p.TableInfo.Name.O {
	case infoschema.TableStatementsSummary,
		infoschema.TableStatementsSummaryHistory,
		infoschema.TableTiDBStatementsStats,
		infoschema.TableSlowQuery,
		infoschema.ClusterTableStatementsSummary,
		infoschema.ClusterTableStatementsSummaryHistory,
		infoschema.ClusterTableTiDBStatementsStats,
		infoschema.ClusterTableSlowLog,
		infoschema.TableTiDBTrx,
		infoschema.ClusterTableTiDBTrx,
		infoschema.TableDataLockWaits,
		infoschema.TableDeadlocks,
		infoschema.ClusterTableDeadlocks,
		infoschema.TableTables:
	default:
		return p, nil
	}
	prunedColumns := make([]*expression.Column, 0)
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && p.Schema().Len() > 1 {
			prunedColumns = append(prunedColumns, p.Schema().Columns[i])
			p.Schema().Columns = append(p.Schema().Columns[:i], p.Schema().Columns[i+1:]...)
			p.SetOutputNames(append(p.OutputNames()[:i], p.OutputNames()[i+1:]...))
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(p, prunedColumns, opt)
	return p, nil
}

// FindBestTask implements the base.LogicalPlan.<3rd> interface.
func (p *LogicalMemTable) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	return utilfuncp.FindBestTask4LogicalMemTable(p, prop, planCounter, opt)
}

// BuildKeyInfo inherits BaseLogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (p *LogicalMemTable) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	statsTable := statistics.PseudoTable(p.TableInfo, false, false)
	stats := &property.StatsInfo{
		RowCount:     float64(statsTable.RealtimeCount),
		ColNDVs:      make(map[int64]float64, len(p.TableInfo.Columns)),
		HistColl:     statsTable.GenerateHistCollFromColumnInfo(p.TableInfo, p.Schema().Columns),
		StatsVersion: statistics.PseudoVersion,
	}
	for _, col := range selfSchema.Columns {
		stats.ColNDVs[col.UniqueID] = float64(statsTable.RealtimeCount)
	}
	p.SetStats(stats)
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans inherits BaseLogicalPlan.LogicalPlan.<14th> implementation.

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************
