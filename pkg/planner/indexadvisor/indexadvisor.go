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

package indexadvisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
	s "github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

// TestKey is the key for test context.
func TestKey(key string) string {
	return "__test_index_advisor_" + key
}

// Option is the option for the index advisor.
type Option struct {
	MaxNumIndexes int
	MaxIndexWidth int
	MaxNumQuery   int
	Timeout       time.Duration
	SpecifiedSQLs []string
}

// AdviseIndexes is the only entry point for the index advisor.
func AdviseIndexes(ctx context.Context, sctx sessionctx.Context,
	option *Option) (results []*Recommendation, err error) {
	if ctx == nil || sctx == nil || option == nil {
		return nil, errors.New("nil input")
	}

	advisorLogger().Info("fill index advisor option")
	if err := fillOption(sctx, option); err != nil {
		advisorLogger().Error("fill index advisor option failed", zap.Error(err))
		return nil, err
	}
	advisorLogger().Info("index advisor option filled and start", zap.Any("option", option))
	defer func() {
		if r := recover(); r != nil {
			advisorLogger().Error("panic in AdviseIndexes", zap.Any("recover", r))
			err = fmt.Errorf("panic in AdviseIndexes: %v", r)
		}
	}()

	// prepare what-if optimizer
	opt := NewOptimizer(sctx)
	advisorLogger().Info("what-if optimizer prepared")

	defaultDB := sctx.GetSessionVars().CurrentDB
	querySet, err := prepareQuerySet(ctx, sctx, defaultDB, opt, option)
	if err != nil {
		advisorLogger().Error("prepare workload failed", zap.Error(err))
		return nil, err
	}

	// identify indexable columns
	indexableColSet, err := CollectIndexableColumnsForQuerySet(opt, querySet)
	if err != nil {
		advisorLogger().Error("fill indexable columns failed", zap.Error(err))
		return nil, err
	}
	advisorLogger().Info("indexable columns filled", zap.Int("indexable-cols", indexableColSet.Size()))

	// start the advisor
	indexes, err := adviseIndexes(querySet, indexableColSet, opt, option)
	if err != nil {
		advisorLogger().Error("advise indexes failed", zap.Error(err))
		return nil, err
	}

	results, err = prepareRecommendation(indexes, querySet, opt)
	if err != nil {
		return nil, err
	}

	saveRecommendations(sctx, results)

	return results, nil
}

// prepareQuerySet prepares the target queries for the index advisor.
func prepareQuerySet(ctx context.Context, sctx sessionctx.Context,
	defaultDB string, opt Optimizer, option *Option) (s.Set[Query], error) {
	advisorLogger().Info("prepare target query set")
	querySet := s.NewSet[Query]()
	if len(option.SpecifiedSQLs) > 0 { // if target queries are specified
		for _, sql := range option.SpecifiedSQLs {
			querySet.Add(Query{SchemaName: defaultDB, Text: sql, Frequency: 1})
		}
	} else {
		if intest.InTest && ctx.Value(TestKey("query_set")) != nil {
			querySet = ctx.Value(TestKey("query_set")).(s.Set[Query])
		} else {
			var err error
			if querySet, err = loadQuerySetFromStmtSummary(sctx, option); err != nil {
				return nil, err
			}
			if querySet.Size() == 0 {
				return nil, errors.New("can't get any queries from statements_summary")
			}
		}
	}

	// filter invalid queries
	var err error
	querySet, err = RestoreSchemaName(defaultDB, querySet, len(option.SpecifiedSQLs) == 0)
	if err != nil {
		return nil, err
	}
	querySet, err = FilterSQLAccessingSystemTables(querySet, len(option.SpecifiedSQLs) == 0)
	if err != nil {
		return nil, err
	}
	querySet, err = FilterInvalidQueries(opt, querySet, len(option.SpecifiedSQLs) == 0)
	if err != nil {
		return nil, err
	}
	if querySet.Size() == 0 {
		return nil, errors.New("empty query set after filtering invalid queries")
	}
	advisorLogger().Info("finish query preparation", zap.Int("num_query", querySet.Size()))
	return querySet, nil
}

func loadQuerySetFromStmtSummary(sctx sessionctx.Context, option *Option) (s.Set[Query], error) {
	template := `SELECT any_value(schema_name) as schema_name,
				any_value(query_sample_text) as query_sample_text,
				sum(cast(exec_count as double)) as exec_count
			FROM information_schema.statements_summary_history
			WHERE stmt_type = "Select" AND
				summary_begin_time >= date_sub(now(), interval 1 day) AND
				prepared = 0 AND
				upper(schema_name) not in ("MYSQL", "INFORMATION_SCHEMA", "METRICS_SCHEMA", "PERFORMANCE_SCHEMA")
			GROUP BY digest
			ORDER BY sum(exec_count) DESC
			LIMIT %?`
	rows, err := exec(sctx, template, option.MaxNumQuery)
	if err != nil {
		return nil, err
	}

	querySet := s.NewSet[Query]()
	for _, r := range rows {
		schemaName := r.GetString(0)
		queryText := r.GetString(1)
		execCount := r.GetFloat64(2)
		querySet.Add(Query{
			SchemaName: schemaName,
			Text:       queryText,
			Frequency:  int(execCount),
		})
	}
	return querySet, nil
}

func prepareRecommendation(indexes s.Set[Index], queries s.Set[Query], optimizer Optimizer) ([]*Recommendation, error) {
	advisorLogger().Info("recommend index", zap.Int("num-index", indexes.Size()))
	results := make([]*Recommendation, 0, indexes.Size())
	for _, idx := range indexes.ToList() {
		workloadImpact := new(WorkloadImpact)
		var cols []string
		for _, col := range idx.Columns {
			cols = append(cols, strings.Trim(col.ColumnName, `'" `))
		}
		advisorLogger().Info("index columns", zap.Strings("columns", cols), zap.Any("index-cols", idx.Columns))
		indexResult := &Recommendation{
			Database:     idx.SchemaName,
			Table:        idx.TableName,
			IndexColumns: cols,
			IndexDetail:  new(IndexDetail),
		}

		// generate a graceful index name
		indexResult.IndexName = gracefulIndexName(optimizer, idx.SchemaName, idx.TableName, cols)
		advisorLogger().Info("graceful index name", zap.String("index-name", indexResult.IndexName))

		// calculate the index size
		indexSize, err := optimizer.EstIndexSize(idx.SchemaName, idx.TableName, cols...)
		if err != nil {
			advisorLogger().Info("show index stats failed", zap.Error(err))
			return nil, err
		}
		indexResult.IndexDetail.IndexSize = uint64(indexSize)

		// calculate the improvements
		var workloadCostBefore, workloadCostAfter float64
		impacts := make([]*ImpactedQuery, 0, queries.Size())
		for _, query := range queries.ToList() {
			costBefore, err := optimizer.QueryPlanCost(query.Text)
			if err != nil {
				advisorLogger().Info("failed to get query plan cost", zap.Error(err))
				return nil, err
			}
			costAfter, err := optimizer.QueryPlanCost(query.Text, idx)
			if err != nil {
				advisorLogger().Info("failed to get query plan cost", zap.Error(err))
				return nil, err
			}
			if costBefore == 0 { // avoid NaN
				costBefore += 0.1
				costAfter += 0.1
			}
			workloadCostBefore += costBefore * float64(query.Frequency)
			workloadCostAfter += costAfter * float64(query.Frequency)

			queryImprovement := round((costBefore-costAfter)/costBefore, 6)
			if queryImprovement < 0.0001 {
				continue // this query has no benefit
			}
			impacts = append(impacts, &ImpactedQuery{
				Query:       query.Text,
				Improvement: queryImprovement,
			})
		}

		sort.Slice(impacts, func(i, j int) bool {
			return impacts[i].Improvement > impacts[j].Improvement
		})

		topN := 3
		if topN > len(impacts) {
			topN = len(impacts)
		}
		indexResult.TopImpactedQueries = impacts[:topN]
		if workloadCostBefore == 0 { // avoid NaN
			workloadCostBefore += 0.1
			workloadCostAfter += 0.1
		}
		workloadImpact.WorkloadImprovement = round((workloadCostBefore-workloadCostAfter)/workloadCostBefore, 6)

		if workloadImpact.WorkloadImprovement < 0.000001 || len(indexResult.TopImpactedQueries) == 0 {
			continue // this index has no benefit
		}

		normText, _ := NormalizeDigest(indexResult.TopImpactedQueries[0].Query)
		indexResult.WorkloadImpact = workloadImpact
		indexResult.IndexDetail.Reason =
			fmt.Sprintf(`Column %v appear in Equal or Range Predicate clause(s) in query: %v`, cols, normText)
		results = append(results, indexResult)
	}
	return results, nil
}

func round(v float64, n int) float64 {
	return math.Round(v*math.Pow(10, float64(n))) / math.Pow(10, float64(n))
}

func gracefulIndexName(opt Optimizer, schema, tableName string, cols []string) string {
	indexName := fmt.Sprintf("idx_%v", strings.Join(cols, "_"))
	if len(indexName) > 64 {
		indexName = indexName[:64]
	}
	if ok, _ := opt.IndexNameExist(schema, tableName, strings.ToLower(indexName)); !ok {
		return indexName
	}
	indexName = fmt.Sprintf("idx_%v", cols[0])
	if len(indexName) > 64 {
		indexName = indexName[:64]
	}
	if ok, _ := opt.IndexNameExist(schema, tableName, strings.ToLower(indexName)); !ok {
		return indexName
	}
	for i := 0; i < 30; i++ {
		indexName = fmt.Sprintf("idx_%v_%v", cols[0], i)
		if len(indexName) > 64 {
			indexName = indexName[:64]
		}
		if ok, _ := opt.IndexNameExist(schema, tableName, strings.ToLower(indexName)); !ok {
			return indexName
		}
	}
	return indexName
}

func saveRecommendations(sctx sessionctx.Context, results []*Recommendation) {
	for _, r := range results {
		q, err := json.Marshal(r.TopImpactedQueries)
		if err != nil {
			advisorLogger().Error("marshal top impacted queries failed", zap.Error(err))
			continue
		}
		w, err := json.Marshal(r.WorkloadImpact)
		if err != nil {
			advisorLogger().Error("marshal workload impact failed", zap.Error(err))
			continue
		}
		d, err := json.Marshal(r.IndexDetail)
		if err != nil {
			advisorLogger().Error("marshal index detail failed", zap.Error(err))
			continue
		}

		template := `insert into mysql.index_advisor_results (
                created_at, updated_at, schema_name, table_name, index_name,
                index_columns, index_details, top_impacted_queries, workload_impact, extra) values
            (now(), now(), %?, %?, %?, %?, %?, %?, %?, null)
            on duplicate key update
            updated_at=now(), index_details=%?, top_impacted_queries=%?, workload_impact=%?`

		if _, err := exec(sctx, template, r.Database, r.Table,
			r.IndexName, strings.Join(r.IndexColumns, ","),
			json.RawMessage(d), json.RawMessage(q), json.RawMessage(w),
			json.RawMessage(d), json.RawMessage(q), json.RawMessage(w)); err != nil {
			advisorLogger().Error("save advise result failed", zap.Error(err))
		}
	}
}
