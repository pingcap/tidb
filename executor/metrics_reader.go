// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
)

const promReadTimeout = time.Second * 10

// MetricRetriever uses to read metric data.
type MetricRetriever struct {
	dummyCloser
	table     *model.TableInfo
	tblDef    *infoschema.MetricTableDef
	extractor *plannercore.MetricTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true

	failpoint.InjectContext(ctx, "mockMetricsTableData", func() {
		m, ok := ctx.Value("__mockMetricsTableData").(map[string][][]types.Datum)
		if ok && m[e.table.Name.L] != nil {
			failpoint.Return(m[e.table.Name.L], nil)
		}
	})

	tblDef, err := infoschema.GetMetricTableDef(e.table.Name.L)
	if err != nil {
		return nil, err
	}
	e.tblDef = tblDef
	queryRange := e.getQueryRange(sctx)
	totalRows := make([][]types.Datum, 0)
	quantiles := e.extractor.Quantiles
	if len(quantiles) == 0 {
		quantiles = []float64{tblDef.Quantile}
	}
	for _, quantile := range quantiles {
		var queryValue pmodel.Value
		// Add retry to avoid network error.
		for i := 0; i < 10; i++ {
			queryValue, err = e.queryMetric(ctx, sctx, queryRange, quantile)
			if err == nil || strings.Contains(err.Error(), "parse error") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			return nil, err
		}
		partRows := e.genRows(queryValue, quantile)
		totalRows = append(totalRows, partRows...)
	}
	return totalRows, nil
}

func (e *MetricRetriever) queryMetric(ctx context.Context, sctx sessionctx.Context, queryRange promv1.Range, quantile float64) (pmodel.Value, error) {
	failpoint.InjectContext(ctx, "mockMetricsPromData", func() {
		failpoint.Return(ctx.Value("__mockMetricsPromData").(pmodel.Matrix), nil)
	})

	addr, err := e.getMetricAddr(sctx)
	if err != nil {
		return nil, err
	}

	queryClient, err := newQueryClient(addr)
	if err != nil {
		return nil, err
	}

	promQLAPI := promv1.NewAPI(queryClient)
	ctx, cancel := context.WithTimeout(ctx, promReadTimeout)
	defer cancel()

	promQL := e.tblDef.GenPromQL(sctx, e.extractor.LabelConditions, quantile)
	result, _, err := promQLAPI.QueryRange(ctx, promQL, queryRange)
	return result, err
}

func (e *MetricRetriever) getMetricAddr(sctx sessionctx.Context) (string, error) {
	// Get PD servers info.
	store := sctx.GetStore()
	etcd, ok := store.(tikv.EtcdBackend)
	if !ok {
		return "", errors.Errorf("%T not an etcd backend", store)
	}
	for _, addr := range etcd.EtcdAddrs() {
		return addr, nil
	}
	return "", errors.Errorf("pd address was not found")
}

type promQLQueryRange = promv1.Range

func (e *MetricRetriever) getQueryRange(sctx sessionctx.Context) promQLQueryRange {
	startTime, endTime := e.extractor.StartTime, e.extractor.EndTime
	step := time.Second * time.Duration(sctx.GetSessionVars().MetricSchemaStep)
	return promQLQueryRange{Start: startTime, End: endTime, Step: step}
}

func (e *MetricRetriever) genRows(value pmodel.Value, quantile float64) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:
		matrix := value.(pmodel.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := e.genRecord(m.Metric, v, quantile)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (e *MetricRetriever) genRecord(metric pmodel.Metric, pair pmodel.SamplePair, quantile float64) []types.Datum {
	record := make([]types.Datum, 0, 2+len(e.tblDef.Labels)+1)
	// Record order should keep same with genColumnInfos.
	record = append(record, types.NewTimeDatum(types.NewTime(
		types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		mysql.TypeDatetime,
		types.MaxFsp,
	)))
	for _, label := range e.tblDef.Labels {
		v := ""
		if metric != nil {
			v = string(metric[pmodel.LabelName(label)])
		}
		if len(v) == 0 {
			v = infoschema.GenLabelConditionValues(e.extractor.LabelConditions[strings.ToLower(label)])
		}
		record = append(record, types.NewStringDatum(v))
	}
	if e.tblDef.Quantile > 0 {
		record = append(record, types.NewFloat64Datum(quantile))
	}
	if math.IsNaN(float64(pair.Value)) {
		record = append(record, types.NewDatum(nil))
	} else {
		record = append(record, types.NewFloat64Datum(float64(pair.Value)))
	}
	return record
}

type queryClient struct {
	api.Client
}

func newQueryClient(addr string) (api.Client, error) {
	promClient, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", addr),
	})
	if err != nil {
		return nil, err
	}
	return &queryClient{
		promClient,
	}, nil
}

// URL implement the api.Client interface.
// This is use to convert prometheus api path to PD API path.
func (c *queryClient) URL(ep string, args map[string]string) *url.URL {
	// FIXME: add `PD-Allow-follower-handle: true` in http header, let pd follower can handle this request too.
	ep = strings.Replace(ep, "api/v1", "pd/api/v1/metric", 1)
	return c.Client.URL(ep, args)
}

// MetricsSummaryRetriever uses to read metric data.
type MetricsSummaryRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricSummaryTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryRetriever) retrieve(_ context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range tables {
		if !filter.enable(name) {
			continue
		}
		def, found := infoschema.MetricTableMap[name]
		if !found {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", name))
			continue
		}
		var sql string
		if def.Quantile > 0 {
			var qs []string
			if len(e.extractor.Quantiles) > 0 {
				for _, q := range e.extractor.Quantiles {
					qs = append(qs, fmt.Sprintf("%f", q))
				}
			} else {
				qs = []string{"0.99"}
			}
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),quantile from `%[2]s`.`%[1]s` %[3]s and quantile in (%[4]s) group by quantile order by quantile",
				name, util.MetricSchemaName.L, condition, strings.Join(qs, ","))
		} else {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%[2]s`.`%[1]s` %[3]s",
				name, util.MetricSchemaName.L, condition)
		}

		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", sql, err)
		}
		for _, row := range rows {
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = row.GetFloat64(row.Len() - 1)
			}
			totalRows = append(totalRows, types.MakeDatums(
				name,
				quantile,
				row.GetFloat64(0),
				row.GetFloat64(1),
				row.GetFloat64(2),
				row.GetFloat64(3),
				def.Comment,
			))
		}
	}
	return totalRows, nil
}

// MetricsSummaryByLabelRetriever uses to read metric detail data.
type MetricsSummaryByLabelRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricSummaryTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryByLabelRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range tables {
		if !filter.enable(name) {
			continue
		}
		def, found := infoschema.MetricTableMap[name]
		if !found {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", name))
			continue
		}
		cols := def.Labels
		cond := condition
		if def.Quantile > 0 {
			cols = append(cols, "quantile")
			if len(e.extractor.Quantiles) > 0 {
				qs := make([]string, len(e.extractor.Quantiles))
				for i, q := range e.extractor.Quantiles {
					qs[i] = fmt.Sprintf("%f", q)
				}
				cond += " and quantile in (" + strings.Join(qs, ",") + ")"
			} else {
				cond += " and quantile=0.99"
			}
		}
		var sql string
		if len(cols) > 0 {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),`%s` from `%s`.`%s` %s group by `%[1]s` order by `%[1]s`",
				strings.Join(cols, "`,`"), util.MetricSchemaName.L, name, cond)
		} else {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%s`.`%s` %s",
				util.MetricSchemaName.L, name, cond)
		}
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", sql, err)
		}
		nonInstanceLabelIndex := 0
		if len(def.Labels) > 0 && def.Labels[0] == "instance" {
			nonInstanceLabelIndex = 1
		}
		// skip sum/avg/min/max
		const skipCols = 4
		for _, row := range rows {
			instance := ""
			if nonInstanceLabelIndex > 0 {
				instance = row.GetString(skipCols) // sum/avg/min/max
			}
			var labels []string
			for i, label := range def.Labels[nonInstanceLabelIndex:] {
				// skip min/max/avg/instance
				val := row.GetString(skipCols + nonInstanceLabelIndex + i)
				if label == "store" || label == "store_id" {
					val = fmt.Sprintf("store_id:%s", val)
				}
				labels = append(labels, val)
			}
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = row.GetFloat64(row.Len() - 1) // quantile will be the last column
			}
			totalRows = append(totalRows, types.MakeDatums(
				instance,
				name,
				strings.Join(labels, ", "),
				quantile,
				row.GetFloat64(0), // sum
				row.GetFloat64(1), // avg
				row.GetFloat64(2), // min
				row.GetFloat64(3), // max
				def.Comment,
			))
		}
	}
	return totalRows, nil
}
