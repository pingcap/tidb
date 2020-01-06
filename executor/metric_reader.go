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
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema/metricschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
)

const promReadTimeout = time.Second * 10

// MetricRetriever uses to read metric data.
type MetricRetriever struct {
	dummyCloser
	table     *model.TableInfo
	tblDef    *metricschema.MetricTableDef
	extractor *plannercore.MetricTableExtractor
	retrieved bool
}

func (e *MetricRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	tblDef, err := metricschema.GetMetricTableDef(e.table.Name.L)
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
		queryValue, err := e.queryMetric(ctx, sctx, queryRange, quantile)
		if err != nil {
			return nil, err
		}

		partRows := e.genRows(queryValue, queryRange, quantile)
		totalRows = append(totalRows, partRows...)
	}
	return totalRows, nil
}

func (e *MetricRetriever) queryMetric(ctx context.Context, sctx sessionctx.Context, queryRange promv1.Range, quantile float64) (pmodel.Value, error) {
	failpoint.InjectContext(ctx, "mockMetricRetrieverQueryPromQL", func() {
		failpoint.Return(ctx.Value("__mockMetricsData").(pmodel.Matrix), nil)
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

func (e *MetricRetriever) genRows(value pmodel.Value, r promQLQueryRange, quantile float64) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:
		matrix := value.(pmodel.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := e.genRecord(m.Metric, v, r, quantile)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (e *MetricRetriever) genRecord(metric pmodel.Metric, pair pmodel.SamplePair, r promQLQueryRange, quantile float64) []types.Datum {
	record := make([]types.Datum, 0, 2+len(e.tblDef.Labels)+1)
	// Record order should keep same with genColumnInfos.
	record = append(record, types.NewTimeDatum(types.NewTime(
		types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		mysql.TypeDatetime,
		types.MaxFsp,
	)))
	record = append(record, types.NewFloat64Datum(float64(pair.Value)))
	for _, label := range e.tblDef.Labels {
		v := ""
		if metric != nil {
			v = string(metric[pmodel.LabelName(label)])
		}
		if len(v) == 0 {
			v = metricschema.GenLabelConditionValues(e.extractor.LabelConditions[strings.ToLower(label)])
		}
		record = append(record, types.NewStringDatum(v))
	}
	if e.tblDef.Quantile > 0 {
		record = append(record, types.NewFloat64Datum(quantile))
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
