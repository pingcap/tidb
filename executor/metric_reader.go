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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema/metricschema"
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
	table      *model.TableInfo
	tblDef     *metricschema.MetricTableDef
	outputCols []*model.ColumnInfo
	retrieved  bool
}

func (e *MetricRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) (fullRows [][]types.Datum, err error) {
	if e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	tblDef, err := metricschema.GetMetricTableDef(e.table.Name.L)
	if err != nil {
		return nil, err
	}
	e.tblDef = tblDef
	// TODO: Get query range from plan instead of use default range.
	queryRange := e.getDefaultQueryRange(sctx)
	queryValue, err := e.queryMetric(ctx, sctx, queryRange)
	if err != nil {
		return nil, err
	}

	fullRows = e.genRows(queryValue, queryRange)
	if len(e.outputCols) == len(e.table.Columns) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(e.outputCols))
		for j, col := range e.outputCols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

func (e *MetricRetriever) queryMetric(ctx context.Context, sctx sessionctx.Context, queryRange promv1.Range) (pmodel.Value, error) {
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

	// TODO: add label condition.
	promQL := e.tblDef.GenPromQL(sctx, nil)
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

func (e *MetricRetriever) getDefaultQueryRange(sctx sessionctx.Context) promQLQueryRange {
	return promQLQueryRange{Start: time.Now(), End: time.Now(), Step: time.Second * time.Duration(sctx.GetSessionVars().MetricSchemaStep)}
}

func (e *MetricRetriever) genRows(value pmodel.Value, r promQLQueryRange) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:
		matrix := value.(pmodel.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := e.genRecord(m.Metric, v, r)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (e *MetricRetriever) genRecord(metric pmodel.Metric, pair pmodel.SamplePair, r promQLQueryRange) []types.Datum {
	record := make([]types.Datum, 0, 2+len(e.tblDef.Labels)+1)
	// Record order should keep same with genColumnInfos.
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))
	record = append(record, types.NewFloat64Datum(float64(pair.Value)))
	for _, label := range e.tblDef.Labels {
		v := ""
		if metric != nil {
			v = string(metric[pmodel.LabelName(label)])
		}
		record = append(record, types.NewStringDatum(v))
	}
	if e.tblDef.Quantile > 0 {
		record = append(record, types.NewFloat64Datum(e.tblDef.Quantile))
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
