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

package metricschema

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
)

const (
	promQLQuantileKey       = "$QUANTILE"
	promQLLabelConditionKey = "$LABEL_CONDITIONS"
	promQRangeDurationKey   = "$RANGE_DURATION"
)

// MetricTableDef is the metric table define.
type MetricTableDef struct {
	PromQL   string
	Labels   []string
	Quantile float64
}

// TODO: read from system table.
var metricTableMap = map[string]MetricTableDef{
	"query_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le))`,
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.90,
	},
	"up": {
		PromQL: `up{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "job"},
	},
}

// IsMetricTable uses to checks whether the table is a metric table.
func IsMetricTable(lowerTableName string) bool {
	_, ok := metricTableMap[lowerTableName]
	return ok
}

// GetMetricTableDef gets the metric table define.
func GetMetricTableDef(lowerTableName string) (*MetricTableDef, error) {
	def, ok := metricTableMap[lowerTableName]
	if !ok {
		return nil, errors.Errorf("can not find metric table: %v", lowerTableName)
	}
	return &def, nil
}

// GetExplainInfo uses to get the explain info of metric table.
func GetExplainInfo(sctx sessionctx.Context, lowerTableName string) string {
	def, ok := metricTableMap[lowerTableName]
	if !ok {
		return ""
	}
	promQL := def.GenPromQL(sctx, nil)
	return "PromQL:" + promQL
}

func (def *MetricTableDef) genColumnInfos() []columnInfo {
	cols := []columnInfo{
		{"time", mysql.TypeDatetime, 19, 0, "CURRENT_TIMESTAMP", nil},
		{"value", mysql.TypeDouble, 22, 0, nil, nil},
	}
	for _, label := range def.Labels {
		cols = append(cols, columnInfo{label, mysql.TypeVarchar, 512, 0, nil, nil})
	}
	if def.Quantile > 0 {
		defaultValue := strconv.FormatFloat(def.Quantile, 'f', -1, 64)
		cols = append(cols, columnInfo{"Quantile", mysql.TypeDouble, 22, 0, defaultValue, nil})
	}
	return cols
}

// GenPromQL generates the promQL.
func (def *MetricTableDef) GenPromQL(sctx sessionctx.Context, labels []string) string {
	promQL := def.PromQL
	if strings.Contains(promQL, promQLQuantileKey) {
		promQL = strings.Replace(promQL, promQLQuantileKey, strconv.FormatFloat(def.Quantile, 'f', -1, 64), -1)
	}

	if strings.Contains(promQL, promQLLabelConditionKey) {
		promQL = strings.Replace(promQL, promQLLabelConditionKey, "", -1)
	}

	if strings.Contains(promQL, promQRangeDurationKey) {
		promQL = strings.Replace(promQL, promQRangeDurationKey, strconv.FormatInt(sctx.GetSessionVars().MetricSchemaRangeDuration, 10)+"s", -1)
	}
	return promQL
}

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}
