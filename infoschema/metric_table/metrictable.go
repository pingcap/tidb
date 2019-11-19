package metric_table

import (
	"fmt"
	"github.com/pingcap/tidb/types"
	"strconv"
	"strings"

	"github.com/pingcap/parser/mysql"
	pmodel "github.com/prometheus/common/model"
)

const (
	promQLQuantileKey       = "$QUANTILE"
	promQLLabelConditionKey = "$LABEL_CONDITION"
	promQRangeDurationKey   = "$RANGE_DURATION"
)

const (
	MetricDBName = "METRIC"
)

type metricTableDef struct {
	promQL        string
	labels        []string
	quantile      float64
	rangeDuration int64 // unit is second.
}

var metricTableMap = map[string]metricTableDef{
	"query_duration": {
		promQL:        `histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITION}[$RANGE_DURATION])) by (le))`,
		labels:        []string{"sql_type"},
		quantile:      0.90,
		rangeDuration: 60,
	},
}

func (def *metricTableDef) genColumnInfos() []columnInfo {
	cols := []columnInfo{
		{"time", mysql.TypeDatetime, 19, 0, nil, nil},
		{"value", mysql.TypeDouble, 22, 0, nil, nil},
		{"start_time", mysql.TypeDatetime, 19, 0, nil, nil},
		{"end_time", mysql.TypeDatetime, 19, 0, nil, nil},
		{"step", mysql.TypeLonglong, 21, 0, nil, nil},
	}
	for _, label := range def.labels {
		cols = append(cols, columnInfo{label, mysql.TypeVarchar, 512, 0, nil, nil})
	}
	if def.quantile > 0 {
		defaultValue := strconv.FormatFloat(def.quantile, 'f', -1, 64)
		cols = append(cols, columnInfo{"quantile", mysql.TypeDouble, 22, 0, defaultValue, nil})
	}
	if def.rangeDuration > 0 {
		cols = append(cols, columnInfo{"range_duration", mysql.TypeLonglong, 21, 0, nil, nil})
	}
	return cols
}

func (def *metricTableDef) genPromQL(labels []string) string {
	promQL := def.promQL
	if strings.Contains(promQL, promQLQuantileKey) {
		promQL = strings.Replace(promQL, promQLQuantileKey, strconv.FormatFloat(def.quantile, 'f', -1, 64), -1)
	}

	// TODO: add label condition.
	if strings.Contains(promQL, promQLLabelConditionKey) {
		promQL = strings.Replace(promQL, promQLLabelConditionKey, "", -1)
	}

	if strings.Contains(promQL, promQRangeDurationKey) {
		promQL = strings.Replace(promQL, promQRangeDurationKey, strconv.FormatInt(def.rangeDuration, 10)+"s", -1)
	}
	fmt.Printf("gen promQL: %v\n\n", promQL)
	return promQL
}

func (def *metricTableDef) genRows(value pmodel.Value) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:

	}
}

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}
