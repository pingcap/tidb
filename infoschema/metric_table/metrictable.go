package metric_table

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
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
	"up": {
		promQL: `up`,
		labels: []string{"instance", "job"},
	},
}

func (def *metricTableDef) genColumnInfos() []columnInfo {
	cols := []columnInfo{
		{"time", mysql.TypeDatetime, 19, 0, nil, nil},
		{"metric", mysql.TypeVarchar, 100, 0, nil, nil},
		{"value", mysql.TypeDouble, 22, 0, nil, nil},
		{"start_time", mysql.TypeDatetime, 19, 0, "CURRENT_TIMESTAMP", nil},
		{"end_time", mysql.TypeDatetime, 19, 0, "CURRENT_TIMESTAMP", nil},
		{"step", mysql.TypeLonglong, 21, 0, strconv.FormatInt(int64(getDefaultQueryRange().Step/time.Second), 10), nil},
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

func (def *metricTableDef) genRows(value pmodel.Value, r promQLQueryRange) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:
		matrix := value.(pmodel.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := def.genRecord(m.Metric, v, r)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (def *metricTableDef) genRecord(metric pmodel.Metric, pair pmodel.SamplePair, r promQLQueryRange) []types.Datum {
	record := make([]types.Datum, 0, 8)
	// Record order should keep same with genColumnInfos.
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))
	if metric != nil {
		record = append(record, types.NewStringDatum(metric.String()))
	} else {
		record = append(record, types.NewStringDatum(""))
	}
	record = append(record, types.NewFloat64Datum(float64(pair.Value)))
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(r.Start),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(r.End),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))

	record = append(record, types.NewIntDatum(int64(r.Step/time.Second)))
	for _, label := range def.labels {
		v := ""
		if metric != nil {
			v = string(metric[pmodel.LabelName(label)])
		}
		record = append(record, types.NewStringDatum(v))
	}
	if def.quantile > 0 {
		record = append(record, types.NewFloat64Datum(def.quantile))
	}
	if def.rangeDuration > 0 {
		record = append(record, types.NewIntDatum(def.rangeDuration))
	}
	return record
}

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}
