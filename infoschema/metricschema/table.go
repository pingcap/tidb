package metricschema

import (
	"github.com/pingcap/tidb/sessionctx"
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

const metricDBName = "METRIC_SCHEMA"

type metricTableDef struct {
	promQL   string
	labels   []string
	quantile float64
}

var metricTableMap = map[string]metricTableDef{
	"query_duration": {
		promQL:   `histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITION}[$RANGE_DURATION])) by (le))`,
		labels:   []string{"instance", "sql_type"},
		quantile: 0.90,
	},
	"up": {
		promQL: `up{$LABEL_CONDITION}`,
		labels: []string{"instance", "job"},
	},
}

func (def *metricTableDef) genColumnInfos() []columnInfo {
	cols := []columnInfo{
		{"time", mysql.TypeDatetime, 19, 0, "CURRENT_TIMESTAMP", nil},
		{"value", mysql.TypeDouble, 22, 0, nil, nil},
	}
	for _, label := range def.labels {
		cols = append(cols, columnInfo{label, mysql.TypeVarchar, 512, 0, nil, nil})
	}
	if def.quantile > 0 {
		defaultValue := strconv.FormatFloat(def.quantile, 'f', -1, 64)
		cols = append(cols, columnInfo{"quantile", mysql.TypeDouble, 22, 0, defaultValue, nil})
	}
	return cols
}

func (def *metricTableDef) genPromQL(sctx sessionctx.Context, labels []string) string {
	promQL := def.promQL
	if strings.Contains(promQL, promQLQuantileKey) {
		promQL = strings.Replace(promQL, promQLQuantileKey, strconv.FormatFloat(def.quantile, 'f', -1, 64), -1)
	}

	// TODO: add label condition.
	if strings.Contains(promQL, promQLLabelConditionKey) {
		promQL = strings.Replace(promQL, promQLLabelConditionKey, "", -1)
	}

	if strings.Contains(promQL, promQRangeDurationKey) {
		promQL = strings.Replace(promQL, promQRangeDurationKey, strconv.FormatInt(sctx.GetSessionVars().MetricSchemaRangeDuration, 10)+"s", -1)
	}
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
	record = append(record, types.NewFloat64Datum(float64(pair.Value)))
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
