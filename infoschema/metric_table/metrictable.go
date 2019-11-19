package metric_table

import (
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"strconv"
)

type metricTableDef struct {
	promQL        string
	labels        []string
	quantile      float64
	rangeDuration int64 // unit is second.
}

const (
	promQLQuantileKey       = "$QUANTILE"
	promQLLabelConditionKey = "$LABEL_CONDITION"
	promQRangeDurationKey   = "$RANGE_DURATION"
)

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

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}

func buildColumnInfo(col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob {
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
		mFlag = col.flag
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      col.tp,
		Flen:    col.size,
		Flag:    mFlag,
	}
	return &model.ColumnInfo{
		Name:         model.NewCIStr(col.name),
		FieldType:    fieldType,
		State:        model.StatePublic,
		DefaultValue: col.deflt,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	for _, c := range cs {
		cols = append(cols, buildColumnInfo(c))
	}
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
}
