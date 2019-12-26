package infoschema

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/set"
)

const (
	promQLQuantileKey       = "$QUANTILE"
	promQLLabelConditionKey = "$LABEL_CONDITIONS"
	promQRangeDurationKey   = "$RANGE_DURATION"
)

func init() {
	// Initialize the metric schema database and register the driver to `drivers`.
	dbID := autoid.MetricSchemaDBID
	tableID := dbID + 1
	metricTables := make([]*model.TableInfo, 0, len(MetricTableMap))
	for name, def := range MetricTableMap {
		cols := def.genColumnInfos()
		tableInfo := buildTableMeta(name, cols)
		tableInfo.ID = tableID
		tableInfo.Comment = def.Comment
		tableID++
		metricTables = append(metricTables, tableInfo)
	}
	dbInfo := &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(util.MetricSchemaName.O),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  metricTables,
	}
	RegisterVirtualTable(dbInfo, tableFromMeta)
}

// MetricTableDef is the metric table define.
type MetricTableDef struct {
	PromQL   string
	Labels   []string
	Quantile float64
	Comment  string
}

// IsMetricTable uses to checks whether the table is a metric table.
func IsMetricTable(lowerTableName string) bool {
	_, ok := MetricTableMap[lowerTableName]
	return ok
}

// GetMetricTableDef gets the metric table define.
func GetMetricTableDef(lowerTableName string) (*MetricTableDef, error) {
	def, ok := MetricTableMap[lowerTableName]
	if !ok {
		return nil, errors.Errorf("can not find metric table: %v", lowerTableName)
	}
	return &def, nil
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
		cols = append(cols, columnInfo{"quantile", mysql.TypeDouble, 22, 0, defaultValue, nil})
	}
	return cols
}

// GenPromQL generates the promQL.
func (def *MetricTableDef) GenPromQL(sctx sessionctx.Context, labels map[string]set.StringSet, quantile float64) string {
	promQL := def.PromQL
	if strings.Contains(promQL, promQLQuantileKey) {
		promQL = strings.Replace(promQL, promQLQuantileKey, strconv.FormatFloat(quantile, 'f', -1, 64), -1)
	}

	if strings.Contains(promQL, promQLLabelConditionKey) {
		promQL = strings.Replace(promQL, promQLLabelConditionKey, def.genLabelCondition(labels), -1)
	}

	if strings.Contains(promQL, promQRangeDurationKey) {
		promQL = strings.Replace(promQL, promQRangeDurationKey, strconv.FormatInt(sctx.GetSessionVars().MetricSchemaRangeDuration, 10)+"s", -1)
	}
	return promQL
}

func (def *MetricTableDef) genLabelCondition(labels map[string]set.StringSet) string {
	var buf bytes.Buffer
	index := 0
	for _, label := range def.Labels {
		values := labels[label]
		if len(values) == 0 {
			continue
		}
		if index > 0 {
			buf.WriteByte(',')
		}
		switch len(values) {
		case 1:
			buf.WriteString(fmt.Sprintf("%s=\"%s\"", label, GenLabelConditionValues(values)))
		default:
			buf.WriteString(fmt.Sprintf("%s=~\"%s\"", label, GenLabelConditionValues(values)))
		}
		index++
	}
	return buf.String()
}

// GenLabelConditionValues generates the label condition values.
func GenLabelConditionValues(values set.StringSet) string {
	vs := make([]string, 0, len(values))
	for k := range values {
		vs = append(vs, k)
	}
	sort.Strings(vs)
	return strings.Join(vs, "|")
}

// metricSchemaTable stands for the fake table all its data is in the memory.
type metricSchemaTable struct {
	infoschemaTable
}

func tableFromMeta(alloc autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &metricSchemaTable{
		infoschemaTable: infoschemaTable{
			meta: meta,
			cols: columns,
			tp:   table.VirtualTable,
		},
	}
	return t, nil
}

// MetricTableMap records the metric table definition, export for test.
// TODO: read from system table.
var MetricTableMap = map[string]MetricTableDef{
	"query_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))`,
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.90,
		Comment:  "TiDB query durations(second)",
	},
	"qps": {
		PromQL:  `sum(rate(tidb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)`,
		Labels:  []string{"instance", "type", "result"},
		Comment: "TiDB query processing numbers per second",
	},
	"qps_ideal": {
		PromQL: `sum(tidb_server_connections) * sum(rate(tidb_server_handle_query_duration_seconds_count[$RANGE_DURATION])) / sum(rate(tidb_server_handle_query_duration_seconds_sum[$RANGE_DURATION]))`,
	},
	"ops_statement": {
		PromQL:  `sum(rate(tidb_executor_statement_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "TiDB statement statistics",
	},
	"failed_query_opm": {
		PromQL:  `sum(increase(tidb_server_execute_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "TiDB failed query opm",
	},
	"slow_query_time": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_process_duration_seconds_bucket[$RANGE_DURATION])) by (le))",
		Quantile: 0.90,
		Comment:  "TiDB slow query statistics with slow query time(second)",
	},
	"slow_query_cop_process_time": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_cop_duration_seconds_bucket[$RANGE_DURATION])) by (le))",
		Quantile: 0.90,
		Comment:  "TiDB slow query statistics with slow query total cop process time(second)",
	},
	"slow_query_cop_wait_time": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_wait_duration_seconds_bucket[$RANGE_DURATION])) by (le))",
		Quantile: 0.90,
		Comment:  "TiDB slow query statistics with slow query total cop wait time(second)",
	},
	"ops_internal": {
		PromQL:  "sum(rate(tidb_session_restricted_sql_total[$RANGE_DURATION]))",
		Comment: "TiDB internal SQL is used by TiDB itself.",
	},
	"process_mem_usage": {
		PromQL:  "process_resident_memory_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "process rss memory usage",
	},
	"heap_mem_usage": {
		PromQL:  "go_memstats_heap_alloc_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "TiDB heap memory size in use",
	},
	"process_cpu_usage": {
		PromQL: "rate(process_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels: []string{"instance", "job"},
	},
	"connection_count": {
		PromQL:  "tidb_server_connections{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "TiDB current connection counts",
	},
	"process_open_fd_count": {
		PromQL:  "process_open_fds{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process opened file descriptors count",
	},
	"goroutines_count": {
		PromQL:  " go_goroutines{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process current goroutines count)",
	},
	"go_gc_duration": {
		PromQL:  "rate(go_gc_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "Go garbage collection time cost(second)",
	},
	"go_threads": {
		PromQL:  "go_threads{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Total threads TiDB/PD process created currently",
	},
	"go_gc_count": {
		PromQL:  " rate(go_gc_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "The Go garbage collection counts per second",
	},
	"go_gc_cpu_usage": {
		PromQL:  "go_memstats_gc_cpu_fraction{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "The fraction of TiDB/PD available CPU time used by the GC since the program started.",
	},
	"tidb_event_opm": {
		PromQL:  "increase(tidb_server_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
		Comment: "TiDB Server critical events total, including start/close/shutdown/hang etc",
	},
	"tidb_keep_alive_opm": {
		PromQL:  "sum(increase(tidb_monitor_keep_alive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB instance monitor average keep alive times",
	},
	"prepared_statement_count": {
		PromQL:  "tidb_server_prepared_stmts{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "TiDB prepare statements count",
	},
	"tidb_time_jump_back_ops": {
		PromQL:  "sum(increase(tidb_monitor_time_jump_back_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB monitor time jump back count",
	},
	"tidb_panic_count": {
		Comment: "TiDB instance panic count",
		PromQL:  "increase(tidb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance"},
	},
	"tidb_binlog_error_count": {
		Comment: "TiDB write binlog error, skip binlog count",
		PromQL:  "tidb_server_critical_error_total{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
	},
	"get_token_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_get_token_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "Duration (us) for getting token, it should be small until concurrency limit is reached(second)",
	},
	"tidb_handshake_error_ops": {
		PromQL:  "sum(increase(tidb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB processing handshake error count",
	},
	"transaction_ops": {
		PromQL:  "sum(rate(tidb_session_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "TiDB transaction processing counts by type and source. Internal means TiDB inner transaction calls",
	},
	"transaction_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,sql_type,instance))",
		Labels:   []string{"instance", "type", "sql_type"},
		Quantile: 0.95,
		Comment:  "Bucketed histogram of transaction execution durations, including retry(second)",
	},
	"transaction_retry_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_retry_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "TiDB transaction retry num",
		Quantile: 0.95,
	},
	"transaction_statement_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_statement_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,sql_type))",
		Labels:   []string{"instance", "sql_type"},
		Comment:  "TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
		Quantile: 0.95,
	},
	"transaction_retry_error_ops": {
		PromQL:  "sum(rate(tidb_session_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "Error numbers of transaction retry",
	},
	"tidb_transaction_local_latch_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_local_latch_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "TiDB transaction latch wait time on key value storage(second)",
		Quantile: 0.95,
	},
	"parse_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_parse_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The time cost of parsing SQL to AST(second)",
	},
	"compile_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_compile_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The time cost of building the query plan(second)",
	},
	"execute_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_execute_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type, instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The time cost of executing the SQL which does not include the time to get the results of the query(second)",
	},
	"expensive_executors_ops": {
		Comment: "TiDB executors using more cpu and memory resources",
		PromQL:  "sum(rate(tidb_executor_expensive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "sql_type"},
	},
	"querie_using_plan_cache_ops": {
		PromQL:  "sum(rate(tidb_server_plan_cache_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "TiDB plan cache hit ops",
	},
	"uptime": {
		PromQL:  "(time() - process_start_time_seconds{$LABEL_CONDITIONS})",
		Labels:  []string{"instance", "job"},
		Comment: "TiDB uptime since last restart(second)",
	},
	"up": {
		PromQL:  `up{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "job"},
		Comment: "whether the instance is up. 1 is up, 0 is down(off-line)",
	},
}
