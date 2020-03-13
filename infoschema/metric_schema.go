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
		Labels:  []string{"instance", "type"},
	},
	"querie_using_plan_cache_ops": {
		PromQL:  "sum(rate(tidb_server_plan_cache_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "TiDB plan cache hit ops",
	},
	"distsql_execution_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "durations of distsql execution(second)",
	},
	"distsql_qps": {
		PromQL:  "sum(rate(tidb_distsql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "distsql query handling durations per second",
	},
	"distsql_partial_qps": {
		PromQL:  "sum(rate(tidb_distsql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
		Comment: "the numebr of distsql partial scan numbers",
	},
	"distsql_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "the numebr of distsql scan numbers",
	},
	"distsql_partial_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "the numebr of distsql partial scan key numbers",
	},
	"distsql_partial_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "distsql partial numbers per query",
	},
	"tidb_cop_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "kv storage coprocessor processing durations",
	},
	"kv_backoff_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_backoff_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "kv backoff time durations(second)",
	},
	"kv_backoff_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "kv storage backoff times",
	},
	"kv_region_error_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "kv region error times",
	},
	"lock_resolver_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "lock resolve times",
	},
	"lock_cleanup_fail_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_lock_cleanup_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "lock cleanup failed ops",
	},
	"load_safepoint_fail_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "safe point update ops",
	},
	"kv_request_ops": {
		Comment: "kv request total by instance and command type",
		PromQL:  "sum(rate(tidb_tikvclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
		Labels:  []string{"instance", "type"},
	},
	"kv_request_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_request_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,store,instance))",
		Labels:   []string{"instance", "type", "store"},
		Quantile: 0.95,
		Comment:  "kv requests durations by store",
	},
	"kv_txn_ops": {
		Comment: "TiDB total kv transaction counts",
		PromQL:  "sum(rate(tidb_tikvclient_txn_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"kv_write_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_kv_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
		Comment:  "kv write times per transaction execution",
	},
	"kv_write_size": {
		Comment:  "kv write size per transaction execution",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
	},
	"txn_region_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_regions_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Comment:  "regions transaction operates on count",
		Quantile: 0.95,
	},
	"load_safepoint_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "safe point loading times",
	},
	"kv_snapshot_ops": {
		Comment: "using snapshots total",
		PromQL:  "sum(rate(tidb_tikvclient_snapshot_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"pd_client_cmd_ops": {
		PromQL:  "sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "pd client command ops",
	},
	"pd_client_cmd_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "pd client command durations",
	},
	"pd_cmd_fail_ops": {
		PromQL:  "sum(rate(pd_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "pd client command fail count",
	},
	"pd_handle_request_ops": {
		PromQL:  "sum(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "pd handle request operation per second",
	},
	"pd_handle_request_duration": {
		Comment:  "pd handle request duration(second)",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.999,
	},
	"pd_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{type=\"wait\"}[$RANGE_DURATION])) by (le))",
		Quantile: 0.999,
		Comment:  "The duration of a client starting to wait for the TS until received the TS result.",
	},
	"pd_tso_rpc_duration": {
		Comment:  "The duration of a client sending TSO request until received the response.",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{type=\"tso\"}[$RANGE_DURATION])) by (le))",
		Quantile: 0.999,
	},
	"pd_start_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_pdclient_ts_future_wait_seconds_bucket[$RANGE_DURATION])) by (le))",
		Quantile: 0.999,
		Comment:  "The duration of the waiting time for getting the start timestamp oracle",
	},
	"load_schema_duration": {
		Comment:  "TiDB loading schema time durations by instance",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_domain_load_schema_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"load_schema_ops": {
		Comment: "TiDB loading schema times including both failed and successful ones",
		PromQL:  "sum(rate(tidb_domain_load_schema_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"schema_lease_error_opm": {
		Comment: "TiDB schema lease error counts",
		PromQL:  "sum(increase(tidb_session_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"load_privilege_ops": {
		Comment: "TiDB load privilege counts",
		PromQL:  "sum(rate(tidb_domain_load_privilege_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"ddl_duration": {
		Comment:  "TiDB DDL duration statistics",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_handle_job_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"ddl_batch_add_index_duration": {
		Comment:  "TiDB batch add index durations by histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_batch_add_idx_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"ddl_add_index_speed": {
		Comment: "TiDB add index speed",
		PromQL:  "sum(rate(tidb_ddl_add_index_total[$RANGE_DURATION])) by (type)",
	},
	"ddl_waiting_jobs_num": {
		Comment: "TiDB ddl request in queue",
		PromQL:  "tidb_ddl_waiting_jobs{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "type"},
	},
	"ddl_meta_opm": {
		Comment: "TiDB different ddl worker numbers",
		PromQL:  "increase(tidb_ddl_worker_operation_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
	},
	"ddl_worker_duration": {
		Comment:  "TiDB ddl worker duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(increase(tidb_ddl_worker_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, action, result,instance))",
		Labels:   []string{"instance", "type", "result", "action"},
		Quantile: 0.95,
	},
	"ddl_deploy_syncer_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_deploy_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
		Comment:  "TiDB ddl schema syncer statistics, including init, start, watch, clear function call time cost",
	},
	"owner_handle_syncer_duration": {
		Comment:  "TiDB ddl owner time operations on etcd duration statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_owner_handle_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"ddl_update_self_version_duration": {
		Comment:  "TiDB schema syncer version update time duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_update_self_ver_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, result,instance))",
		Labels:   []string{"instance", "result"},
		Quantile: 0.95,
	},
	"ddl_opm": {
		Comment: "executed DDL jobs per minute",
		PromQL:  "sum(rate(tidb_ddl_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"statistics_auto_analyze_duration": {
		Comment:  "TiDB auto analyze time durations within 95 percent histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_auto_analyze_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"statistics_auto_analyze_ops": {
		Comment: "TiDB auto analyze query per second",
		PromQL:  "sum(rate(tidb_statistics_auto_analyze_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"statistics_stats_inaccuracy_rate": {
		Comment:  "TiDB statistics inaccurate rate",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_stats_inaccuracy_rate_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"statistics_pseudo_estimation_ops": {
		Comment: "TiDB optimizer using pseudo estimation counts",
		PromQL:  "sum(rate(tidb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"statistics_dump_feedback_ops": {
		Comment: "TiDB dumping statistics back to kv storage times",
		PromQL:  "sum(rate(tidb_statistics_dump_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"statistics_store_query_feedback_qps": {
		Comment: "TiDB store quering feedback counts",
		PromQL:  "sum(rate(tidb_statistics_store_query_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) ",
		Labels:  []string{"instance", "type"},
	},
	"statistics_significant_feedback": {
		Comment: "Counter of query feedback whose actual count is much different than calculated by current statistics",
		PromQL:  "sum(rate(tidb_statistics_high_error_rate_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"statistics_update_stats_ops": {
		Comment: "TiDB updating statistics using feed back counts",
		PromQL:  "sum(rate(tidb_statistics_update_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"statistics_fast_analyze_status": {
		Comment:  "TiDB fast analyze statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_fast_analyze_status_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_new_etcd_session_duration": {
		Comment:  "TiDB new session durations for new etcd sessions",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_owner_new_session_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,result, instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"tidb_owner_watcher_ops": {
		Comment: "TiDB owner  watcher counts",
		PromQL:  "sum(rate(tidb_owner_watch_owner_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, result, instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_auto_id_qps": {
		Comment: "TiDB auto id requests per second including  single table/global auto id processing and single table auto id rebase processing",
		PromQL:  "sum(rate(tidb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"tidb_auto_id_request_duration": {
		Comment:  "TiDB auto id requests durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_autoid_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_region_cache_ops": {
		Comment: "TiDB region cache operations count",
		PromQL:  "sum(rate(tidb_tikvclient_region_cache_operations_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_meta_operation_duration": {
		Comment:  "TiDB meta operation durations including get/set schema and ddl jobs",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_meta_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"tidb_gc_worker_action_opm": {
		Comment: "kv storage garbage collection counts by type",
		PromQL:  "sum(increase(tidb_tikvclient_gc_worker_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_duration": {
		Comment:  "kv storage garbage collection time durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_gc_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_gc_config": {
		Comment: "kv storage garbage collection config including gc_life_time and gc_run_interval",
		PromQL:  "max(tidb_tikvclient_gc_config{$LABEL_CONDITIONS}) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_fail_opm": {
		Comment: "kv storage garbage collection failing counts",
		PromQL:  "sum(increase(tidb_tikvclient_gc_failure{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_delete_range_fail_opm": {
		Comment: "kv storage unsafe destroy range failed counts",
		PromQL:  "sum(increase(tidb_tikvclient_gc_unsafe_destroy_range_failures{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_too_many_locks_opm": {
		Comment: "kv storage region garbage collection clean too many locks count",
		PromQL:  "sum(increase(tidb_tikvclient_gc_region_too_many_locks[$RANGE_DURATION]))",
	},
	"tidb_gc_action_result_opm": {
		Comment: "kv storage garbage collection results including failed and successful ones",
		PromQL:  "sum(increase(tidb_tikvclient_gc_action_result{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_delete_range_task_status": {
		Comment: "kv storage delete range task execution status by type",
		PromQL:  "sum(tidb_tikvclient_range_task_stats{$LABEL_CONDITIONS}) by (type, result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_gc_push_task_duration": {
		Comment:  "kv storage range worker processing one task duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_range_task_push_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_batch_client_pending_req_count": {
		Comment: "kv storage batch requests in queue",
		PromQL:  "sum(tidb_tikvclient_pending_batch_requests{$LABEL_CONDITIONS}) by (store,instance)",
		Labels:  []string{"instance", "store"},
	},
	"tidb_batch_client_wait_duration": {
		Comment:  "kv storage batch processing durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_wait_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_batch_client_unavailable_duration": {
		Comment:  "kv storage batch processing unvailable durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_client_unavailable_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
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
	"pd_role": {
		PromQL:  `delta(pd_tso_events{type="save"}[$RANGE_DURATION]) > bool 0`,
		Labels:  []string{"instance"},
		Comment: "It indicates whether the current PD is the leader or a follower.",
	},
	"normal_stores": {
		PromQL:  `sum(pd_cluster_status{type="store_up_count"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of healthy stores",
	},
	"abnormal_stores": {
		PromQL: `sum(pd_cluster_status{ type=~"store_disconnected_count|store_unhealth_count|store_low_space_count|store_down_count|store_offline_count|store_tombstone_count"})`,
		Labels: []string{"instance", "type"},
	},
	"pd_scheduler_config": {
		PromQL: `pd_config_status{$LABEL_CONDITIONS}`,
		Labels: []string{"type"},
	},
	"region_label_isolation_level": {
		PromQL: `pd_regions_label_level{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"label_distribution": {
		PromQL: `pd_cluster_placement_status{$LABEL_CONDITIONS}`,
		Labels: []string{"name"},
	},
	"pd_cluster_status": {
		PromQL: `sum(pd_cluster_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels: []string{"instance", "type"},
	},
	"pd_cluster_metadata": {
		PromQL: `pd_cluster_metadata{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"region_health": {
		PromQL:  `sum(pd_regions_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "It records the unusual Regions' count which may include pending peers, down peers, extra peers, offline peers, missing peers or learner peers",
	},
	"pd_schedule_operator": {
		PromQL:  `sum(delta(pd_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)`,
		Labels:  []string{"instance", "type", "event"},
		Comment: "The number of different operators",
	},
	"pd_operator_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operators_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The time consumed when the operator is finished",
	},
	"pd_operator_step_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operator_steps_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The time consumed when the operator step is finished",
	},
	"pd_scheduler_store_status": {
		PromQL: `pd_scheduler_store_status{$LABEL_CONDITIONS}`,
		Labels: []string{"address", "instance", "store", "type"},
	},
	"store_available_ratio": {
		PromQL:  `sum(pd_scheduler_store_status{type="store_available"}) by (address, store) / sum(pd_scheduler_store_status{type="store_capacity"}) by (address, store)`,
		Labels:  []string{"address", "store"},
		Comment: "It is equal to Store available capacity size over Store capacity size for each TiKV instance",
	},
	"size_amplification": {
		PromQL:  `sum(pd_scheduler_store_status{type="region_size"}) by (address, store) / sum(pd_scheduler_store_status{type="store_used"}) by (address, store) * 2^20`,
		Labels:  []string{"address", "store"},
		Comment: "The size amplification, which is equal to Store Region size over Store used capacity size, of each TiKV instance",
	},
	"pd_scheduler_op_influence": {
		PromQL: `pd_scheduler_op_influence{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "store", "type"},
	},
	"pd_scheduler_tolerant_resource": {
		PromQL: `pd_scheduler_tolerant_resource{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "source", "target"},
	},
	"pd_hotspot_status": {
		PromQL: `pd_hotspot_status{$LABEL_CONDITIONS}`,
		Labels: []string{"address", "instance", "store", "type"},
	},
	"pd_scheduler_status": {
		PromQL: `pd_scheduler_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "kind", "type"},
	},
	"pd_scheduler_balance_leader": {
		PromQL:  `sum(delta(pd_scheduler_balance_leader{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)`,
		Labels:  []string{"instance", "address", "store", "type"},
		Comment: "The leader movement details among TiKV instances",
	},
	"pd_balance_region_movement": {
		PromQL:  `sum(delta(pd_scheduler_balance_region{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)`,
		Labels:  []string{"instance", "address", "store", "type"},
		Comment: "The Region movement details among TiKV instances",
	},
	"pd_balance_scheduler_status": {
		PromQL:  `sum(delta(pd_scheduler_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,name)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The inner status of balance leader scheduler",
	},
	"pd_checker_event_count": {
		PromQL:  `sum(delta(pd_checker_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance,type)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The replica/region checker's status",
	},
	"pd_schedule_filter": {
		PromQL: `sum(delta(pd_schedule_filter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (store, type, scope, instance)`,
		Labels: []string{"instance", "scope", "store", "type"},
	},
	"pd_scheduler_balance_direction": {
		PromQL: `sum(delta(pd_scheduler_balance_direction{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,source,target,instance)`,
		Labels: []string{"instance", "source", "target", "type"},
	},
	"pd_schedule_store_limit": {
		PromQL: `pd_schedule_store_limit{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "store", "type"},
	},

	"pd_grpc_completed_commands_rate": {
		PromQL:  `sum(rate(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (grpc_method,instance)`,
		Labels:  []string{"grpc_method", "instance"},
		Comment: "The rate of completing each kind of gRPC commands",
	},
	"pd_grpc_completed_commands_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(grpc_server_handling_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,grpc_method,instance))`,
		Labels:   []string{"grpc_method", "instance"},
		Quantile: 0.99,
		Comment:  "The time consumed of completing each kind of gRPC commands",
	},
	"pd_handle_transactions_rate": {
		PromQL:  `sum(rate(pd_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, result)`,
		Labels:  []string{"instance", "result"},
		Comment: "The rate of handling etcd transactions",
	},
	"pd_handle_transactions_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_txn_handle_txns_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance, result))`,
		Labels:   []string{"instance", "result"},
		Quantile: 0.99,
		Comment:  "The time consumed of handling etcd transactions",
	},
	"etcd_wal_fsync_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The time consumed of writing WAL into the persistent storage",
	},
	"pd_peer_round_trip_time_seconds": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_network_peer_round_trip_time_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,To))`,
		Labels:   []string{"To", "instance"},
		Quantile: 0.99,
		Comment:  "The latency of the network in .99",
	},
	"etcd_disk_wal_fsync_rate": {
		PromQL:  `delta(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance"},
		Comment: "The rate of writing WAL into the persistent storage",
	},
	"pd_server_etcd_state": {
		PromQL:  `pd_server_etcd_state{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "type"},
		Comment: "The current term of Raft",
	},
	"pd_handle_requests_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, le))`,
		Labels:   []string{"type"},
		Quantile: 0.98,
	},
	"pd_handle_requests_duration_avg": {
		PromQL: `avg(rate(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type) /  avg(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)`,
		Labels: []string{"type"},
	},
	"pd_region_heartbeat_latency": {
		PromQL:   `round(histogram_quantile($QUANTILE, sum(rate(pd_scheduler_region_heartbeat_latency_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,address, store)), 1000)`,
		Labels:   []string{"address", "store"},
		Quantile: 0.99,
		Comment:  "The heartbeat latency of each TiKV instance in",
	},
	"pd_scheduler_region_heartbeat": {
		PromQL: `sum(rate(pd_scheduler_region_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])*60) by (address,instance, store, status,type)`,
		Labels: []string{"address", "instance", "status", "store", "type"},
	},
	"pd_region_syncer_status": {
		PromQL: `pd_region_syncer_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
}
