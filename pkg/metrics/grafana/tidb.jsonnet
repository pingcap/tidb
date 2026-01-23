// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

local grafana = import 'grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local template = grafana.template;

local myNameFlag = 'DS_TEST-CLUSTER';
local myDS = '${' + myNameFlag + '}';

// A new dashboard
local newDash = dashboard.new(
  title='Test-Cluster-TiDB',
  editable=true,
  graphTooltip='shared_crosshair',
  refresh='30s',
  time_from='now-1h',
)
.addInput(
  name=myNameFlag,
  label='test-cluster',
  type='datasource',
  pluginId='prometheus',
  pluginName='Prometheus',
)
.addTemplate(
  template.new(
    datasource=myDS,
    hide=2,
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(pd_cluster_status, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    allValues=null,
    current=null,
    datasource=myDS,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(pd_cluster_status{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
    refresh='time',
    regex='',
    sort=1,
    tagValuesQuery='',
  )
).addTemplate(
  template.new(
    allValues='.*',
    current=null,
    datasource=myDS,
    hide='',
    includeAll=true,
    label='Instance',
    multi=false,
    name='instance',
    query='label_values(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    regex='',
    sort=1,
    tagValuesQuery='',
  )
);

// ============== Row: Query Summary ==============
local querySummaryRow = row.new(collapse=true, title='Query Summary');

local durationP = graphPanel.new(
  title='Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB query durations by histogram buckets with different percents',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='80',
  )
);

local cpsP = graphPanel.new(
  title='Command Per Second',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='MySQL commands processing numbers per second. See https://dev.mysql.com/doc/internals/en/text-protocol.html and https://dev.mysql.com/doc/internals/en/prepared-statements.html',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (result)',
    legendFormat='query {{result}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", result="OK"}[1m]  offset 1d))',
    legendFormat='yesterday',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) * sum(rate(tidb_server_handle_query_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) / sum(rate(tidb_server_handle_query_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='ideal CPS',
    hide=true,
  )
);

local qpsP = graphPanel.new(
  title='QPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  format='short',
  logBase1Y=2,
  description='TiDB statement statistics',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_statement_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_statement_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='total',
  )
);

local cpsByInstP = graphPanel.new(
  title='CPS By Instance',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
  format='short',
  description='TiDB command total statistics including both successful and failed ones',
)
.addTarget(
  prometheus.target(
    'rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])',
    legendFormat='{{instance}} {{type}} {{result}}',
  )
);

local failedQueryOPMP = graphPanel.new(
  title='Failed Query OPM',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='TiDB failed query statistics by query type',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_server_execute_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, instance)',
    legendFormat=' {{type}}-{{instance}}',
  )
);

local affectedRowsP = graphPanel.new(
  title='Affected Rows By Type',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Affected rows of DMLs (INSERT/UPDATE/DELETE/REPLACE) per second. It could present the written rows/s for TiDB instances.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_affected_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (sql_type)',
    legendFormat='{{sql_type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_affected_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='total',
  )
);

local slowQueryP = graphPanel.new(
  title='Slow Query',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB slow query statistics with slow query durations and coprocessor waiting/executing durations',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_slow_query_process_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le,sql_type))',
    legendFormat='all_proc',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_slow_query_cop_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le,sql_type))',
    legendFormat='all_cop_proc',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_slow_query_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le,sql_type))',
    legendFormat='all_cop_wait',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_slow_query_cop_mvcc_ratio_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le,sql_type))',
    legendFormat='mvcc_ratio',
  )
);

local connIdleDurationP = graphPanel.new(
  title='Connection Idle Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB connection idle durations',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'1\'}[1m])) by (le,in_txn))',
    legendFormat='99-in-txn',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'0\'}[1m])) by (le,in_txn))',
    legendFormat='99-not-in-txn',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'1\'}[1m])) by (le,in_txn))',
    legendFormat='90-in-txn',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'0\'}[1m])) by (le,in_txn))',
    legendFormat='90-not-in-txn',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'1\'}[1m])) by (le,in_txn))',
    legendFormat='80-in-txn',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", in_txn=\'0\'}[1m])) by (le,in_txn))',
    legendFormat='80-not-in-txn',
  )
);

local duration999P = graphPanel.new(
  title='999 Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations for different query types with 99.9 percent buckets',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local duration99P = graphPanel.new(
  title='99 Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations for different query types with 99 percent buckets',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local duration95P = graphPanel.new(
  title='95 Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations for different query types with 95 percent buckets',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local duration80P = graphPanel.new(
  title='80 Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations for different query types with 80 percent buckets',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
    legendFormat='{{sql_type}}',
  )
);

// ============== Row: Query Detail ==============
local queryDetailRow = row.new(collapse=true, title='Query Detail');

local duration80ByInstP = graphPanel.new(
  title='Duration 80 By Instance',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations with 80 percent buckets by instance',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, instance))',
    legendFormat='{{instance}}',
  )
);

local duration95ByInstP = graphPanel.new(
  title='Duration 95 By Instance',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations with 95 percent buckets by instance',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, instance))',
    legendFormat='{{ instance }}',
  )
);

local duration99ByInstP = graphPanel.new(
  title='Duration 99 By Instance',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations with 99 percent buckets by instance',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, instance))',
    legendFormat='{{instance}}',
  )
);

local duration999ByInstP = graphPanel.new(
  title='Duration 999 By Instance',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='TiDB durations with 99.9 percent buckets by instance',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, instance))',
    legendFormat='{{instance}}',
  )
);

local failedQueryOPMDetailP = graphPanel.new(
  title='Failed Query OPM Detail',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  logBase1Y=2,
  description='TiDB failed query statistics with failing information',
)
.addTarget(
  prometheus.target(
    'increase(tidb_server_execute_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])',
    legendFormat='{{type}} @ {{instance}}',
  )
);

local internalSqlOpsP = graphPanel.new(
  title='Internal SQL OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The internal SQL is used by TiDB itself.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_restricted_sql_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s]))',
    legendFormat='',
  )
);

local queriesInMultiStmtP = graphPanel.new(
  title='Queries In Multi-Statement',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The number of queries contained in a multi-query statement per second.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_multi_query_num_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s]))/sum(rate(tidb_server_multi_query_num_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s]))',
    legendFormat='avg',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_multi_query_num_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s]))',
    legendFormat='sum',
  )
);

// ============== Row: Server ==============
local serverRow = row.new(collapse=true, title='Server');

local tidbServerStatusP = graphPanel.new(
  title='TiDB Server Status',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='TiDB server status',
)
.addTarget(
  prometheus.target(
    'count(up{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} == 1)',
    legendFormat='Up',
  )
)
.addTarget(
  prometheus.target(
    'count(up{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} == 0)',
    legendFormat='Down',
  )
);

local uptimeP = graphPanel.new(
  title='Uptime',
  datasource=myDS,
  legend_rightSide=false,
  format='dtdurations',
  logBase1Y=2,
  description='TiDB uptime since last restart',
)
.addTarget(
  prometheus.target(
    '(time() - process_start_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"})',
    legendFormat='{{instance}}',
  )
);

local cpuUsageP = graphPanel.new(
  title='CPU Usage',
  datasource=myDS,
  legend_rightSide=false,
  format='percentunit',
  description='TiDB cpu usage calculated with process cpu running seconds',
)
.addTarget(
  prometheus.target(
    'irate(process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}[30s])',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tidb_server_maxprocs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='quota-{{instance}}',
  )
);

local memUsageP = graphPanel.new(
  title='Memory Usage',
  datasource=myDS,
  legend_rightSide=false,
  format='bytes',
  description='TiDB process rss memory usage. TiDB heap memory size in use',
)
.addTarget(
  prometheus.target(
    'process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='process-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_objects_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_unused_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_released_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_free_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapSys-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_objects_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_unused_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapInuse-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_objects_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapAlloc-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_released_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_free_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapIdle-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_released_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapReleased-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'go_gc_heap_goal_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}',
    legendFormat='GCTrigger-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tidb_server_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}',
    legendFormat='{{module}}-{{type}}-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}) by (module, instance)',
    legendFormat='{{module}}-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tidb_server_memory_quota_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='quota-{{instance}}',
  )
);

local runtimeGcRateP = graphPanel.new(
  title='Runtime GC Rate And GOMEMLIMIT',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
)
.addTarget(
  prometheus.target(
    'rate(go_gc_cycles_total_gc_cycles_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}[1m])',
    legendFormat='gc-rate - {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'go_gc_gomemlimit_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='gomemlimit - {{instance}}',
  )
);

local openFdCountP = graphPanel.new(
  title='Open FD Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB process opened file descriptors count',
)
.addTarget(
  prometheus.target(
    'process_open_fds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='{{instance}}',
  )
);

local connectionCountP = graphPanel.new(
  title='Connection Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB current connection counts',
)
.addTarget(
  prometheus.target(
    'tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}} {{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"})',
    legendFormat='total',
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_tokens{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"})',
    legendFormat='active connections',
  )
);

local eventsOpmP = graphPanel.new(
  title='Events OPM',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB Server critical events total, including start/close/shutdown/hang etc',
)
.addTarget(
  prometheus.target(
    'increase(tidb_server_event_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[10m])',
    legendFormat='{{instance}}-{{type}}',
  )
);

local disconnectionCountP = graphPanel.new(
  title='Disconnection Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB connection disconnected counts',
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_disconnection_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) by (instance, result)',
    legendFormat='{{instance}}-{{result}}',
  )
);

local prepareStmtCountP = graphPanel.new(
  title='Prepare Statement Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB instance prepare statements count',
)
.addTarget(
  prometheus.target(
    'tidb_server_prepared_stmts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_prepared_stmts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"})',
    legendFormat='total',
  )
);

local goroutineCountP = graphPanel.new(
  title='Goroutine Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB process current goroutines count',
)
.addTarget(
  prometheus.target(
    'go_sched_goroutines_goroutines{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job=~"tidb.*"}',
    legendFormat='{{instance}}',
  )
);

local panicCriticalErrorP = graphPanel.new(
  title='Panic And Critial Error',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB instance critical errors count including panic etc',
)
.addTarget(
  prometheus.target(
    'increase(tidb_server_panic_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])',
    legendFormat='panic-{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'increase(tidb_server_critical_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])',
    legendFormat='critical-{{instance}}',
  )
);

local keepAliveOpmP = graphPanel.new(
  title='Keep Alive OPM',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB instance monitor average keep alive times',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_monitor_keep_alive_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local getTokenDurationP = graphPanel.new(
  title='Get Token Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='µs',
  description='Duration (us) for getting token, it should be small until concurrency limit is reached.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_get_token_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
);

local timeJumpBackOpsP = graphPanel.new(
  title='Time Jump Back OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB monitor time jump back count',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_monitor_time_jump_back_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local clientDataTrafficP = graphPanel.new(
  title='Client Data Traffic',
  datasource=myDS,
  legend_rightSide=false,
  format='Bps',
  description='Data traffic statistics between TiDB and the client.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_packet_io_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}-rate',
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_packet_io_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) by (type)',
    legendFormat='{{type}}-total',
  )
);

local skipBinlogCountP = graphPanel.new(
  title='Skip Binlog Count',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB instance critical errors count including panic etc',
)
.addTarget(
  prometheus.target(
    'tidb_server_critical_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}',
  )
);

local rcCheckTsWriteConflictP = graphPanel.new(
  title='RCCheckTS WriteConflict Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The number of WriteConflict errors caused by RCCheckTS',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_server_rc_check_ts_conflict_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local handshakeErrorOpsP = graphPanel.new(
  title='Handshake Error OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB processing handshake error count',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_server_handshake_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local internalSessionsP = graphPanel.new(
  title='Internal Sessions',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The total count of internal sessions.',
)
.addTarget(
  prometheus.target(
    'tidb_server_internal_sessions{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}',
    legendFormat='{{instance}}',
  )
);

local activeUsersP = graphPanel.new(
  title='Active Users',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The total count of active users.',
)
.addTarget(
  prometheus.target(
    'tidb_server_active_users{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}',
    legendFormat='{{instance}}',
  )
);

local connPerTlsCipherP = graphPanel.new(
  title='Connections Per TLS Cipher',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Connections per TLS Cipher and instance',
)
.addTarget(
  prometheus.target(
    'rate(tidb_server_tls_cipher{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}[1m])',
    legendFormat='{{cipher}} - {{instance}}',
  )
);

local connPerTlsVersionP = graphPanel.new(
  title='Connections Per TLS Version',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Connections per TLS version and instance',
)
.addTarget(
  prometheus.target(
    'rate(tidb_server_tls_version{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job="tidb"}[1m])',
    legendFormat='{{version}} - {{instance}}',
  )
);

// ============== Row: Transaction ==============
local transactionRow = row.new(collapse=true, title='Transaction');

local txnOpsP = graphPanel.new(
  title='Transaction OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB transaction processing counts by type and source.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_transaction_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[1m])) by (type, txn_mode)',
    legendFormat='{{type}}-{{txn_mode}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_transaction_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"internal"}[1m])) by (type, txn_mode)',
    legendFormat='Internal-{{type}}-{{txn_mode}}',
    hide=true,
  )
);

local txnDurationP = graphPanel.new(
  title='Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='Bucketed histogram of transaction execution durations, including retry',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_transaction_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[1m])) by (le, txn_mode))',
    legendFormat='99-{{txn_mode}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_transaction_duration_seconds_sum{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", scope=~"general"}[1m])) by (txn_mode) / sum(rate(tidb_session_transaction_duration_seconds_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", scope=~"general"}[1m])) by (txn_mode)',
    legendFormat='avg-{{txn_mode}}',
  )
);

local txnStmtNumP = graphPanel.new(
  title='Transaction Statement Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB statements numbers within one transaction.',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_transaction_statement_num_bucket{instance=~"$instance", scope=~"general"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local txnRetryNumP = graphPanel.new(
  title='Transaction Retry Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB transaction retry histogram bucket statistics',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_retry_num_bucket{instance=~"$instance", scope=~"general"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local sessionRetryErrorOpsP = graphPanel.new(
  title='Session Retry Error OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Error numbers of transaction retry',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_retry_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s])) by (type, sql_type)',
    legendFormat='{{type}}-{{sql_type}}',
  )
);

local commitTokenWaitP = graphPanel.new(
  title='Commit Token Wait Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='ns',
  logBase1Y=2,
  description='The duration of a transaction waits for a token when committing.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_batch_executor_token_wait_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_tikvclient_batch_executor_token_wait_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_tikvclient_batch_executor_token_wait_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='80',
  )
);

local kvTxnOpsP = graphPanel.new(
  title='KV Transaction OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB total kv transaction counts',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_txn_cmd_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[1m])) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
  )
);

local kvTxnDurationP = graphPanel.new(
  title='KV Transaction Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  logBase1Y=2,
  description='The duration of the transaction commit/rollback on TiKV.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_txn_cmd_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_tikvclient_txn_cmd_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_tikvclient_txn_cmd_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    hide=true,
  )
);

local txnRegionsNumP = graphPanel.new(
  title='Transaction Regions Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='regions transaction operates on count',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_tikvclient_txn_regions_num_bucket{instance=~"$instance", type="2pc_prewrite", scope=~"general"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local txnWriteKvNumRateP = graphPanel.new(
  title='Transaction Write KV Num Rate And Sum',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='kv write times per transaction execution',
)
.addTarget(
  prometheus.target(
    'rate(tidb_tikvclient_txn_write_kv_num_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[30s])',
    legendFormat='{{instance}}-rate',
  )
)
.addTarget(
  prometheus.target(
    'tidb_tikvclient_txn_write_kv_num_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}',
    legendFormat='{{instance}}-sum',
  )
);

local txnWriteKvNumP = graphPanel.new(
  title='Transaction Write KV Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='kv write times per transaction execution',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_tikvclient_txn_write_kv_num_bucket{instance=~"$instance", scope=~"general"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local stmtLockKeysP = graphPanel.new(
  title='Statement Lock Keys',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The number of statement acquires locks.',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_statement_lock_keys_count_bucket{instance=~"$instance"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local sendHeartbeatDurationP = graphPanel.new(
  title='Send HeartBeat Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='When the pessimistic transaction begins to work, it will send heartbeat requests to update its TTL. This metric is the latency of the send heartbeat operation.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_tikvclient_txn_heart_beat_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_tikvclient_txn_heart_beat_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_txn_heart_beat_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
);

local txnWriteSizeRateP = graphPanel.new(
  title='Transaction Write Size Bytes Rate And Sum',
  datasource=myDS,
  legend_rightSide=false,
  format='Bps',
  description='kv write size per transaction execution',
)
.addTarget(
  prometheus.target(
    'rate(tidb_tikvclient_txn_write_size_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}[30s])',
    legendFormat='{{instance}}-rate',
  )
)
.addTarget(
  prometheus.target(
    'tidb_tikvclient_txn_write_size_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope=~"general"}',
    legendFormat='{{instance}}-sum',
  )
);

local txnWriteSizeP = graphPanel.new(
  title='Transaction Write Size Bytes',
  datasource=myDS,
  legend_rightSide=false,
  format='bytes',
  description='kv write size per transaction execution',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_tikvclient_txn_write_size_bytes_bucket{instance=~"$instance", scope=~"general"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local acquirePessimisticLocksP = graphPanel.new(
  title='Acquire Pessimistic Locks Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The duration of a statement acquiring all pessimistic locks at a time.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_tikvclient_pessimistic_lock_keys_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='80',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_tikvclient_pessimistic_lock_keys_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_pessimistic_lock_keys_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
);

local ttlLifetimeReachP = graphPanel.new(
  title='TTL Lifetime Reach Counter',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='This metric means the pessimistic lives too long which is abnormal.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_ttl_lifetime_reach_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local loadSafepointOpsP = graphPanel.new(
  title='Load Safepoint OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='safe point loading times',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_load_safepoint_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="ok"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local pessimisticStmtRetryP = graphPanel.new(
  title='Pessimistic Statement Retry OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='When the pessimistic statement is executed, the lock fails and it can retry automatically. The number of times the statement is retried is recorded.',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_statement_pessimistic_retry_count_bucket{instance=~"$instance"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local txnTypesPerSecP = graphPanel.new(
  title='Transaction Types Per Second',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='This metric shows the OPS of different types of transactions.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_commit_txn_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='2PC-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_async_commit_txn_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='async commit-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_one_pc_txn_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='1PC-{{type}}',
  )
);

local txnCommitP99BackoffP = graphPanel.new(
  title='Transaction Commit P99 Backoff',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='99th percentile of backoff count and duration in a transaction commit',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, rate(tidb_tikvclient_txn_commit_backoff_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='count - {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, rate(tidb_tikvclient_txn_commit_backoff_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='duration - {{instance}}',
  )
);

local safeTsUpdateP = graphPanel.new(
  title='SafeTS Update Conuter',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='This metric refers to the SafeTS update status count.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_safets_update_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (result, store)',
    legendFormat='{{result}}-store-{{store}}',
  )
);

local maxSafeTsGapP = graphPanel.new(
  title='Max SafeTS Gap',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The gap between SafeTS and current time',
)
.addTarget(
  prometheus.target(
    'tidb_tikvclient_min_safets_gap_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}-store-{{store}}',
  )
);

local assertionP = graphPanel.new(
  title='Assertion',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(irate(tidb_tikvclient_prewrite_assertion_count{instance=~"$instance"}[30s])) by (type)',
    legendFormat='{{type}}',
  )
);

local txnExecStatesDurationP = graphPanel.new(
  title='Transaction Execution States Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='How much time transactions spend on each state',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (le, type))',
    legendFormat='{{type}}-99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (le, type))',
    legendFormat='{{type}}-90',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.8, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (le, type))',
    legendFormat='{{type}}-80',
  )
);

local txnWithLockExecStatesDurationP = graphPanel.new(
  title='Transaction With Lock Execution States Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='How much time transactions spend on each state after it acquire at least one lock',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", has_lock="true"}[1m])) by (le, type))',
    legendFormat='{{type}}-99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", has_lock="true"}[1m])) by (le, type))',
    legendFormat='{{type}}-90',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_session_txn_state_seconds_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", has_lock="true"}[1m])) by (le, type))',
    legendFormat='{{type}}-80',
  )
);

local txnExecStatesDurationSumP = graphPanel.new(
  title='Transaction Execution States Duration Sum',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='How much time transactions spend on each state',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_txn_state_seconds_sum{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_transaction_duration_seconds_sum{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat='total',
  )
);

local txnEnterStateP = graphPanel.new(
  title='Transaction Enter State',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='How many times transactions enter this state in the last minute',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_txn_state_seconds_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local txnLeaveStateP = graphPanel.new(
  title='Transaction Leave State',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='How many times transactions leave this state in the last minute',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_txn_state_seconds_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local txnStateCountChangeP = graphPanel.new(
  title='Transaction State Count Change',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='Transaction leave state minus Transaction enter state',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_session_txn_state_seconds_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])) by (type) - on (type) increase(tidb_session_txn_state_entering_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])',
    legendFormat='{{type}}',
  )
);

local fairLockingUsageP = graphPanel.new(
  title='Fair Locking Usage',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Counters of transactions or statements in which fair locking is enabled / takes effect.',
)
.addTarget(
  prometheus.target(
    'sum(irate(tidb_session_transaction_fair_locking_usage{instance=~"$instance"}[30s])) by (type)',
    legendFormat='{{type}}',
  )
);

local fairLockingKeysP = graphPanel.new(
  title='Fair Locking Keys',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='Counters of keys involved in fair locking.',
)
.addTarget(
  prometheus.target(
    'sum(irate(tidb_tikvclient_aggressive_locking_count{instance=~"$instance"}[30s])) by (type)',
    legendFormat='{{type}}',
  )
);

local pipelinedFlushKeysP = graphPanel.new(
  title='Pipelined Flush Keys',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='The keys of pipelined flush.',
)
.addTarget(
  prometheus.target(
    'sum(delta(tidb_tikvclient_pipelined_flush_len_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local pipelinedFlushSizeP = graphPanel.new(
  title='Pipelined Flush Size',
  datasource=myDS,
  legend_rightSide=false,
  format='bytes',
  description='The size of pipelined flush.',
)
.addTarget(
  prometheus.target(
    'sum(delta(tidb_tikvclient_pipelined_flush_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

local pipelinedFlushDurationP = graphPanel.new(
  title='Pipelined Flush Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The flush duration of each pipelined batch.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_tikvclient_pipelined_flush_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_pipelined_flush_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_tikvclient_pipelined_flush_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.8, sum(rate(tidb_tikvclient_pipelined_flush_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='80',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_pipelined_flush_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))/ sum(rate(tidb_tikvclient_pipelined_flush_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='avg',
  )
);

// ============== Row: Executor ==============
local executorRow = row.new(collapse=true, title='Executor');

local parseDurationP = graphPanel.new(
  title='Parse Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The time cost of parsing SQL to AST',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local compileDurationP = graphPanel.new(
  title='Compile Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The time cost of building the query plan',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_compile_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local executionDurationP = graphPanel.new(
  title='Execution Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The time cost of executing the SQL which does not include the time to get the results of the query.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, sql_type))',
    legendFormat='{{sql_type}}',
  )
);

local expensiveExecutorsOpsP = graphPanel.new(
  title='Expensive Executors OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=10,
  description='TiDB executors using more cpu and memory resources',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_expensive_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local planCacheOpsP = graphPanel.new(
  title='Queries Using Plan Cache OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='TiDB plan cache hit total',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_plan_cache_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local planCacheMissOpsP = graphPanel.new(
  title='Plan Cache Miss OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='TiDB plan cache miss total',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_plan_cache_miss_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local readFromTableCacheOpsP = graphPanel.new(
  title='Read From Table Cache OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='TiDB read table cache hit total',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_read_from_tablecache_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='qps',
  )
);

local planCacheMemUsageP = graphPanel.new(
  title='Plan Cache Memory Usage',
  datasource=myDS,
  legend_rightSide=false,
  format='bytes',
  description='Total memory usage of all prepared plan cache in a instance',
)
.addTarget(
  prometheus.target(
    'tidb_server_plan_cache_instance_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}{{type}}',
  )
);

local planCachePlanNumP = graphPanel.new(
  title='Plan Cache Plan Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB prepared plan cache plan num',
)
.addTarget(
  prometheus.target(
    'tidb_server_plan_cache_instance_plan_num_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}{{type}}',
  )
);

local planCacheProcessDurationP = graphPanel.new(
  title='Plan Cache Process Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The time cost of Plan Cache Process',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_plan_cache_process_duration_seconds_sum{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", sql_type!="internal"}[1m])) by (le, type) / sum(rate(tidb_server_plan_cache_process_duration_seconds_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", sql_type!="internal"}[1m])) by (le, type)',
    legendFormat='avg-{{type}}',
  )
);

local mppCoordinatorCounterP = graphPanel.new(
  title='Mpp Coordinator Counter',
  datasource=myDS,
  legend_rightSide=false,
  format='none',
  description='Records Mpp coordinator related stats',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_mpp_coordinator_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local mppCoordinatorLatencyP = graphPanel.new(
  title='Mpp Coordinator Latency',
  datasource=myDS,
  legend_rightSide=false,
  format='ms',
  description='Records Mpp coordinator related stats',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_executor_mpp_coordinator_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='0.95-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.75, sum(rate(tidb_executor_mpp_coordinator_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='0.75-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.50, sum(rate(tidb_executor_mpp_coordinator_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='0.50-{{type}}',
  )
);

local indexLookUpOpsP = graphPanel.new(
  title='IndexLookUp OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='The OPS for IndexLookUp Executor or Cop tasks',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_expensive_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="IndexLookUpExecutor"}[1m]))',
    legendFormat='executor-total',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_row_number_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="enable_index_lookup_push_down"}[1m]))',
    legendFormat='executor-index-lookup-pushdown',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_cop_task_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type=~"index_scan_.*"}[1m]))',
    legendFormat='cop-task-index-scan-total',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_cop_task_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="index_scan_with_lookup_push_down"}[1m]))',
    legendFormat='cop-task-index-scan-with-lookup-pushdown',
  )
);

local indexLookUpDurationP = graphPanel.new(
  title='IndexLookUp Duration',
  datasource=myDS,
  legend_rightSide=false,
  format='s',
  description='The time cost of executing the IndexLookUp executor',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_executor_index_lookup_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="enable_index_lookup_push_down"}[1m])) by (le))',
    legendFormat='index-lookup-pushdown p80',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_executor_index_lookup_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="enable_index_lookup_push_down"}[1m])) by (le))',
    legendFormat='index-lookup-pushdown p95',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_executor_index_lookup_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="enable_index_lookup_push_down"}[1m])) by (le))',
    legendFormat='index-lookup-pushdown p99',
  )
);

local indexLookUpRowsP = graphPanel.new(
  title='IndexLookUp Rows',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='The processed rate of rows in IndexLookUp executor',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='total',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type=~"normal"}[1m])) by (type)',
    legendFormat='non_index_lookup_push_down',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_index_lookup_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type=~"index_lookup_push_down_.*"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local indexLookUpRowNumP = graphPanel.new(
  title='IndexLookUp Row Num With PushDown Enabled',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB row histogram bucket statistics in IndexLookUp PushDown executor. It includes both hit and miss rows.',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_executor_index_lookup_row_number_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type=~"enable_index_lookup_push_down"}[1m])) by (le)',
    legendFormat='{{le}}',
  )
);

// ============== Row: Distsql ==============
local distsqlRow = row.new(collapse=true, title='Distsql');

local distsqlDurationP = graphPanel.new(
  title='Distsql Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='s',
  logBase1Y=2,
  description='durations of distsql execution by type',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='90-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.50, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='50-{{type}}',
  )
);

local distsqlQpsP = graphPanel.new(
  title='Distsql QPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='distsql query handling durations per second',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_distsql_handle_query_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (copr_type)',
    legendFormat='{{copr_type}}',
  )
);

local distsqlPartialQpsP = graphPanel.new(
  title='Distsql Partial QPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='the numebr of distsql partial scan numbers',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_distsql_scan_keys_partial_num_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='',
  )
);

local scanKeysNumP = graphPanel.new(
  title='Scan Keys Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='the numebr of distsql scan numbers',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1, sum(rate(tidb_distsql_scan_keys_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='100',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_distsql_scan_keys_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='90',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.50, sum(rate(tidb_distsql_scan_keys_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='50',
  )
);

local scanKeysPartialNumP = graphPanel.new(
  title='Scan Keys Partial Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='the numebr of distsql partial scan key numbers',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='100',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='90',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='50',
  )
);

local partialNumP = graphPanel.new(
  title='Partial Num',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  logBase1Y=2,
  description='distsql partial numbers per query',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1, sum(rate(tidb_distsql_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='100',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_distsql_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='90',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.50, sum(rate(tidb_distsql_partial_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='50',
  )
);

local coprocessorCacheP = graphPanel.new(
  title='Coprocessor Cache',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_values=true,
  legend_sort='avg',
  legend_sortDesc=true,
  format='none',
  description='TiDB coprocessor cache hit, evict and miss number',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_distsql_copr_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local coprocessorSeconds999P = graphPanel.new(
  title='Coprocessor Seconds 999',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_values=true,
  legend_sort='max',
  legend_sortDesc=true,
  format='s',
  description='kv storage coprocessor processing durations',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_tikvclient_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", store!="0", scope="false", type="Cop"}[1m])) by (le,instance))',
    legendFormat='{{instance}}',
  )
);

// ============== Row: KV Errors ==============
local kvErrorsRow = row.new(collapse=true, title='KV Errors');

local kvBackoffDurationP = graphPanel.new(
  title='KV Backoff Duration',
  datasource=myDS,
  legend_rightSide=false,
  legend_max=true,
  legend_values=true,
  format='s',
  description='kv backoff time durations by type',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_tikvclient_backoff_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_backoff_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_tikvclient_backoff_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='80',
  )
);

local ticlientRegionErrorOpsP = graphPanel.new(
  title='TiClient Region Error OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  legend_values=true,
  format='short',
  description='kv region error times',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_region_err_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_region_err_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}{EXTERNAL_LABELtype="server_is_busy"}[1m]))',
    legendFormat='sum',
    hide=true,
  )
);

local kvBackoffOpsP = graphPanel.new(
  title='KV Backoff OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  legend_total=true,
  legend_values=true,
  format='short',
  description='kv storage backoff times',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_backoff_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local lockResolveOpsP = graphPanel.new(
  title='Lock Resolve OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  legend_values=true,
  format='short',
  description='lock resolve times',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_lock_resolver_actions_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local replicaSelectorFailurePerSecondP = graphPanel.new(
  title='Replica Selector Failure Per Second',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='This metric shows the reasons of replica selector failure (which needs a backoff).',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_replica_selector_failure_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

// ============== Row: KV Request ==============
local kvRequestRow = row.new(collapse=true, title='KV Request');

local kvRequestOpsP = graphPanel.new(
  title='KV Request OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='kv request total by instance and command type',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_request_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope="false"}[1m])) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
  )
);

local kvRequestDuration99ByStoreP = graphPanel.new(
  title='KV Request Duration 99 By Store',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_values=true,
  format='s',
  description='kv requests durations by store',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", store!="0", scope="false"}[1m])) by (le, store))',
    legendFormat='store-{{store}}',
  )
);

local kvRequestDuration99ByTypeP = graphPanel.new(
  title='KV Request Duration 99 By Type',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_values=true,
  format='s',
  description='kv request durations by request type',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", store!="0", scope="false"}[1m])) by (le,type))',
    legendFormat='{{type}}',
  )
);

local kvRequestForwardingOpsP = graphPanel.new(
  title='KV Request Forwarding OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='kv requests that\'s forwarded by different stores',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_forward_request_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (from_store, to_store, result)',
    legendFormat='{{from_store}}-to-{{to_store}}-{{result}}',
  )
);

local kvRequestForwardingOpsByTypeP = graphPanel.new(
  title='KV Request Forwarding OPS By Type',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='kv requests that\'s forwarded by different stores, grouped by request type',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_forward_request_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, result)',
    legendFormat='{{type}}-{{result}}',
  )
);

local successfulKvRequestWaitDurationP = graphPanel.new(
  title='Successful KV Request Wait Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_min=true,
  legend_values=true,
  format='s',
  description='KV request wait duration caused by Resource Control (RU). This shows the time a request waits in TiDB client before being sent to TiKV due to RU token bucket throttling.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(resource_manager_client_request_success_bucket{k8s_cluster="$k8s_cluster", tidb_cluster_id="$tidb_cluster", instance=~"$instance"}[1m])) by (instance, resource_group, le))',
    legendFormat='{{instance}}-{{resource_group}}-99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9, sum(rate(resource_manager_client_request_success_bucket{k8s_cluster="$k8s_cluster", tidb_cluster_id="$tidb_cluster", instance=~"$instance"}[1m])) by (instance, resource_group, le))',
    legendFormat='{{instance}}-{{resource_group}}-90',
  )
);

local regionCacheOkOpsP = graphPanel.new(
  title='Region Cache OK OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB successful region cache operations count',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_region_cache_operations_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", result="ok"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local regionCacheErrorOpsP = graphPanel.new(
  title='Region Cache Error OPS',
  datasource=myDS,
  legend_rightSide=false,
  format='short',
  description='TiDB error region cache operations count',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_region_cache_operations_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", result="err"}[1m])) by (type)',
    legendFormat='{{type}}-err',
  )
);

local loadRegionDurationP = graphPanel.new(
  title='Load Region Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='s',
  description='TiDB loading region cache durations',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_load_region_cache_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_load_region_cache_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type) / sum(rate(tidb_tikvclient_load_region_cache_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type)',
    legendFormat='avg-{{type}}',
  )
);

local rpcLayerLatencyP = graphPanel.new(
  title='RPC Layer Latency',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_current=true,
  legend_values=true,
  format='s',
  description='Time spent on the RPC layer between TiDB and TiKV, including the part used in the TiDB batch client',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_rpc_net_latency_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope="false"}[1m])) by (le, store))',
    legendFormat='99-store{{store}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_rpc_net_latency_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope="false"}[1m])) by (le, store) / sum(rate(tidb_tikvclient_rpc_net_latency_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", scope="false"}[1m])) by (le, store)',
    legendFormat='avg-store{{store}}',
  )
);

local staleReadHitMissOpsP = graphPanel.new(
  title='Stale Read Hit/Miss OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='TiDB hit/miss stale-read operations count',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_stale_read_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (result)',
    legendFormat='{{result}}',
  )
);

local staleReadReqOpsP = graphPanel.new(
  title='Stale Read Req OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='short',
  description='TiDB stale-read requests count',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_stale_read_req_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local staleReadReqTrafficP = graphPanel.new(
  title='Stale Read Req Traffic',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='Bps',
  description='TiDB stale-read requests traffic statistic',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_stale_read_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (result, direction)',
    legendFormat='{{result}}-{{direction}}',
  )
);

local clientSideSlowScoreP = graphPanel.new(
  title='Client-side Slow Score',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='none',
  description='The slow score calculated by time cost of some specific TiKV RPC requests.',
)
.addTarget(
  prometheus.target(
    'max(tidb_tikvclient_store_slow_score{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) by (store)',
    legendFormat='store-{{store}}',
  )
);

local tikvSideSlowScoreP = graphPanel.new(
  title='TiKV-side Slow Score',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='none',
  description='The slow score calculated by TiKV rafstore and sent to TiDB via health feedback.',
)
.addTarget(
  prometheus.target(
    'max(tidb_tikvclient_feedback_slow_score{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) by (store)',
    legendFormat='store-{{store}}',
  )
);

local readReqTrafficP = graphPanel.new(
  title='Read Req Traffic',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_avg=true,
  legend_current=true,
  legend_max=true,
  legend_values=true,
  format='Bps',
  description='TiDB read requests traffic statistic',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_read_request_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, result)',
    legendFormat='{{type}}-{{result}}',
  )
);

// ============== Row: PD Client ==============
local pdClientRow = row.new(collapse=true, title='PD Client');

local pdClientCmdOpsP = graphPanel.new(
  title='PD Client CMD OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='short',
  description='pd command count by type',
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type!="tso"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local pdClientCmdDurationP = graphPanel.new(
  title='PD Client CMD Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='s',
  description='pd client command durations by type within 99.9 percent buckets',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type!~"tso|tso_async_wait"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type!~"tso|tso_async_wait"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type!~"tso|tso_async_wait"}[1m])) by (le, type))',
    legendFormat='90-{{type}}',
  )
);

local pdClientCmdFailOpsP = graphPanel.new(
  title='PD Client CMD Fail OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='short',
  description='pd client command fail count by type',
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_cmd_handle_failed_cmds_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local pdTsoOpsP = graphPanel.new(
  title='PD TSO OPS',
  datasource=myDS,
  format='none',
  description='The duration of a client calling GetTSAsync until received the TS result.',
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m]))',
    legendFormat='cmd',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_request_handle_requests_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m]))',
    legendFormat='request',
  )
);

local pdTsoWaitDurationP = graphPanel.new(
  title='PD TSO Wait Duration',
  datasource=myDS,
  format='s',
  description='The duration of a client starting to wait for the TS until received the TS result.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="wait"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="wait"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="wait"}[1m])) by (le))',
    legendFormat='90',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_cmd_handle_cmds_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="wait"}[1m])) / sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="wait"}[1m]))',
    legendFormat='avg',
  )
);

local pdTsoRpcDurationP = graphPanel.new(
  title='PD TSO RPC Duration',
  datasource=myDS,
  format='s',
  description='The duration of a client sending TSO request until received the response.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m])) by (le))',
    legendFormat='9999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_request_handle_requests_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m])) / sum(rate(pd_client_request_handle_requests_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso"}[1m]))',
    legendFormat='avg',
  )
);

local estimateTsoRttLatencyP = graphPanel.new(
  title='Estimate TSO RTT Latency',
  datasource=myDS,
  format='s',
  description='The estimated latency of TSO RPC calls that\'s used to adjust batching time for parallel RPC requests',
)
.addTarget(
  prometheus.target(
    'pd_client_request_estimate_tso_latency{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}-{{stream}}',
  )
);

local asyncTsoDurationP = graphPanel.new(
  title='Async TSO Duration',
  datasource=myDS,
  format='s',
  description='The duration of the async TS until called the Wait function.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso_async_wait"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso_async_wait"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", type="tso_async_wait"}[1m])) by (le))',
    legendFormat='90',
  )
);

local requestForwardedStatusP = graphPanel.new(
  title='Request Forwarded Status',
  datasource=myDS,
  legend_rightSide=false,
  legend_alignAsTable=false,
  legend_hideEmpty=true,
  legend_hideZero=true,
  format='none',
  description='It indicates if a request of PD client is forwarded by the PD follower',
)
.addTarget(
  prometheus.target(
    'pd_client_request_forwarded_status',
    legendFormat='{{delegate}}-{{host}}',
  )
);

local pdHttpRequestDurationP = graphPanel.new(
  title='PD HTTP Request Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='s',
  description='The duration of a client sending one HTTP request to PD util received the response.',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='999-all',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99-all',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='90-all',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_pd_api_execution_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le, type))',
    legendFormat='90-{{type}}',
  )
);

local pdHttpRequestOpsP = graphPanel.new(
  title='PD HTTP Request OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='short',
  description='PD HTTP API request count per second.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_pd_api_request_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m]))',
    legendFormat='all',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_pd_api_request_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

local pdHttpRequestFailOpsP = graphPanel.new(
  title='PD HTTP Request Fail OPS',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='short',
  description='PD failed HTTP request count per second.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_pd_api_request_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", result!~"200.*"}[1m]))',
    legendFormat='all',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_pd_api_request_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", result!~"200.*"}[1m])) by (type, result)',
    legendFormat='{{type}} - {{result}}',
  )
);

local staleRegionFromPdP = graphPanel.new(
  title='Stale Region From PD',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='ops',
  description='The stale regions from PD per second.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_stale_region_from_pd{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s]))',
    legendFormat='all',
  )
);

local circuitBreakerEventP = graphPanel.new(
  title='Circuit Breaker Event',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(pd_client_request_circuit_breaker_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (name, event)',
    legendFormat='{{name}}-{{event}}',
  )
);

local tidbWaitTsoFutureDurationP = graphPanel.new(
  title='TiDB Wait TSO Future Duration',
  datasource=myDS,
  legend_rightSide=true,
  legend_alignAsTable=true,
  format='s',
  description='How long tidb side wait for tso future',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_tikvclient_ts_future_wait_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_tikvclient_ts_future_wait_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_tikvclient_ts_future_wait_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='90',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_tikvclient_ts_future_wait_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le) / sum(rate(tidb_tikvclient_ts_future_wait_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le)',
    legendFormat='avg',
  )
);

// Merge together.
local panelW = 12;
local panelH = 7;
local rowW = 24;
local rowH = 1;

local rowPos = {x:0, y:0, w:rowW, h:rowH};
local leftPanelPos = {x:0, y:0, w:panelW, h:panelH};
local rightPanelPos = {x:panelW, y:0, w:panelW, h:panelH};
local thirdPanelW = 8;
local leftThirdPanelPos = {x:0, y:0, w:thirdPanelW, h:panelH};
local midThirdPanelPos = {x:thirdPanelW, y:0, w:thirdPanelW, h:panelH};
local rightThirdPanelPos = {x:thirdPanelW * 2, y:0, w:thirdPanelW, h:panelH};

newDash
.addPanel(
  querySummaryRow
  .addPanel(durationP, gridPos=leftPanelPos)
  .addPanel(cpsP, gridPos=rightPanelPos)
  .addPanel(qpsP, gridPos=leftPanelPos)
  .addPanel(cpsByInstP, gridPos=rightPanelPos)
  .addPanel(failedQueryOPMP, gridPos=leftPanelPos)
  .addPanel(affectedRowsP, gridPos=rightPanelPos)
  .addPanel(slowQueryP, gridPos=leftPanelPos)
  .addPanel(connIdleDurationP, gridPos=rightPanelPos)
  .addPanel(duration999P, gridPos=leftPanelPos)
  .addPanel(duration99P, gridPos=rightPanelPos)
  .addPanel(duration95P, gridPos=leftPanelPos)
  .addPanel(duration80P, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  queryDetailRow
  .addPanel(duration80ByInstP, gridPos=leftPanelPos)
  .addPanel(duration95ByInstP, gridPos=rightPanelPos)
  .addPanel(duration99ByInstP, gridPos=leftPanelPos)
  .addPanel(duration999ByInstP, gridPos=rightPanelPos)
  .addPanel(failedQueryOPMDetailP, gridPos=leftPanelPos)
  .addPanel(internalSqlOpsP, gridPos=rightPanelPos)
  .addPanel(queriesInMultiStmtP, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  serverRow
  .addPanel(tidbServerStatusP, gridPos=leftPanelPos)
  .addPanel(uptimeP, gridPos=rightPanelPos)
  .addPanel(cpuUsageP, gridPos=leftPanelPos)
  .addPanel(memUsageP, gridPos=rightPanelPos)
  .addPanel(runtimeGcRateP, gridPos=leftPanelPos)
  .addPanel(openFdCountP, gridPos=rightPanelPos)
  .addPanel(connectionCountP, gridPos=leftPanelPos)
  .addPanel(eventsOpmP, gridPos=rightPanelPos)
  .addPanel(disconnectionCountP, gridPos=leftPanelPos)
  .addPanel(prepareStmtCountP, gridPos=rightPanelPos)
  .addPanel(goroutineCountP, gridPos=leftPanelPos)
  .addPanel(panicCriticalErrorP, gridPos=rightPanelPos)
  .addPanel(keepAliveOpmP, gridPos=leftPanelPos)
  .addPanel(getTokenDurationP, gridPos=rightPanelPos)
  .addPanel(timeJumpBackOpsP, gridPos=leftPanelPos)
  .addPanel(clientDataTrafficP, gridPos=rightPanelPos)
  .addPanel(skipBinlogCountP, gridPos=leftPanelPos)
  .addPanel(rcCheckTsWriteConflictP, gridPos=rightPanelPos)
  .addPanel(handshakeErrorOpsP, gridPos=leftPanelPos)
  .addPanel(internalSessionsP, gridPos=rightPanelPos)
  .addPanel(activeUsersP, gridPos=leftPanelPos)
  .addPanel(connPerTlsCipherP, gridPos=rightPanelPos)
  .addPanel(connPerTlsVersionP, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  transactionRow
  .addPanel(txnOpsP, gridPos=leftPanelPos)
  .addPanel(txnDurationP, gridPos=rightPanelPos)
  .addPanel(txnStmtNumP, gridPos=leftPanelPos)
  .addPanel(txnRetryNumP, gridPos=rightPanelPos)
  .addPanel(sessionRetryErrorOpsP, gridPos=leftPanelPos)
  .addPanel(commitTokenWaitP, gridPos=rightPanelPos)
  .addPanel(kvTxnOpsP, gridPos=leftPanelPos)
  .addPanel(kvTxnDurationP, gridPos=rightPanelPos)
  .addPanel(txnRegionsNumP, gridPos=leftPanelPos)
  .addPanel(txnWriteKvNumRateP, gridPos=rightPanelPos)
  .addPanel(txnWriteKvNumP, gridPos=leftPanelPos)
  .addPanel(stmtLockKeysP, gridPos=rightPanelPos)
  .addPanel(sendHeartbeatDurationP, gridPos=leftPanelPos)
  .addPanel(txnWriteSizeRateP, gridPos=rightPanelPos)
  .addPanel(txnWriteSizeP, gridPos=leftPanelPos)
  .addPanel(acquirePessimisticLocksP, gridPos=rightPanelPos)
  .addPanel(ttlLifetimeReachP, gridPos=leftPanelPos)
  .addPanel(loadSafepointOpsP, gridPos=rightPanelPos)
  .addPanel(pessimisticStmtRetryP, gridPos=leftPanelPos)
  .addPanel(txnTypesPerSecP, gridPos=rightPanelPos)
  .addPanel(txnCommitP99BackoffP, gridPos=leftPanelPos)
  .addPanel(safeTsUpdateP, gridPos=rightPanelPos)
  .addPanel(maxSafeTsGapP, gridPos=leftPanelPos)
  .addPanel(assertionP, gridPos=rightPanelPos)
  .addPanel(txnExecStatesDurationP, gridPos=leftPanelPos)
  .addPanel(txnWithLockExecStatesDurationP, gridPos=rightPanelPos)
  .addPanel(txnExecStatesDurationSumP, gridPos=leftPanelPos)
  .addPanel(txnEnterStateP, gridPos=rightPanelPos)
  .addPanel(txnLeaveStateP, gridPos=leftPanelPos)
  .addPanel(txnStateCountChangeP, gridPos=rightPanelPos)
  .addPanel(fairLockingUsageP, gridPos=leftPanelPos)
  .addPanel(fairLockingKeysP, gridPos=rightPanelPos)
  .addPanel(pipelinedFlushKeysP, gridPos=leftPanelPos)
  .addPanel(pipelinedFlushSizeP, gridPos=rightPanelPos)
  .addPanel(pipelinedFlushDurationP, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  executorRow
  .addPanel(parseDurationP, gridPos=leftPanelPos)
  .addPanel(compileDurationP, gridPos=rightPanelPos)
  .addPanel(executionDurationP, gridPos=leftPanelPos)
  .addPanel(expensiveExecutorsOpsP, gridPos=rightPanelPos)
  .addPanel(planCacheOpsP, gridPos=leftPanelPos)
  .addPanel(planCacheMissOpsP, gridPos=rightPanelPos)
  .addPanel(readFromTableCacheOpsP, gridPos=leftPanelPos)
  .addPanel(planCacheMemUsageP, gridPos=rightPanelPos)
  .addPanel(planCachePlanNumP, gridPos=leftPanelPos)
  .addPanel(planCacheProcessDurationP, gridPos=rightPanelPos)
  .addPanel(mppCoordinatorCounterP, gridPos=leftPanelPos)
  .addPanel(mppCoordinatorLatencyP, gridPos=rightPanelPos)
  .addPanel(indexLookUpOpsP, gridPos=leftPanelPos)
  .addPanel(indexLookUpDurationP, gridPos=rightPanelPos)
  .addPanel(indexLookUpRowsP, gridPos=leftPanelPos)
  .addPanel(indexLookUpRowNumP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  distsqlRow
  .addPanel(distsqlDurationP, gridPos=leftPanelPos)
  .addPanel(distsqlQpsP, gridPos=rightPanelPos)
  .addPanel(distsqlPartialQpsP, gridPos=leftThirdPanelPos)
  .addPanel(scanKeysNumP, gridPos=midThirdPanelPos)
  .addPanel(scanKeysPartialNumP, gridPos=rightThirdPanelPos)
  .addPanel(partialNumP, gridPos=leftThirdPanelPos)
  .addPanel(coprocessorSeconds999P, gridPos=midThirdPanelPos)
  .addPanel(coprocessorCacheP, gridPos=rightThirdPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  kvErrorsRow
  .addPanel(kvBackoffDurationP, gridPos=leftPanelPos)
  .addPanel(ticlientRegionErrorOpsP, gridPos=rightPanelPos)
  .addPanel(kvBackoffOpsP, gridPos=leftPanelPos)
  .addPanel(lockResolveOpsP, gridPos=rightPanelPos)
  .addPanel(replicaSelectorFailurePerSecondP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  kvRequestRow
  .addPanel(kvRequestOpsP, gridPos=leftThirdPanelPos)
  .addPanel(kvRequestDuration99ByStoreP, gridPos=midThirdPanelPos)
  .addPanel(kvRequestDuration99ByTypeP, gridPos=rightThirdPanelPos)
  .addPanel(kvRequestForwardingOpsP, gridPos=leftThirdPanelPos)
  .addPanel(kvRequestForwardingOpsByTypeP, gridPos=midThirdPanelPos)
  .addPanel(successfulKvRequestWaitDurationP, gridPos=leftThirdPanelPos)
  .addPanel(regionCacheOkOpsP, gridPos=rightThirdPanelPos)
  .addPanel(regionCacheErrorOpsP, gridPos=leftThirdPanelPos)
  .addPanel(loadRegionDurationP, gridPos=midThirdPanelPos)
  .addPanel(rpcLayerLatencyP, gridPos=rightThirdPanelPos)
  .addPanel(staleReadHitMissOpsP, gridPos=leftThirdPanelPos)
  .addPanel(staleReadReqOpsP, gridPos=midThirdPanelPos)
  .addPanel(staleReadReqTrafficP, gridPos=rightThirdPanelPos)
  .addPanel(clientSideSlowScoreP, gridPos=leftThirdPanelPos)
  .addPanel(tikvSideSlowScoreP, gridPos=midThirdPanelPos)
  .addPanel(readReqTrafficP, gridPos=rightThirdPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  pdClientRow
  .addPanel(pdClientCmdOpsP, gridPos=leftThirdPanelPos)
  .addPanel(pdClientCmdDurationP, gridPos=midThirdPanelPos)
  .addPanel(pdClientCmdFailOpsP, gridPos=rightThirdPanelPos)
  .addPanel(pdTsoOpsP, gridPos=leftThirdPanelPos)
  .addPanel(pdTsoWaitDurationP, gridPos=midThirdPanelPos)
  .addPanel(pdTsoRpcDurationP, gridPos=rightThirdPanelPos)
  .addPanel(estimateTsoRttLatencyP, gridPos=leftThirdPanelPos)
  .addPanel(asyncTsoDurationP, gridPos=midThirdPanelPos)
  .addPanel(requestForwardedStatusP, gridPos=rightThirdPanelPos)
  .addPanel(pdHttpRequestDurationP, gridPos=leftThirdPanelPos)
  .addPanel(pdHttpRequestOpsP, gridPos=midThirdPanelPos)
  .addPanel(pdHttpRequestFailOpsP, gridPos=rightThirdPanelPos)
  .addPanel(staleRegionFromPdP, gridPos=leftThirdPanelPos)
  .addPanel(circuitBreakerEventP, gridPos=midThirdPanelPos)
  .addPanel(tidbWaitTsoFutureDurationP, gridPos=rightThirdPanelPos)
  ,
  gridPos=rowPos
)
