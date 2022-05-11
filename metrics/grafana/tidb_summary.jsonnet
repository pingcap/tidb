// Copyright 2022 PingCAP, Inc.
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
  title='Test-Cluster-TiDB-Summary',
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
    hide= 2,
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(pd_cluster_status, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  // Default template for tidb-cloud
  template.new(
    allValues=null,
    current=null,
    datasource=myDS,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(pd_cluster_status{k8s_cluster="$kuberentes"}, tidb_cluster)',
    refresh='time',
    regex='',
    sort=1,
    tagValuesQuery='',
  )
).addTemplate(
  // Default template for tidb-cloud
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

// Server row and its panels
local serverRow = row.new(collapse=true, title='Server');
local uptimeP = graphPanel.new(
  title='Uptime',
  datasource=myDS,
  legend_rightSide=true,
  format='s',
  description='TiDB uptime since the last restart.',
)
.addTarget(
  prometheus.target(
    'time() - process_start_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='{{instance}}',
  )
);

local connectionP = graphPanel.new(
  title='Connection Count',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB current connection counts.',
  format='short',
  stack=true,
)
.addTarget(
  prometheus.target(
    'tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"})',
    legendFormat='total',
  )
);

local cpuP = graphPanel.new(
  title='CPU Usage',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB CPU usage calculated with process CPU running seconds.',
  format='percentunit',
)
.addTarget(
  prometheus.target(
    'rate(process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}[1m])',
    legendFormat='{{instance}}',
  )
);

local memP = graphPanel.new(
  title='Memory Usage',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB process rss memory usage.TiDB heap memory size in use.',
  format='bytes',
)
.addTarget(
  prometheus.target(
    'process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='process-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'go_memstats_heap_inuse_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapInuse-{{instance}}',
  )
);

// Query Summary
local queryRow = row.new(collapse=true, title='Query Summary');
local durationP = graphPanel.new(
  title='Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB query durations by histogram buckets with different percents.',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type!="internal"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type!="internal"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_handle_query_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type!="internal"}[30s])) / sum(rate(tidb_server_handle_query_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", sql_type!="internal"}[30s]))',
    legendFormat='avg',
  )
);

local failedP = graphPanel.new(
  title='Failed Query OPS',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB failed query statistics by query type.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(increase(tidb_server_execute_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
  )
);

local cpsP = graphPanel.new(
  title='Command Per Second',
  datasource=myDS,
  legend_rightSide=true,
  description='MySQL command processing numbers per second. See https://dev.mysql.com/doc/internals/en/text-protocol.html and https://dev.mysql.com/doc/internals/en/prepared-statements.html',
  format='short',
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
    'sum(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) * sum(rate(tidb_server_handle_query_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m])) / sum(rate(tidb_server_handle_query_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat='ideal CPS',
    hide=true,
  )
);

local cpsByInstP = graphPanel.new(
  title='CPS By Instance',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB query total statistics including both successful and failed ones.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local qpsP = graphPanel.new(
  title='QPS',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB statement statistics.',
  format='short',
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

local cpsByCMDP = graphPanel.new(
  title='CPS by CMD',
  datasource=myDS,
  legend_rightSide=true,
  description='MySQL command statistics by command type. See https://dev.mysql.com/doc/internals/en/text-protocol.html and https://dev.mysql.com/doc/internals/en/prepared-statements.html',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

// Query Detail row and its panels
local queryDetailRow = row.new(collapse=true, title='Query Detail');
local parseP = graphPanel.new(
  title='Parse Duration',
  datasource=myDS,
  legend_rightSide=true,
  format='s',
  description='The time cost of parsing SQL to AST',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_parse_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='95',
  )
);

local compileP = graphPanel.new(
  title='Compile Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='The time cost of building the query plan',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_compile_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_compile_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='95',
  )
);

local exeP = graphPanel.new(
  title='Execution Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='The time cost of executing the SQL which does not include the time to get the results of the query.',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_execute_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le))',
    legendFormat='95',
  )
);

local planCacheP = graphPanel.new(
  title='Queries Using Plan Cache OPS',
  datasource=myDS,
  legend_rightSide=true,
  description='TiDB plan cache hit total.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_server_plan_cache_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);

// Transaction row and its panels
local txnRow = row.new(collapse=true, title='Transaction');
local tpsP = graphPanel.new(
  title='TPS',
  datasource=myDS,
  legend_rightSide=true,
  format='short',
  description='TiDB transaction processing counts by type.',
)
.addTarget(
  prometheus.target(
    'sum(rate(tidb_session_transaction_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, txn_mode)',
    legendFormat='{{type}}-{{txn_mode}}',
  )
);

local txnDurationP = graphPanel.new(
  title='Transaction Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='Bucketed histogram of transaction execution durations, including retry.',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_session_transaction_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le, txn_mode))',
    legendFormat='99-{{txn_mode}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tidb_session_transaction_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le, txn_mode))',
    legendFormat='95-{{txn_mode}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tidb_session_transaction_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", sql_type="general"}[1m])) by (le, txn_mode))',
    legendFormat='80-{{txn_mode}}',
  )
);

local maxTxnStmtP = graphPanel.new(
  title='Max Transaction Statement Num',
  datasource=myDS,
  legend_rightSide=true,
  description='The max TiDB statements numbers within one transaction.',
  format='short',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1, sum(rate(tidb_session_transaction_statement_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s])) by (le))',
    legendFormat='max',
  )
);

local maxTxnRetryP = graphPanel.new(
  title='Max Transaction Retry Num',
  datasource=myDS,
  legend_rightSide=true,
  description='The max TiDB transaction retry count.',
  format='short',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.0, sum(rate(tidb_session_retry_num_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s])) by (le))',
    legendFormat='max',
  )
);

// Merge together.
local panelW = 12;
local panelH = 6;
local rowW = 24;
local rowH = 1;

local rowPos = {x:0, y:0, w:rowW, h:rowH};
local leftPanelPos = {x:0, y:0, w:panelW, h:panelH};
local rightPanelPos = {x:panelW, y:0, w:panelW, h:panelH};

newDash
.addPanel(
  serverRow
  .addPanel(uptimeP, gridPos=leftPanelPos)
  .addPanel(connectionP, gridPos=rightPanelPos)
  .addPanel(cpuP, gridPos=leftPanelPos)
  .addPanel(memP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  queryRow
  .addPanel(durationP, gridPos=leftPanelPos)
  .addPanel(failedP, gridPos=rightPanelPos)
  .addPanel(cpsP, gridPos=leftPanelPos)
  .addPanel(cpsByInstP, gridPos=rightPanelPos)
  .addPanel(qpsP, gridPos=leftPanelPos)
  .addPanel(cpsByCMDP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  queryDetailRow
  .addPanel(parseP, gridPos=leftPanelPos)
  .addPanel(compileP, gridPos=rightPanelPos)
  .addPanel(exeP, gridPos=leftPanelPos)
  .addPanel(planCacheP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  txnRow
  .addPanel(tpsP, gridPos=leftPanelPos)
  .addPanel(txnDurationP, gridPos=rightPanelPos)
  .addPanel(maxTxnStmtP, gridPos=leftPanelPos)
  .addPanel(maxTxnRetryP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
