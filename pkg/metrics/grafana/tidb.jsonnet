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

// Merge together.
local panelW = 12;
local panelH = 7;
local rowW = 24;
local rowH = 1;

local rowPos = {x:0, y:0, w:rowW, h:rowH};
local leftPanelPos = {x:0, y:0, w:panelW, h:panelH};
local rightPanelPos = {x:panelW, y:0, w:panelW, h:panelH};

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
