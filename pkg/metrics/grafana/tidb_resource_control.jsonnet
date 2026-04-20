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

local grafana = import "grafonnet/grafana.libsonnet";
local dashboard = grafana.dashboard;
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local template = grafana.template;

local myNameFlag = "DS_TEST-CLUSTER";
local myDS = "${" + myNameFlag + "}";

local TiDBResourceControlDash = dashboard.new(
  title="Test-Cluster-TiDB-Resource-Control",
  editable=true,
  graphTooltip="shared_crosshair",
  refresh="30s",
  time_from="now-1h",
).addInput(
  name=myNameFlag,
  label="test-cluster",
  type="datasource",
  pluginId="prometheus",
  pluginName="Prometheus",
).addTemplate(
  template.new(
    allValues=null,
    current=null,
    datasource=myDS,
    hide="all",
    includeAll=false,
    label="tidb_cluster",
    multi=false,
    name="tidb_cluster",
    query='label_values(pd_cluster_status{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
    refresh="time",
    regex="",
    sort=1,
    tagValuesQuery="",
  )
).addTemplate(
  template.new(
    datasource=myDS,
    hide=2,
    label="K8s-cluster",
    name="k8s_cluster",
    query="label_values(pd_cluster_status, k8s_cluster)",
    refresh="time",
    sort=1,
  )
).addTemplate(
  template.new(
    allValues=".*",
    current=null,
    datasource=myDS,
    hide="",
    includeAll=true,
    label="TiDB Instance",
    multi=false,
    name="tidb_instance",
    query='label_values(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh="load",
    regex="",
    sort=1,
    tagValuesQuery="",
  )
).addTemplate(
  template.new(
    allValues=".*",
    current=null,
    datasource=myDS,
    hide="",
    includeAll=true,
    label="Resource Group",
    multi=true,
    name="resource_group",
    query='label_values(tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, resource_group)',
    refresh="load",
    regex="",
    sort=1,
    tagValuesQuery="",
  )
).addTemplate(
  template.new(
    allValues=".*",
    current=null,
    datasource=myDS,
    hide="",
    includeAll=true,
    label="Keyspace Name",
    multi=true,
    name="keyspace_name",
    query='label_values(resource_manager_server_group_config{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, keyspace_name)',
    refresh="load",
    regex="",
    sort=1,
    tagValuesQuery="",
  )
);

local panelW = 12;
local panelH = 7;
local rowW = 24;
local rowH = 1;
local rowPos = { x: 0, y: 0, w: rowW, h: rowH };
local leftPanelPos = { x: 0, y: 0, w: panelW, h: panelH };
local rightPanelPos = { x: panelW, y: 0, w: panelW, h: panelH };
local fullPanelPos = { x: 0, y: 0, w: rowW, h: panelH };

local tidbSelector = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"';
local tidbInstanceSelector = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"';
local clientRCSelector = tidbSelector + ', keyspace_name=~"$keyspace_name"';
local pdRCSelector = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group", keyspace_name=~"$keyspace_name"';

local sqlLatencyRow = row.new(collapse=true, title="SQL latency / Time_queued_by_rc / query_total");
local sqlLatencyPanel = graphPanel.new(
  title="SQL Latency And Query Total",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  description="Shows resource-group SQL latency together with instance-level RC queue wait and query throughput.",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{' + tidbSelector + '}[1m])) by (le, instance, resource_group))',
    legendFormat="{{instance}}-{{resource_group}}-p99",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(tidb_server_handle_query_duration_seconds_bucket{' + tidbSelector + '}[1m])) by (le, instance, resource_group))',
    legendFormat="{{instance}}-{{resource_group}}-p90",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_slow_query_wait_duration_seconds_bucket{' + tidbInstanceSelector + ', sql_type="general"}[1m])) by (le, instance))',
    legendFormat="{{instance}}-queue-p99",
  )
).addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{' + tidbSelector + '}[1m])) by (instance, resource_group, result)',
    legendFormat="{{instance}}-{{resource_group}}-{{result}}-qps",
  )
) + {
  seriesOverrides: [
    {
      alias: "/-qps$/",
      yaxis: 2,
    },
  ],
  yaxes: [
    {
      format: "s",
      label: null,
      logBase: 1,
      max: null,
      min: null,
      show: true,
    },
    {
      format: "ops",
      label: null,
      logBase: 1,
      max: null,
      min: null,
      show: true,
    },
  ],
};

local demandPanel = graphPanel.new(
  title="Demand And Fill Rate",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="Shows demand, average RU, fill rate, and throttling together.",
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_demand_ru_per_sec{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-demand",
  )
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_avg_ru_per_sec{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-avg",
  )
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_fill_rate{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-fill",
  )
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_throttled{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-throttled",
  )
);

local tokenRow = row.new(collapse=true, title="Token balance / throttled / token request latency");
local tokenBalancePanel = graphPanel.new(
  title="Token Balance And Throttle",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="Shows available token balance and throttling state by resource group and TiDB instance.",
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_token_balance{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-balance",
  )
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_throttled{' + clientRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-throttled",
  )
);

local tokenLatencyPanel = graphPanel.new(
  title="Token Request Latency And Volume",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  description="Shows instance-level token request latency together with per-group request volume.",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(resource_manager_client_token_request_duration_bucket{' + tidbInstanceSelector + '}[1m])) by (instance, le))',
    legendFormat="{{instance}}-p99",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.90, sum(rate(resource_manager_client_token_request_duration_bucket{' + tidbInstanceSelector + '}[1m])) by (instance, le))',
    legendFormat="{{instance}}-p90",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_token_request_duration_sum{' + tidbInstanceSelector + '}[1m])) by (instance) / sum(rate(resource_manager_client_token_request_duration_count{' + tidbInstanceSelector + '}[1m])) by (instance)',
    legendFormat="{{instance}}-avg",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_token_request_resource_group{' + tidbSelector + '}[1m])) by (instance, resource_group)',
    legendFormat="{{instance}}-{{resource_group}}-req/s",
  )
) + {
  seriesOverrides: [
    {
      alias: "/-req\\/s$/",
      yaxis: 2,
    },
  ],
  yaxes: [
    {
      format: "s",
      label: null,
      logBase: 1,
      max: null,
      min: null,
      show: true,
    },
    {
      format: "ops",
      label: null,
      logBase: 1,
      max: null,
      min: null,
      show: true,
    },
  ],
};

local serverRow = row.new(collapse=true, title="trickle_duration_ms / active_slot_count / token_loan / slot_events_total");
local serverAllocationPanel = graphPanel.new(
  title="Server Slots, Loan, And Trickle",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="Tracks PD-side slot state, outstanding token loan, slot lifecycle, and average trickle duration.",
).addTarget(
  prometheus.target(
    'resource_manager_server_token_loan{' + pdRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-loan",
  )
).addTarget(
  prometheus.target(
    'resource_manager_server_active_slot_count{' + pdRCSelector + '}',
    legendFormat="{{instance}}-{{resource_group}}-slots",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_server_slot_events_total{' + pdRCSelector + '}[1m])) by (instance, resource_group, event)',
    legendFormat="{{instance}}-{{resource_group}}-{{event}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_server_trickle_duration_ms_sum{' + pdRCSelector + '}[1m])) / sum(rate(resource_manager_server_trickle_duration_ms_count{' + pdRCSelector + '}[1m]))',
    legendFormat="trickle-avg-ms",
  )
);

local limitRow = row.new(collapse=true, title="ru_per_sec / ru_capacity / throttling cause");
local limitConfigPanel = graphPanel.new(
  title="RU Config",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="Shows configured RU rate and burst capacity for the selected resource groups.",
).addTarget(
  prometheus.target(
    'max by (instance, resource_group, type) (resource_manager_server_group_config{' + pdRCSelector + ', type="ru_per_sec"})',
    legendFormat="{{instance}}-{{resource_group}}-ru_per_sec",
  )
).addTarget(
  prometheus.target(
    'max by (instance, resource_group, type) (resource_manager_server_group_config{' + pdRCSelector + ', type="ru_capacity"})',
    legendFormat="{{instance}}-{{resource_group}}-ru_capacity",
  )
);

local limitCausePanel = graphPanel.new(
  title="Throttling Causes",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="Shows whether throttling and trickle are caused by group fill/burst or by service_limit.",
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_server_request_cause_total{' + pdRCSelector + '}[1m])) by (instance, resource_group, kind, cause)',
    legendFormat="{{instance}}-{{resource_group}}-{{kind}}-{{cause}}",
  )
);

TiDBResourceControlDash
.addPanel(
  sqlLatencyRow
  .addPanel(sqlLatencyPanel, gridPos=leftPanelPos)
  .addPanel(demandPanel, gridPos=rightPanelPos),
  gridPos=rowPos
).addPanel(
  tokenRow
  .addPanel(tokenBalancePanel, gridPos=leftPanelPos)
  .addPanel(tokenLatencyPanel, gridPos=rightPanelPos),
  gridPos=rowPos
).addPanel(
  serverRow
  .addPanel(serverAllocationPanel, gridPos=fullPanelPos),
  gridPos=rowPos
).addPanel(
  limitRow
  .addPanel(limitConfigPanel, gridPos=leftPanelPos)
  .addPanel(limitCausePanel, gridPos=rightPanelPos),
  gridPos=rowPos
)
