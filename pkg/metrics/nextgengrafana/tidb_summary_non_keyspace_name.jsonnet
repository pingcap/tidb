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
)
.addTarget(
  prometheus.target(
    'tidb_server_maxprocs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='quota-{{instance}}',
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
    'go_memory_classes_heap_objects_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"} + go_memory_classes_heap_unused_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='HeapInuse-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tidb_server_memory_quota_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tidb"}',
    legendFormat='quota-{{instance}}',
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
  serverRow
  .addPanel(uptimeP, gridPos=leftPanelPos)
  .addPanel(cpuP, gridPos=rightPanelPos)
  .addPanel(memP, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
