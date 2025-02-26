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

local tableNewPanel = import "grafonnet-7.0/panel/table.libsonnet";
local grafana = import "grafonnet/grafana.libsonnet";
local dashboard = grafana.dashboard;
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local template = grafana.template;
local transformation = grafana.transformation;

local myNameFlag = "DS_TEST-CLUSTER";
local myDS = "${" + myNameFlag + "}";

// A new dashboard
// Add the template variables
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
  // Default template for tidb-cloud
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
  // Default template for tidb-cloud
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
    label="TiKV Instance",
    multi=false,
    name="tikv_instance",
    query='label_values(tikv_engine_size_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
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
);


//*  ==============Panel (Resource Unit)==================
//*  Panel Title: Resource Unit
//*  Description: The metrics about request unit(abstract unit) cost for all resource groups.
//*  Panels: 7
//*  ==============Panel (Resource Unit)==================
local ruRow = row.new(collapse=true, title="Resource Unit");

local ConfigPanel = tableNewPanel.new(
  title="RU Config",
  datasource=myDS,
).addTarget(
  prometheus.target(
    'max by (resource_group, type) (resource_manager_server_group_config{type="priority"})',
    legendFormat="{{resource_group}}",
    instant=true,
  )
).addTarget(
  prometheus.target(
    'max by (resource_group, type) (resource_manager_server_group_config{type="ru_capacity"}) < bool 0',
    legendFormat="{{resource_group}}",
    instant=true,
  )
).addTarget(
  prometheus.target(
    'max by (resource_group, type) (resource_manager_server_group_config{type="ru_per_sec"})',
    legendFormat="{{resource_group}}",
    instant=true,
  )
).addTransformation(
  transformation.new("labelsToFields", options={
    valueLabel: "type",
  })
).addTransformation(
  transformation.new("organize", options={
    excludeByName: {
      Time: true,
      __name__: true,
      instance: true,
      job: true,
    },
    indexByName: {
      Time: 0,
      __name__: 1,
      instance: 2,
      job: 3,
      resource_group: 4,
      priority: 5,
      ru_per_sec: 6,
      ru_capacity: 7,
    },
    renameByName: {
      priority: "Priority",
      ru_per_sec: "RU_PER_SEC",
      resource_group: "Group Name",
      ru_capacity: "Burstable",
    },
  })
).addOverride(
  matcher={
    id: "byName",
    options: "Burstable",
  },
  properties=[
    {
      id: "mappings",
      value: [
        {
          options: {
            "0": {
              index: 1,
              text: "false",
            },
            "1": {
              index: 0,
              text: "true",
            },
          },
          type: "value",
        },
      ],
    },
  ],
);

local RUPanel = graphPanel.new(
  title="RU",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about request unit cost for all resource groups.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m])) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m]))',
    legendFormat="total",
  )
);

local RUMaxPanel = graphPanel.new(
  title="RU Max(Max Cost During 20s Period)",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The max request unit cost for resource groups during in a period(20s).",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(resource_manager_resource_unit_read_request_unit_max_per_sec{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}) by (resource_group)',
    legendFormat="{{resource_group}}-read",
  )
).addTarget(
  prometheus.target(
    'sum(resource_manager_resource_unit_write_request_unit_max_per_sec{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}) by (resource_group)',
    legendFormat="{{resource_group}}-write",
  )
);

local RUPerQueryPanel = graphPanel.new(
  title="RU Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The avg request unit cost for each query.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    '(sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (name) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group)) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    '(sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m])) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m]))) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local RRUPanel = graphPanel.new(
  title="RRU",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The read request unit cost for all resource groups.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m]))',
    legendFormat="total",
  )
);

local RRUPerQueryPanel = graphPanel.new(
  title="RRU Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The avg read request unit cost for each query.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (name) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local WRUPanel = graphPanel.new(
  title="WRU",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The write request unit cost for all resource groups.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m]))',
    legendFormat="total",
  )
);

local WRUPerQueryPanel = graphPanel.new(
  title="WRU Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The avg write request unit cost for each query.",
  logBase1Y=10,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp", resource_group=~"$resource_group"}[1m])) by (resource_group) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"|tp"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);


//*  ============== Panel (Resource Details)==================
//*  Panel Title: Resource Details
//*  Description: The metrics about actual resource usage for all resource groups.
//*  Panels: 8
//*  ============== Panel (Resource Details)==================

local resourceRow = row.new(collapse=true, title="Resource Details");
local KVRequestCountPanel = graphPanel.new(
  title="KV Request Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about kv request count for all resource groups.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group, type)',
    legendFormat="{{resource_group}}-{{type}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}-total",
  )
);

local KVRequestCountPerQueryPanel = graphPanel.new(
  title="KV Request Count Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The avg kv request count for each query.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="read", resource_group=~"$resource_group"}[1m])) by (resource_group) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}-read",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="write", resource_group=~"$resource_group"}[1m])) by (resource_group) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}-write",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="read"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total-read",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="write"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total-write",
  )
);

local BytesReadPanel = graphPanel.new(
  title="Bytes Read",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  description="The metrics about bytes read for all resource groups.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_read_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_read_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local BytesReadPerQueryPanel = graphPanel.new(
  title="Bytes Read Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  description="The avg bytes read for each query.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_read_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_read_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local BytesWrittenPanel = graphPanel.new(
  title="Bytes Written",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  description="The metrics about bytes written for all resource groups.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_write_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_write_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local BytesWrittenPerQueryPanel = graphPanel.new(
  title="Bytes Written Per Query",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  description="The avg bytes written for each query.",
  logBase1Y=2,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_write_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m])) by (name) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_write_byte_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m])) / sum(rate(tidb_session_resource_group_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local KVCPUTimePanel = graphPanel.new(
  title="KV CPU Time",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="ms",
  description="The metrics about kv cpu time for all resource groups.",
  logBase1Y=1,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_kv_cpu_time_ms_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_kv_cpu_time_ms_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

local SQLCPUTimePanel = graphPanel.new(
  title="SQL CPU Time",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="ms",
  description="The metrics about sql cpu time for all resource groups.",
  logBase1Y=1,
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_sql_cpu_time_ms_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_sql_cpu_time_ms_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[1m]))',
    legendFormat="total",
  )
);

//*  ==============Panel (Client)==================
//*  Row Title: Client
//*  Description:  The metrics about resource control client
//*  Panels: 7
//*  ==============Panel (Client)==================

local clientRow = row.new(collapse=true, title="Client");

local ActiveResourceGroupPanel = graphPanel.new(
  title="Active Resource Groups",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  bars=true,
  format="short",
  description="The metrics about active resource groups.",
).addTarget(
  prometheus.target(
    'resource_manager_client_resource_group_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}',
    legendFormat="{{instance}}-{{resource_group}}",
  )
);

local TotalKVRequestCountPanel = graphPanel.new(
  title="Total KV Request Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about total kv request count.",
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_success_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance) + sum(rate(resource_manager_client_request_fail{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance)',
    legendFormat="{{instance}}-total",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_success_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, name) + sum(rate(resource_manager_client_request_fail{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, name)',
    legendFormat="total",
  )
);

local FailedKVRequestCountPanel = graphPanel.new(
  title="Failed KV Request Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about failed kv request count.",
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_fail{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}[1m])) by (instance, resource_group, type)',
    legendFormat="{{resource_group}}-{{type}}-{{instance}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_fail{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance)',
    legendFormat="{{instance}}-total",
    hide=true,
  )
);

local SuccessfulKVRequestWaitDurationPanel = graphPanel.new(
  title="Successful KV Request Wait Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about successful kv request wait duration.",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(resource_manager_client_request_success_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}[1m])) by (instance, resource_group, le))',
    legendFormat="{{instance}}-{{resource_group}}-99",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.9, sum(rate(resource_manager_client_request_success_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}[1m])) by (instance, resource_group, le))',
    legendFormat="{{instance}}-{{resource_group}}-90",
  )
);

// Successful KV Request Count
local SuccessfulKVRequestCountPanel = graphPanel.new(
  title="Successful KV Request Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  logBase1Y=2,
  format="short",
  description="The metrics about successful kv request count.",
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_success_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance)',
    legendFormat="{{instance}}-total",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_request_success_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}[1m])) by (instance, resource_group)',
    legendFormat="{{instance}}-{{resource_group}}",
  )
);

// Token Request Handle Duration
local TokenRequestHandleDurationPanel = graphPanel.new(
  title="Token Request Handle Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  logBase1Y=1,
  description="The metrics about token request handle duration.",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(resource_manager_client_token_request_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, le))',
    legendFormat="{{instance}}-99",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(resource_manager_client_token_request_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, le))',
    legendFormat="{{instance}}-999",
  )
);

// Token Request Count
local TokenRequestCountPanel = graphPanel.new(
  title="Token Request Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  description="The metrics about token request count.",
).addTarget(
  prometheus.target(
    'sum(delta(resource_manager_client_token_request_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance)',
    legendFormat="{{instance}}-total",
  )
).addTarget(
  prometheus.target(
    'sum(delta(resource_manager_client_token_request_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", type="success"}[1m])) by (instance)',
    legendFormat="{{instance}}-successful",
  )
).addTarget(
  prometheus.target(
    'sum(delta(resource_manager_client_token_request_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", type="fail"}[1m])) by (instance)',
    legendFormat="{{instance}}-failed",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_client_token_request_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance", resource_group=~"$resource_group"}[1m])) by (instance, resource_group)',
    legendFormat="{{instance}}-{{resource_group}}",
  )
);

//*  ==============Panel (Runaway)==================
//*  Row Title: Runaway
//*  Description: The metrics about runaway resource control
//*  Panels: 2
//*  ==============Panel (Runaway)==================

local runawayRow = row.new(collapse=true, title="Runaway");
// Query Max Duration
local QueryMaxDurationPanel = graphPanel.new(
  title="Query Max Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_avg=true,
  legend_max=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  logBase1Y=2,
  description="TiDB max durations for different resource group",
).addTarget(
  prometheus.target(
    'histogram_quantile(1.0, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", resource_group=~"$resource_group"}[1m])) by (le,resource_group))',
    legendFormat="{{resource_group}}",
  )
);

// Runaway Event
local RunawayEventPanel = graphPanel.new(
  title="Runaway Event",
  datasource=myDS,
  legend_rightSide=true,
  legend_avg=true,
  legend_max=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=2,
  description="Runaway manager events for different resource group",
).addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_runaway_check{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="hit", resource_group=~"$resource_group"}[5m])) by (resource_group)',
    legendFormat="{{resource_group}}-hit",
  )
).addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_runaway_check{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type!="hit", resource_group=~"$resource_group"}[5m])) by (resource_group, type, action)',
    legendFormat="{{resource_group}}-{{type}}-{{action}}",
  )
);

//*  ==============Panel (Priority Task Control)==================
//*  Row Title: Priority Task Control
//*  Description: The metrics about Priority Tasks Control resource control
//*  Panels: 4
//*  ==============Panel (Background Task Control)==================

local priorityTaskRow = row.new(collapse=true, title="Priority Task Control");

// The CPU time used of each priority
local PriorityTaskCPUPanel = graphPanel.new(
  title="CPU Time By Priority",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="µs",
  logBase1Y=1,
  description="The total CPU time cost by tasks of each priority.",
).addTarget(
  prometheus.target(
    'sum(rate(tikv_resource_control_priority_task_exec_duration{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, priority)',
    legendFormat="{{instance}}-{{priority}}",
  )
);

// The CPU Limiter Quota of each priority
local PriorityTaskQuotaLimitPanel = graphPanel.new(
  title="CPU Quota Limit By Priority",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="µs",
  logBase1Y=1,
  description="The CPU quota limiter applied to each priority.",
).addTarget(
  prometheus.target(
    'tikv_resource_control_priority_quota_limit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance"}',
    legendFormat="{{instance}}-{{priority}}",
  )
);

// Task QPS that triggers wait
local PriorityTaskWaitQPSPanel = graphPanel.new(
  title="Tasks Wait QPS By Priority",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=1,
  description="Tasks number per second that triggers quota limiter wait",
).addTarget(
  prometheus.target(
    'sum(rate(tikv_resource_control_priority_wait_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tidb_instance"}[1m])) by (instance, priority)',
    legendFormat="{{instance}}-{{priority}}",
  )
);

// The task wait distribution by priority
local PriorityTaskWaitDurationPanel = graphPanel.new(
  title="Priority Task Wait Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  logBase1Y=2,
  description="The wait Duration of tasks that triggers quota limiter wait per priority",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tikv_resource_control_priority_wait_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance"}[1m])) by (instance, priority, le))',
    legendFormat="{{instance}}-{{priority}}-P99",
  )
).addTarget(
  prometheus.target(
    'sum(rate(tikv_resource_control_priority_wait_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance"}[1m])) by (instance, priority) / sum(rate(tikv_resource_control_priority_wait_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance"}[1m])) by (instance, priority)',
    legendFormat="{{instance}}-{{priority}}-avg",
  )
);


//*  ==============Panel (Background Task Control)==================
//*  Row Title: Background Task Control
//*  Description: The metrics about Background Task Control resource control
//*  Panels: 7
//*  ==============Panel (Background Task Control)==================

local backgroundTaskRow = row.new(collapse=true, title="Background Task Control");

// Background Tasks' RU
local BackgroundTaskRUPanel = graphPanel.new(
  title="Background Task RU",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=10,
  description="The total background task's request unit cost for all resource groups.",
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"background", resource_group=~"$resource_group"}[1m])) by (resource_group) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"background", resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(resource_manager_resource_unit_read_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"background"}[1m])) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type=~"background"}[1m]))',
    legendFormat="total",
  )
);

// Background Task Resource Utilization
local BackgroundTaskResourceUtilizationPanel = graphPanel.new(
  title="Background Task Resource Utilization",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="percent",
  logBase1Y=1,
  description="The resource(CPU, IO) utilization percentage and limit of background tasks.",
).addTarget(
  prometheus.target(
    'tikv_resource_control_bg_resource_utilization{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance"}',
    legendFormat="{{instance}}-{{type}}",
  )
);

// Background Task CPU Limit
local BackgroundTaskCPULimitPanel = graphPanel.new(
  title="Background Task CPU Limit",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="µs",
  logBase1Y=1,
  description="The total background task's cpu limit for all resource groups.",
).addTarget(
  prometheus.target(
    'tikv_resource_control_background_quota_limiter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance", resource_group=~"$resource_group", type="cpu"}',
    legendFormat="{{resource_group}}-{{instance}}",
  )
);

// Background Task IO Limit
local BackgroundTaskIOLimitPanel = graphPanel.new(
  title="Background Task IO Limit",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  logBase1Y=1,
  description="The total background task's io limit for all resource groups.",
).addTarget(
  prometheus.target(
    'tikv_resource_control_background_quota_limiter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance", resource_group=~"$resource_group", type="io"}',
    legendFormat="{{resource_group}}-{{instance}}",
  )
);

// Background Task CPU Consumption
local BackgroundTaskCPUConsumptionPanel = graphPanel.new(
  title="Background Task CPU Consumption",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="µs",
  logBase1Y=1,
  description="The total background task's cpu consumption for all resource groups.",
).addTarget(
  prometheus.target(
    'rate(tikv_resource_control_background_resource_consumption{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance", resource_group=~"$resource_group", type="cpu"}[1m])',
    legendFormat="{{resource_group}}-{{instance}}",
  )
);

// Background Task IO Consumption
local BackgroundTaskIOConsumptionPanel = graphPanel.new(
  title="Background Task IO Consumption",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="bytes",
  logBase1Y=1,
  description="The total background task's io consumption for all resource groups.",
).addTarget(
  prometheus.target(
    'rate(tikv_resource_control_background_resource_consumption{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance", resource_group=~"$resource_group", type="io"}[1m])',
    legendFormat="{{resource_group}}-{{instance}}",
  )
);

// Background Task Total Wait Duration
local BackgroundTaskTotalWaitDurationPanel = graphPanel.new(
  title="Background Task Total Wait Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="µs",
  logBase1Y=1,
  description="The total background task's wait duration for all resource groups.",
).addTarget(
  prometheus.target(
    'rate(tikv_resource_control_background_task_wait_duration{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tikv_instance", resource_group=~"$resource_group"}[1m])',
    legendFormat="{{resource_group}}-{{instance}}",
  )
);

//*  ==============Panel (Query Sumary)==================
//*  Row Title: Query Sumary
//*  Description: The metrics about query summary
//*  Panels: 5
//*  ==============Panel (Query Sumary)==================

local querySummaryRow = row.new(collapse=true, title="Query Summary");

// Query Duration
local QueryDurationPanel = graphPanel.new(
  title="Query Duration",
  datasource=myDS,
  legend_rightSide=true,
  legend_min=true,
  legend_max=true,
  legend_avg=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="s",
  logBase1Y=2,
  description="The Duration of sql execute for different resource group",
).addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (le,resource_group))',
    legendFormat="{{resource_group}}-P999",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (le,resource_group))',
    legendFormat="{{resource_group}}-P99",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (le,resource_group))',
    legendFormat="{{resource_group}}-P90",
  )
).addTarget(
  prometheus.target(
    'histogram_quantile(0.8, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (le,resource_group))',
    legendFormat="{{resource_group}}-P80",
  )
);

// Command per second
local CommandPerSecondPanel = graphPanel.new(
  title="Command Per Second",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=10,
  description="MySQL commands processing numbers per second. See https://dev.mysql.com/doc/internals/en/text-protocol.html and https://dev.mysql.com/doc/internals/en/prepared-statements.html",
).addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (result,resource_group)',
    legendFormat="{{resource_group}}-{{result}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",result="OK",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m] offset 1d)) by (result,resource_group)',
    legendFormat="{{resource_group}}-yesterday",
    hide=true,
  )
);

// QPS
local QPSPanel = graphPanel.new(
  title="QPS",
  datasource=myDS,
  legend_rightSide=true,
  legend_avg=true,
  legend_max=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=2,
  description="TiDB statement statistics",
).addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_statement_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (type,resource_group)',
    legendFormat="{{resource_group}}-{{type}}",
  )
).addTarget(
  prometheus.target(
    'sum(rate(tidb_executor_statement_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}[1m])) by (resource_group)',
    legendFormat="{{resource_group}}-total",
  )
);

// Connection Count
local ConnectionCountPanel = graphPanel.new(
  title="Connection Count",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=1,
  description="The number of connections to the TiDB server",
).addTarget(
  prometheus.target(
    'tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}',
    legendFormat="{{instance}}--{{resource_group}}",
  )
).addTarget(
  prometheus.target(
    'tidb_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster",instance=~"$tidb_instance",resource_group=~"$resource_group"}',
    legendFormat="{{resource_group}}--total",
  )
);

// Failed Query OPM
local FailedQueryOPMPanel = graphPanel.new(
  title="Failed Query OPM",
  datasource=myDS,
  legend_rightSide=true,
  legend_current=true,
  legend_max=true,
  legend_alignAsTable=true,
  legend_values=true,
  format="short",
  logBase1Y=2,
  description="The number of failed queries per minute",
).addTarget(
  prometheus.target(
    'sum(increase(tidb_server_execute_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance",resource_group=~"$resource_group"}[1m])) by (type, instance,resource_group)',
    legendFormat="{{type}}-{{instance}}-{{resource_group}}",
  )
);

//* ============== Dashboard ===============
//* Merge together
//* ============== Dashboard ===============

// Position definition
local panelW = 12;
local panelH = 7;
local rowW = 24;
local rowH = 1;

local rowPos = { x: 0, y: 0, w: rowW, h: rowH };
local leftPanelPos = { x: 0, y: 0, w: panelW, h: panelH };
local rightPanelPos = { x: panelW, y: 0, w: panelW, h: panelH };
local fullPanelPos = { x: 0, y: 0, w: rowW, h: panelH };

TiDBResourceControlDash
.addPanel(
  ruRow/* Resource Unit */
  .addPanel(ConfigPanel, gridPos=fullPanelPos)
  .addPanel(RUPanel, gridPos=leftPanelPos)
  .addPanel(RUMaxPanel, gridPos=rightPanelPos)
  .addPanel(RUPerQueryPanel, gridPos=leftPanelPos)
  .addPanel(RRUPanel, gridPos=rightPanelPos)
  .addPanel(RRUPerQueryPanel, gridPos=leftPanelPos)
  .addPanel(WRUPanel, gridPos=rightPanelPos)
  .addPanel(WRUPerQueryPanel, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  resourceRow/* Resource Details */
  .addPanel(KVRequestCountPanel, gridPos=leftPanelPos)
  .addPanel(KVRequestCountPerQueryPanel, gridPos=rightPanelPos)
  .addPanel(BytesReadPanel, gridPos=leftPanelPos)
  .addPanel(BytesReadPerQueryPanel, gridPos=rightPanelPos)
  .addPanel(BytesWrittenPanel, gridPos=leftPanelPos)
  .addPanel(BytesWrittenPerQueryPanel, gridPos=rightPanelPos)
  .addPanel(KVCPUTimePanel, gridPos=leftPanelPos)
  .addPanel(SQLCPUTimePanel, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  clientRow/* Client */
  .addPanel(ActiveResourceGroupPanel, gridPos=fullPanelPos)
  .addPanel(TotalKVRequestCountPanel, gridPos=leftPanelPos)
  .addPanel(FailedKVRequestCountPanel, gridPos=rightPanelPos)
  .addPanel(SuccessfulKVRequestWaitDurationPanel, gridPos=leftPanelPos)
  .addPanel(SuccessfulKVRequestCountPanel, gridPos=rightPanelPos)
  .addPanel(TokenRequestHandleDurationPanel, gridPos=leftPanelPos)
  .addPanel(TokenRequestCountPanel, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  runawayRow/* Runaway */
  .addPanel(QueryMaxDurationPanel, gridPos=leftPanelPos)
  .addPanel(RunawayEventPanel, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  priorityTaskRow/* Priority Task Control */
  .addPanel(PriorityTaskCPUPanel, gridPos=leftPanelPos)
  .addPanel(PriorityTaskQuotaLimitPanel, gridPos=rightPanelPos)
  .addPanel(PriorityTaskWaitQPSPanel, gridPos=leftPanelPos)
  .addPanel(PriorityTaskWaitDurationPanel, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  backgroundTaskRow/* Background Task Control */
  .addPanel(BackgroundTaskRUPanel, gridPos=leftPanelPos)
  .addPanel(BackgroundTaskResourceUtilizationPanel, gridPos=rightPanelPos)
  .addPanel(BackgroundTaskIOLimitPanel, gridPos=leftPanelPos)
  .addPanel(BackgroundTaskCPUConsumptionPanel, gridPos=rightPanelPos)
  .addPanel(BackgroundTaskCPULimitPanel, gridPos=leftPanelPos)
  .addPanel(BackgroundTaskIOConsumptionPanel, gridPos=rightPanelPos)
  .addPanel(BackgroundTaskTotalWaitDurationPanel, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
).addPanel(
  querySummaryRow/* Query Summary */
  .addPanel(QueryDurationPanel, gridPos=leftPanelPos)
  .addPanel(CommandPerSecondPanel, gridPos=rightPanelPos)
  .addPanel(QPSPanel, gridPos=leftPanelPos)
  .addPanel(ConnectionCountPanel, gridPos=rightPanelPos)
  .addPanel(FailedQueryOPMPanel, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
