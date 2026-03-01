// Copyright 2026 PingCAP, Inc.
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
local template = grafana.template;

local myNameFlag = 'DS_TEST-CLUSTER';
local myDS = '${' + myNameFlag + '}';

local querySummaryRow = import 'tidb_rows/query_summary.libsonnet';

local annotations = {
  list: [
    {
      builtIn: 1,
      datasource: myDS,
      enable: true,
      hide: true,
      iconColor: 'rgba(0, 211, 255, 1)',
      name: 'Annotations & Alerts',
      type: 'dashboard',
    },
  ],
};

local requires = [
  {
    type: 'panel',
    id: 'bargauge',
    name: 'Bar gauge',
    version: '',
  },
  {
    type: 'grafana',
    id: 'grafana',
    name: 'Grafana',
    version: '7.5.11',
  },
  {
    type: 'panel',
    id: 'graph',
    name: 'Graph',
    version: '',
  },
  {
    type: 'panel',
    id: 'heatmap',
    name: 'Heatmap',
    version: '',
  },
  {
    type: 'panel',
    id: 'piechart',
    name: 'Pie chart v2',
    version: '',
  },
  {
    type: 'datasource',
    id: 'prometheus',
    name: 'Prometheus',
    version: '1.0.0',
  },
  {
    type: 'panel',
    id: 'table',
    name: 'Table',
    version: '',
  },
];

local timepicker = {
  refresh_intervals: ['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
  time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
};

local baseDash = dashboard.new(
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
)
.addTemplate(
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

local tidbDash = baseDash + {
  annotations: annotations,
  __requires: requires,
  schemaVersion: 27,
  style: 'dark',
  timezone: 'browser',
  uid: '000000011',
  version: 1,
  timepicker: timepicker,
  tags: [],
  iteration: 1699393280088,
  gnetId: null,
  id: null,
};

tidbDash.addPanel(
  querySummaryRow,
  gridPos={h: 1, w: 24, x: 0, y: 0},
)
