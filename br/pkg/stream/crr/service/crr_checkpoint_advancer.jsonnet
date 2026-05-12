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
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local statPanel = grafana.statPanel;
local prometheus = grafana.prometheus;
local template = grafana.template;
local fieldConfig = grafana.fieldConfig;

local myNameFlag = 'DS_TEST-CLUSTER';
local myDS = '${' + myNameFlag + '}';

local MappingType = {
    Value: 1,
    Range: 2,
};

local newDash = dashboard.new(
    title='Test-Cluster-CRR-Checkpoint-Advancer',
    editable=true,
    graphTooltip='shared_crosschair',
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
        query='label_values(pd_cluster_status{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
        refresh='time',
        regex='',
        sort=1,
        tagValuesQuery='',
  )
)
.addTemplate(
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
local serverRow = row.new(collapse=true, title='Advancer');

local liveP = statPanel.new(
    title='Live Status',
    datasource=myDS,
    description='Whether CRR checkpoint advancer is live.',
) {
    options: {
        reduceOptions: {
            values: false,
            calcs: ['lastNotNull'],
            fields: '',
        },
        orientation: 'horizontal',
        textMode: 'value',
        colorMode: 'background',
        graphMode: 'none',
    },
    fieldConfig: {
        defaults: {
            noValue: 'UNKNOWN',
            color: { mode: 'thresholds' },
            thresholds: {
                mode: 'absolute',
                steps: [
                    { color: 'gray', value: null },
                    { color: 'red', value: 0 },
                    { color: 'green', value: 1 },
                ],
            },
            mappings: [
                { type: MappingType.Value, value: '1', text: 'LIVE' },
                { type: MappingType.Value, value: '0', text: 'OFFLINE' }
            ]
        }
    }
}
.addTarget(
    prometheus.target(
        'round(tidb_br_crr_service_live{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"})',
        legendFormat='live status'
    )
);

local readyP = statPanel.new(
    title='Ready Status',
    datasource=myDS,
    description='Whether CRR checkpoint advancer is ready.',
) {
    options: {
        reduceOptions: {
            values: false,
            calcs: ['lastNotNull'],
            fields: '',
        },
        orientation: 'horizontal',
        textMode: 'value',
        colorMode: 'background',
        graphMode: 'none',
    },
    fieldConfig: {
        defaults: {
            noValue: 'UNKNOWN',
            color: { mode: 'thresholds' },
            thresholds: {
                mode: 'absolute',
                steps: [
                    { color: 'gray', value: null },
                    { color: 'red', value: 0 },
                    { color: 'green', value: 1 },
                ],
            },
            mappings: [
                { type: MappingType.Value, value: '1', text: 'Ready' },
                { type: MappingType.Value, value: '0', text: 'NOT Ready' }
            ]
        }
    }
}
.addTarget(
    prometheus.target(
        'round(tidb_br_crr_service_ready{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"})',
        legendFormat='ready status'
    )
);

local roundP = statPanel.new(
    title='Current Round',
    datasource=myDS,
    description='Current CRR checkpoint advancer calculation round.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_current_round{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='current round'
    )
);

local aliveStoreCountP = statPanel.new(
    title='Alive Store Count',
    datasource=myDS,
    description='Current alive store count.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_alive_store_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='alive store count'
    )
);

local stateP = graphPanel.new(
    title='Advancer Service State',
    datasource=myDS,
    description='The history of CRR advancer service state.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_service_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{state}}-{{task}}'
    )
);

local phaseP = graphPanel.new(
    title='Advancer Service Phase',
    datasource=myDS,
    description='The history of CRR advancer service phase.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_service_phase{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{phase}}-{{task}}'
    )
);

local syncedtsP = graphPanel.new(
    title='Synced TS',
    datasource=myDS,
    legend_rightSide=true,
    description='Current synced ts(minimum synced flush ts across all alive stores) of CRR service adancer.',
    format='dateTimeAsIso',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_synced_ts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"} / 262144',
        legendFormat='{{task}}'
    )
);

local checkpointGapP = graphPanel.new(
    title='Global Log Backup Checkpoint Gap',
    datasource=myDS,
    legend_rightSide=true,
    description='Log backup current gap between upstream and downstream global checkpoint.',
    format='ms',
)
.addTarget(
    prometheus.target(
        '(tidb_br_crr_last_upstream_checkpoint{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"} - tidb_br_crr_safe_checkpoint{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}) / 262144',
        legendFormat='upstream/downstream-gap-{{task}}'
    )
)
.addTarget(
    prometheus.target(
        'time() * 1000 - tidb_br_crr_safe_checkpoint{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"} / 262144',
        legendFormat='downstream-rpo-{{task}}'
    )
);

local checkpointP = graphPanel.new(
    title='Global Log Backup Checkpoint',
    datasource=myDS,
    legend_rightSide=true,
    description='Log backup current upstream/downstream global checkpoint.',
    format='dateTimeAsIso',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_last_upstream_checkpoint{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"} / 262144',
        legendFormat='upstream-{{task}}'
    )
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_safe_checkpoint{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"} / 262144',
        legendFormat='downstream-{{task}}'
    )
);

local loopIterationP = graphPanel.new(
    title='Loop iteration',
    datasource=myDS,
    legend_rightSide=true,
    description='Loop iteration to wait downstream files synced in each phase.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_last_loop_iteration{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

local pendingFileCountP = graphPanel.new(
    title='Pending File Count',
    datasource=myDS,
    legend_rightSide=true,
    description='Pending file count in each round.',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_pending_file_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

local consecutiveFailuresP = graphPanel.new(
    title='Consecutive Failures',
    datasource=myDS,
    legend_rightSide=true,
    description='Consecutive failure count since last success time',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_consecutive_failures{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

local upstreamReadMetaFileCountP = graphPanel.new(
    title='Upstream Read Meta File Count',
    datasource=myDS,
    legend_rightSide=true,
    description='Read upstream meta file count in each round',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_upstream_read_meta_file_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

local estimatedSyncLogFileCountP = graphPanel.new(
    title='Estimated Sync Log File Count',
    datasource=myDS,
    legend_rightSide=true,
    description='Estimated sync log file count in each round',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_estimated_sync_log_file_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

local downstreamCheckFileCountP = graphPanel.new(
    title='Estimated Sync Log File Count',
    datasource=myDS,
    legend_rightSide=true,
    description='Estimated sync log file count in each round',
)
.addTarget(
    prometheus.target(
        'tidb_br_crr_downstream_check_file_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
        legendFormat='{{task}}'
    )
);

// Merge together.
local panelW = 12;
local panelLHalfW = 6;
local panelRHalfW = 18;
local panelLQW = 8;
local panelMQW = 16;
local panelH = 7;
local panelHalfH = 4;
local panelQHalfH = 11;
local rowW = 24;
local rowH = 1;

local rowPos = {x:0, y:0, w:rowW, h:rowH};
local leftLUPanelPos = {x:0, y:0, w:panelLHalfW, h:panelHalfH};
local leftRUPanelPos = {x:panelLHalfW, y:0, w:panelLHalfW, h:panelHalfH};
local rightLUPanelPos = {x:panelW, y:0, w:panelLHalfW, h:panelHalfH};
local rightRUPanalPos = {x:panelRHalfW, y:0, w:panelLHalfW, h:panelHalfH};
local leftQPanelPos = {x:0, y:panelHalfH, w:panelLQW, h:panelH};
local middleQPanelPos = {x:panelLQW, y:panelHalfH, w:panelLQW, h:panelH};
local rightQPanelPos = {x:panelMQW, y:panelHalfH, w:panelLQW, h:panelH};
local leftPanelPos = {x:0, y:panelQHalfH, w:panelW, h:panelH};
local rightPanelPos = {x:panelW, y:panelQHalfH, w:panelW, h:panelH};

newDash
.addPanel(
    serverRow
    .addPanel(liveP, gridPos=leftLUPanelPos)
    .addPanel(readyP, gridPos=leftRUPanelPos)
    .addPanel(roundP, gridPos=rightLUPanelPos)
    .addPanel(aliveStoreCountP, gridPos=rightRUPanalPos)
    .addPanel(syncedtsP, gridPos=leftQPanelPos)
    .addPanel(checkpointGapP, gridPos=middleQPanelPos)
    .addPanel(checkpointP, gridPos=rightQPanelPos)
    .addPanel(stateP, gridPos=leftPanelPos)
    .addPanel(phaseP, gridPos=rightPanelPos)
    .addPanel(loopIterationP, gridPos=leftPanelPos)
    .addPanel(pendingFileCountP, gridPos=rightPanelPos)
    .addPanel(consecutiveFailuresP, gridPos=leftPanelPos)
    .addPanel(upstreamReadMetaFileCountP, gridPos=rightPanelPos)
    .addPanel(estimatedSyncLogFileCountP, gridPos=leftPanelPos)
    .addPanel(downstreamCheckFileCountP, gridPos=rightPanelPos),
    gridPos=rowPos
)
