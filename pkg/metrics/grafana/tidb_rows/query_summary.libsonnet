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
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;

local myNameFlag = 'DS_TEST-CLUSTER';
local myDS = '${' + myNameFlag + '}';

local panelPos(x, y) = {w: 12, h: 7, x: x, y: y};

local yAxes(leftFormat, rightFormat, leftLogBase=1, rightLogBase=1, leftMin=null) = [
  {
    format: leftFormat,
    label: null,
    logBase: leftLogBase,
    max: null,
    min: leftMin,
    show: true,
  },
  {
    format: rightFormat,
    label: null,
    logBase: rightLogBase,
    max: null,
    min: null,
    show: true,
  },
];

local withDefaults(panel, id) = panel + {
  id: id,
  pluginVersion: '7.5.11',
};

local durationP = withDefaults(
  graphPanel.new(
    title='Duration',
    datasource=myDS,
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
  ),
  80,
) + {
  yaxes: yAxes('s', 'short', 2, 1),
};

local commandPerSecondP = withDefaults(
  graphPanel.new(
    title='Command Per Second',
    datasource=myDS,
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
  ),
  42,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('short', 'short', 1, 1, '0'),
};

local qpsP = withDefaults(
  graphPanel.new(
    title='QPS',
    datasource=myDS,
    description='TiDB statement statistics',
    legend_rightSide=true,
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
  ),
  21,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('short', 'short', 2, 1, '0'),
};

local cpsByInstanceP = withDefaults(
  graphPanel.new(
    title='CPS By Instance',
    datasource=myDS,
    description='TiDB command total statistics including both successful and failed ones',
    legend_rightSide=true,
  )
  .addTarget(
    prometheus.target(
      'rate(tidb_server_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])',
      legendFormat='{{instance}} {{type}} {{result}}',
    )
  ),
  2,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('short', 'short', 1, 1, '0'),
};

local failedQueryP = withDefaults(
  graphPanel.new(
    title='Failed Query OPM',
    datasource=myDS,
    description='TiDB failed query statistics by query type',
    legend_rightSide=true,
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tidb_server_execute_error_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (type, instance)',
      legendFormat=' {{type}}-{{instance}}',
    )
  ),
  137,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('short', 'short', 2, 1, '0'),
};

local affectedRowsP = withDefaults(
  graphPanel.new(
    title='Affected Rows By Type',
    datasource=myDS,
    description='Affected rows of DMLs (INSERT/UPDATE/DELETE/REPLACE) per second. It could present the written rows/s for TiDB instances.',
    legend_rightSide=true,
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
  ),
  267,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('short', 'short', 1, 1, '0'),
};

local slowQueryP = withDefaults(
  graphPanel.new(
    title='Slow Query',
    datasource=myDS,
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
  ),
  112,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('s', 'short', 2, 1),
};

local connectionIdleDurationP = withDefaults(
  graphPanel.new(
    title='Connection Idle Duration',
    datasource=myDS,
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
  ),
  218,
) + {
  nullPointMode: 'null as zero',
  yaxes: yAxes('s', 'short', 2, 1),
};

local duration999P = withDefaults(
  graphPanel.new(
    title='999 Duration',
    datasource=myDS,
    description='TiDB durations for different query types with 99.9 percent buckets',
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.999, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
      legendFormat='{{sql_type}}',
    )
  ),
  136,
) + {
  yaxes: yAxes('s', 'short', 2, 1),
};

local duration99P = withDefaults(
  graphPanel.new(
    title='99 Duration',
    datasource=myDS,
    description='TiDB durations for different query types with 99 percent buckets',
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
      legendFormat='{{sql_type}}',
    )
  ),
  134,
) + {
  yaxes: yAxes('s', 'short', 2, 1),
};

local duration95P = withDefaults(
  graphPanel.new(
    title='95 Duration',
    datasource=myDS,
    description='TiDB durations for different query types with 95 percent buckets',
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.95, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
      legendFormat='{{sql_type}}',
    )
  ),
  132,
) + {
  yaxes: yAxes('s', 'short', 2, 1),
};

local duration80P = withDefaults(
  graphPanel.new(
    title='80 Duration',
    datasource=myDS,
    description='TiDB durations for different query types with 80 percent buckets',
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.80, sum(rate(tidb_server_handle_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le,sql_type))',
      legendFormat='{{sql_type}}',
    )
  ),
  130,
) + {
  yaxes: yAxes('s', 'short', 2, 1),
};

(
  row.new(title='Query Summary', collapse=true)
  .addPanel(durationP, gridPos=panelPos(0, 1))
  .addPanel(commandPerSecondP, gridPos=panelPos(12, 1))
  .addPanel(qpsP, gridPos=panelPos(0, 7))
  .addPanel(cpsByInstanceP, gridPos=panelPos(12, 7))
  .addPanel(failedQueryP, gridPos=panelPos(0, 13))
  .addPanel(affectedRowsP, gridPos=panelPos(12, 13))
  .addPanel(slowQueryP, gridPos=panelPos(0, 19))
  .addPanel(connectionIdleDurationP, gridPos=panelPos(12, 19))
  .addPanel(duration999P, gridPos=panelPos(0, 25))
  .addPanel(duration99P, gridPos=panelPos(12, 25))
  .addPanel(duration95P, gridPos=panelPos(0, 31))
  .addPanel(duration80P, gridPos=panelPos(12, 31))
) + {
  id: 138,
}
