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

package metrics

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// RUv2 metrics.
var (
	RUV2Total        prometheus.Counter
	RUV2BySQLType    *prometheus.CounterVec
	RUV2BySQLTypeDDL prometheus.Counter
	RUV2ByEngine     *prometheus.CounterVec
	RUV2ByEngineTiKV prometheus.Counter
	RUV2Unit         *prometheus.CounterVec
	RUV2Statements   *prometheus.CounterVec
	ruv2TiDB         prometheus.Counter
	ruv2TiFlash      prometheus.Counter
	ruv2Select       prometheus.Counter
	ruv2Insert       prometheus.Counter
	ruv2Replace      prometheus.Counter
	ruv2Update       prometheus.Counter
	ruv2Delete       prometheus.Counter
	ruv2Commit       prometheus.Counter
	ruv2Analyze      prometheus.Counter
	ruv2Other        prometheus.Counter
	RUV2TTLTotal     prometheus.Counter
)

// RUV2 unit label constants define the label name and values for RU v2 raw unit metrics.
const (
	LblRUV2Unit = "unit"

	LblRUV2UnitCPUWork              = "cpu_work"
	LblRUV2UnitScanBytes            = "scan_bytes"
	LblRUV2UnitNetBytes             = "net_bytes"
	LblRUV2UnitCrossAZNetBytes      = "cross_az_net_bytes"
	LblRUV2UnitFrontendCompileBytes = "frontend_compile_bytes"
	LblRUV2UnitHashStateRows        = "hash_state_rows"
	LblRUV2UnitJoinOutputRows       = "join_output_rows"
	LblRUV2UnitWriteStatement       = "write_statement"
	LblRUV2UnitOperatorNum          = "operator_num"
	LblRUV2UnitWriteKeys            = "write_keys"
	LblRUV2UnitWriteBytes           = "write_bytes"
)

// InitRUV2Metrics initializes RUv2 metrics.
func InitRUV2Metrics() {
	RUV2TTLTotal = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "ttl_ru_total",
			Help: "Counter of RU v2 consumption from TTL user-table scans and deletes, including their commits; " +
				"included in ru_total.",
		},
	)
	RUV2Total = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "ru_total",
			Help:      "Counter of resource unit consumption for RU v2.",
		},
	)

	RUV2BySQLType = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "ru_by_sql_type_total",
			Help:      "Counter of resource unit consumption by SQL type for RU v2.",
		}, []string{LblSQLType},
	)
	RUV2BySQLTypeDDL = RUV2BySQLType.WithLabelValues(LblSQLTypeDDL)

	ruv2Select = RUV2BySQLType.WithLabelValues("select")
	ruv2Insert = RUV2BySQLType.WithLabelValues("insert")
	ruv2Replace = RUV2BySQLType.WithLabelValues("replace")
	ruv2Update = RUV2BySQLType.WithLabelValues("update")
	ruv2Delete = RUV2BySQLType.WithLabelValues("delete")
	ruv2Commit = RUV2BySQLType.WithLabelValues("commit")
	ruv2Analyze = RUV2BySQLType.WithLabelValues("analyze")
	ruv2Other = RUV2BySQLType.WithLabelValues("other")

	RUV2ByEngine = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "ru_by_engine_total",
			Help:      "Counter of resource unit consumption by engine for RU v2.",
		}, []string{LblEngine},
	)
	ruv2TiDB = RUV2ByEngine.WithLabelValues("tidb")
	ruv2TiFlash = RUV2ByEngine.WithLabelValues(LblEngineTiFlash)
	RUV2ByEngineTiKV = RUV2ByEngine.WithLabelValues(LblEngineTiKV)

	RUV2Unit = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "unit_total",
			Help:      "Counter of raw statement units for RU v2.",
		}, []string{LblEngine, "opclass", LblRUV2Unit},
	)
	RUV2Statements = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "statements_total",
			Help:      "Counter of RU v2 calculation outcomes in full report mode; success with incomplete evidence remains best effort.",
		}, []string{"status", "reason"},
	)
}

// AddRUV2Results records total, SQL-type and supported engine results without
// a label lookup on the statement hot path.
func AddRUV2Results(tikvRU, tidbRU, tiflashRU, totalRU float64, sqlType string) {
	counter := ruv2Other
	switch sqlType {
	case "select":
		counter = ruv2Select
	case "insert":
		counter = ruv2Insert
	case "replace":
		counter = ruv2Replace
	case "update":
		counter = ruv2Update
	case "delete":
		counter = ruv2Delete
	case "commit":
		counter = ruv2Commit
	case "analyze":
		counter = ruv2Analyze
	}
	RUV2Total.Add(totalRU)
	counter.Add(totalRU)
	RUV2ByEngineTiKV.Add(tikvRU)
	ruv2TiDB.Add(tidbRU)
	ruv2TiFlash.Add(tiflashRU)
}
