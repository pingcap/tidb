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

// RUv3 metrics.
var (
	RUV3Total        prometheus.Counter
	RUV3BySQLType    *prometheus.CounterVec
	RUV3BySQLTypeDDL prometheus.Counter
	RUV3ByEngine     *prometheus.CounterVec
	RUV3ByEngineTiKV prometheus.Counter
	RUV3Unit         *prometheus.CounterVec
	RUV3Statements   *prometheus.CounterVec
	ruv3TiDB         prometheus.Counter
	ruv3Select       prometheus.Counter
	ruv3Insert       prometheus.Counter
	ruv3Replace      prometheus.Counter
	ruv3Update       prometheus.Counter
	ruv3Delete       prometheus.Counter
	ruv3Commit       prometheus.Counter
	ruv3Analyze      prometheus.Counter
	ruv3Other        prometheus.Counter
	RUV3TTLTotal     prometheus.Counter
)

// RUV3 unit label constants define the label name and values for RU v3 raw unit metrics.
const (
	LblRUV3Unit = "unit"

	LblRUV3UnitCPUWork              = "cpu_work"
	LblRUV3UnitScanBytes            = "scan_bytes"
	LblRUV3UnitNetBytes             = "net_bytes"
	LblRUV3UnitFrontendCompileBytes = "frontend_compile_bytes"
	LblRUV3UnitHashStateRows        = "hash_state_rows"
	LblRUV3UnitJoinOutputRows       = "join_output_rows"
	LblRUV3UnitWriteStatement       = "write_statement"
	LblRUV3UnitOperatorNum          = "operator_num"
	LblRUV3UnitWriteKeys            = "write_keys"
	LblRUV3UnitWriteBytes           = "write_bytes"
)

// InitRUV3Metrics initializes RUv3 metrics.
func InitRUV3Metrics() {
	RUV3TTLTotal = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "ttl_ru_total",
			Help:      "Counter of RU v3 consumption attributable to TTL jobs, included in ru_total. Excludes global TTL maintenance.",
		},
	)
	RUV3Total = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "ru_total",
			Help:      "Counter of resource unit consumption for RU v3.",
		},
	)

	RUV3BySQLType = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "ru_by_sql_type_total",
			Help:      "Counter of resource unit consumption by SQL type for RU v3.",
		}, []string{LblSQLType},
	)
	RUV3BySQLTypeDDL = RUV3BySQLType.WithLabelValues(LblSQLTypeDDL)

	ruv3Select = RUV3BySQLType.WithLabelValues("select")
	ruv3Insert = RUV3BySQLType.WithLabelValues("insert")
	ruv3Replace = RUV3BySQLType.WithLabelValues("replace")
	ruv3Update = RUV3BySQLType.WithLabelValues("update")
	ruv3Delete = RUV3BySQLType.WithLabelValues("delete")
	ruv3Commit = RUV3BySQLType.WithLabelValues("commit")
	ruv3Analyze = RUV3BySQLType.WithLabelValues("analyze")
	ruv3Other = RUV3BySQLType.WithLabelValues("other")

	RUV3ByEngine = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "ru_by_engine_total",
			Help:      "Counter of resource unit consumption by engine for RU v3.",
		}, []string{LblEngine},
	)
	ruv3TiDB = RUV3ByEngine.WithLabelValues("tidb")
	RUV3ByEngineTiKV = RUV3ByEngine.WithLabelValues(LblEngineTiKV)

	RUV3Unit = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "unit_total",
			Help:      "Counter of raw statement units for RU v3.",
		}, []string{LblEngine, "opclass", LblRUV3Unit},
	)
	RUV3Statements = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv3",
			Name:      "statements_total",
			Help:      "Counter of RU v3 calculation outcomes in full report mode; success with incomplete evidence remains best effort.",
		}, []string{"status", "reason"},
	)
}

// AddRUV3Results records total, SQL-type and supported engine results without
// a label lookup on the statement hot path. TiFlash has no RU v3 model yet.
func AddRUV3Results(tikvRU, tidbRU, totalRU float64, sqlType string) {
	counter := ruv3Other
	switch sqlType {
	case "select":
		counter = ruv3Select
	case "insert":
		counter = ruv3Insert
	case "replace":
		counter = ruv3Replace
	case "update":
		counter = ruv3Update
	case "delete":
		counter = ruv3Delete
	case "commit":
		counter = ruv3Commit
	case "analyze":
		counter = ruv3Analyze
	}
	RUV3Total.Add(totalRU)
	counter.Add(totalRU)
	RUV3ByEngineTiKV.Add(tikvRU)
	ruv3TiDB.Add(tidbRU)
}
