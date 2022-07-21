// Copyright 2021 PingCAP, Inc.
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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Metrics
var (
	TelemetrySQLCTECnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "non_recursive_cte_usage",
			Help:      "Counter of usage of CTE",
		}, []string{LblCTEType})
	TelemetryMultiSchemaChangeCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "multi_schema_change_usage",
			Help:      "Counter of usage of multi-schema change",
		})
)

// readCounter reads the value of a prometheus.Counter.
// Returns -1 when failing to read the value.
func readCounter(m prometheus.Counter) int64 {
	// Actually, it's not recommended to read the value of prometheus metric types directly:
	// https://github.com/prometheus/client_golang/issues/486#issuecomment-433345239
	pb := &dto.Metric{}
	// It's impossible to return an error though.
	if err := m.Write(pb); err != nil {
		return -1
	}
	return int64(pb.GetCounter().GetValue())
}

// CTEUsageCounter records the usages of CTE.
type CTEUsageCounter struct {
	NonRecursiveCTEUsed int64 `json:"nonRecursiveCTEUsed"`
	RecursiveUsed       int64 `json:"recursiveUsed"`
	NonCTEUsed          int64 `json:"nonCTEUsed"`
}

// Sub returns the difference of two counters.
func (c CTEUsageCounter) Sub(rhs CTEUsageCounter) CTEUsageCounter {
	return CTEUsageCounter{
		NonRecursiveCTEUsed: c.NonRecursiveCTEUsed - rhs.NonRecursiveCTEUsed,
		RecursiveUsed:       c.RecursiveUsed - rhs.RecursiveUsed,
		NonCTEUsed:          c.NonCTEUsed - rhs.NonCTEUsed,
	}
}

// GetCTECounter gets the TxnCommitCounter.
func GetCTECounter() CTEUsageCounter {
	return CTEUsageCounter{
		NonRecursiveCTEUsed: readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "nonRecurCTE"})),
		RecursiveUsed:       readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "recurCTE"})),
		NonCTEUsed:          readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "notCTE"})),
	}
}

// MultiSchemaChangeUsageCounter records the usages of multi-schema change.
type MultiSchemaChangeUsageCounter struct {
	MultiSchemaChangeUsed int64 `json:"multi_schema_change_used"`
}

// Sub returns the difference of two counters.
func (c MultiSchemaChangeUsageCounter) Sub(rhs MultiSchemaChangeUsageCounter) MultiSchemaChangeUsageCounter {
	return MultiSchemaChangeUsageCounter{
		MultiSchemaChangeUsed: c.MultiSchemaChangeUsed - rhs.MultiSchemaChangeUsed,
	}
}

// GetMultiSchemaCounter gets the TxnCommitCounter.
func GetMultiSchemaCounter() MultiSchemaChangeUsageCounter {
	return MultiSchemaChangeUsageCounter{
		MultiSchemaChangeUsed: readCounter(TelemetryMultiSchemaChangeCnt),
	}
}

// NonTransactionalStmtCounter records the usages of non-transactional statements.
type NonTransactionalStmtCounter struct {
	DeleteCount int64 `json:"delete"`
}

// Sub returns the difference of two counters.
func (n NonTransactionalStmtCounter) Sub(rhs NonTransactionalStmtCounter) NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: n.DeleteCount - rhs.DeleteCount,
	}
}

// GetNonTransactionalStmtCounter gets the NonTransactionalStmtCounter.
func GetNonTransactionalStmtCounter() NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: readCounter(NonTransactionalDeleteCount),
	}
}

// GetSavepointStmtCounter gets the savepoint statement executed counter.
func GetSavepointStmtCounter() int64 {
	return readCounter(StmtNodeCounter.With(prometheus.Labels{LblType: "Savepoint"}))
}
