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

package tracing

// CETraceRecord records an expression and related cardinality estimation result.
type CETraceRecord struct {
	TableName string `json:"table_name"`
	Type      string `json:"type"`
	Expr      string `json:"expr"`
	TableID   int64  `json:"-"`
	RowCount  uint64 `json:"row_count"`
}

// DedupCETrace deduplicate a slice of *CETraceRecord and return the deduplicated slice
func DedupCETrace(records []*CETraceRecord) []*CETraceRecord {
	ret := make([]*CETraceRecord, 0, len(records))
	exists := make(map[CETraceRecord]struct{}, len(records))
	for _, rec := range records {
		if _, ok := exists[*rec]; !ok {
			ret = append(ret, rec)
			exists[*rec] = struct{}{}
		}
	}
	return ret
}

// OptimizeTracer indicates tracer for optimizer
type OptimizeTracer struct {
}
