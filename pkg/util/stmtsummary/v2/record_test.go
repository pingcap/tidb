// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"encoding/json"
	"testing"

	contextutil "github.com/pingcap/tidb/pkg/util/context"
	stmtsummaryv1 "github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/stretchr/testify/require"
)

func TestStmtRecord(t *testing.T) {
	info := GenerateStmtExecInfo4Test("digest1")
	record1 := NewStmtRecord(info)
	require.Equal(t, info.SchemaName, record1.SchemaName)
	require.Equal(t, info.Digest, record1.Digest)
	require.Equal(t, info.PlanDigest, record1.PlanDigest)
	require.Equal(t, info.StmtCtx.StmtType, record1.StmtType)
	require.Equal(t, info.NormalizedSQL, record1.NormalizedSQL)
	require.Equal(t, "db1.tb1,db2.tb2", record1.TableNames)
	require.Equal(t, info.IsInternal, record1.IsInternal)
	require.Equal(t, formatSQL(info.LazyInfo.GetOriginalSQL()), record1.SampleSQL)
	require.Equal(t, info.Charset, record1.Charset)
	require.Equal(t, info.Collation, record1.Collation)
	require.Equal(t, info.PrevSQL, record1.PrevSQL)
	require.Equal(t, info.StmtCtx.IndexNames, record1.IndexNames)
	require.Equal(t, info.TotalLatency, record1.MinLatency)
	require.Equal(t, info.Prepared, record1.Prepared)
	require.Equal(t, info.StartTime, record1.FirstSeen)
	require.Equal(t, info.StartTime, record1.LastSeen)
	require.Equal(t, info.KeyspaceName, record1.KeyspaceName)
	require.Equal(t, info.KeyspaceID, record1.KeyspaceID)
	require.Empty(t, record1.AuthUsers)
	require.Zero(t, record1.ExecCount)
	require.Zero(t, record1.SumLatency)
	require.Zero(t, record1.MaxLatency)
	require.Equal(t, info.ResourceGroupName, record1.ResourceGroupName)

	record1.Add(info)
	require.Len(t, record1.AuthUsers, 1)
	require.Contains(t, record1.AuthUsers, "user")
	require.Equal(t, int64(1), record1.ExecCount)
	require.Equal(t, info.TotalLatency, record1.SumLatency)
	require.Equal(t, info.TotalLatency, record1.MaxLatency)
	require.Equal(t, info.TotalLatency, record1.MinLatency)
	require.Equal(t, info.RUDetail.RRU(), record1.MaxRRU)
	require.Equal(t, info.RUDetail.RRU(), record1.SumRRU)
	require.Equal(t, info.RUDetail.WRU(), record1.MaxWRU)
	require.Equal(t, info.RUDetail.WRU(), record1.SumWRU)
	require.Equal(t, info.RUDetail.RUWaitDuration(), record1.MaxRUWaitDuration)
	require.Equal(t, info.RUDetail.RUWaitDuration(), record1.SumRUWaitDuration)
	require.Equal(t, info.CPUUsages.TidbCPUTime, record1.SumTidbCPU)
	require.Equal(t, info.CPUUsages.TikvCPUTime, record1.SumTikvCPU)

	record2 := NewStmtRecord(info)
	record2.Add(info)
	record2.Merge(record1)
	require.Len(t, record2.AuthUsers, 1)
	require.Contains(t, record2.AuthUsers, "user")
	require.Equal(t, int64(2), record2.ExecCount)
	require.Equal(t, info.TotalLatency*2, record2.SumLatency)
	require.Equal(t, info.TotalLatency, record2.MaxLatency)
	require.Equal(t, info.TotalLatency, record2.MinLatency)
	require.Equal(t, info.RUDetail.RRU()*2, record2.SumRRU)
	require.Equal(t, info.RUDetail.WRU()*2, record2.SumWRU)
	require.Equal(t, info.RUDetail.RUWaitDuration()*2, record2.SumRUWaitDuration)
	require.Equal(t, info.CPUUsages.TidbCPUTime*2, record2.SumTidbCPU)
	require.Equal(t, info.CPUUsages.TikvCPUTime*2, record2.SumTikvCPU)
}

func TestPlanCacheMissSummary(t *testing.T) {
	info := GenerateStmtExecInfo4Test("digest")
	info.PlanCacheFallbackReason = contextutil.PlanCacheFallbackLookupMiss
	info.PlanCacheFallbackTimeUnixNano = 200
	record := NewStmtRecord(info)
	record.Add(info)
	require.Equal(t, int64(1), record.PlanCacheMisses)
	require.Equal(t, "lookup_miss", record.PlanCacheMissLastReason)
	require.Equal(t, int64(200), record.PlanCacheMissLastSeen)
	topReason, topCount := record.PlanCacheFallbackCounts.Top()
	require.Equal(t, contextutil.PlanCacheFallbackLookupMiss, topReason)
	require.Equal(t, int64(1), topCount)

	info.PlanCacheFallbackReason = contextutil.PlanCacheFallbackReuseRejected
	info.PlanCacheFallbackTimeUnixNano = 100
	record.Add(info)
	require.Equal(t, int64(2), record.PlanCacheMisses)
	require.Equal(t, "lookup_miss", record.PlanCacheMissLastReason)

	other := NewStmtRecord(info)
	other.PlanCacheMisses = 3
	other.PlanCacheFallbackCounts = new(stmtsummaryv1.PlanCacheFallbackCounts)
	other.PlanCacheFallbackCounts.Add(contextutil.PlanCacheFallbackParameterTypeMismatch)
	other.PlanCacheFallbackCounts.Add(contextutil.PlanCacheFallbackParameterTypeMismatch)
	other.PlanCacheFallbackCounts.Add(contextutil.PlanCacheFallbackParameterTypeMismatch)
	other.PlanCacheMissLastReason = "parameter_type_mismatch"
	other.PlanCacheMissLastSeen = 200
	record.Merge(other)
	require.Equal(t, int64(5), record.PlanCacheMisses)
	require.Equal(t, "parameter_type_mismatch", record.PlanCacheMissLastReason)
	require.Equal(t, int64(200), record.PlanCacheMissLastSeen)
	topReason, topCount = record.PlanCacheFallbackCounts.Top()
	require.Equal(t, contextutil.PlanCacheFallbackParameterTypeMismatch, topReason)
	require.Equal(t, int64(3), topCount)

	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	var decoded StmtRecord
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, record.PlanCacheMisses, decoded.PlanCacheMisses)
	require.Equal(t, record.PlanCacheMissLastReason, decoded.PlanCacheMissLastReason)
	require.Equal(t, record.PlanCacheMissLastSeen, decoded.PlanCacheMissLastSeen)
	require.Equal(t, record.PlanCacheFallbackCounts, decoded.PlanCacheFallbackCounts)

	var oldRecord StmtRecord
	require.NoError(t, json.Unmarshal([]byte(`{"plan_cache_hits":1}`), &oldRecord))
	require.Zero(t, oldRecord.PlanCacheMisses)
	require.Empty(t, oldRecord.PlanCacheMissLastReason)
	require.Zero(t, oldRecord.PlanCacheMissLastSeen)
	require.Nil(t, oldRecord.PlanCacheFallbackCounts)
}
