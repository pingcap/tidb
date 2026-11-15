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

package executor_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func decodeSPFreshSearchRequestForTest(t *testing.T, body []byte) *tipb.SPFreshSearchRequest {
	t.Helper()

	reqPB := &tipb.SPFreshSearchRequest{}
	require.NoError(t, proto.Unmarshal(body, reqPB))
	return reqPB
}

func collectSPFreshFilterTimeConstTypes(expr *tipb.Expr, counts map[int32]int) {
	if expr == nil {
		return
	}
	if expr.GetTp() == tipb.ExprType_MysqlTime {
		counts[expr.GetFieldType().GetTp()]++
	}
	for _, child := range expr.GetChildren() {
		collectSPFreshFilterTimeConstTypes(child, counts)
	}
}

func requireSPFreshTemporalFilterRequest(t *testing.T, reqPB *tipb.SPFreshSearchRequest, tp byte) {
	t.Helper()

	require.NotNil(t, reqPB)
	require.NotNil(t, reqPB.FilterExpr)
	require.Len(t, reqPB.FilterExprColumns, 1)
	require.Equal(t, int64(2), reqPB.FilterExprColumns[0].GetColumnId())
	require.Equal(t, int32(tp), reqPB.FilterExprColumns[0].GetFieldType().GetTp())

	timeConstTypes := make(map[int32]int)
	collectSPFreshFilterTimeConstTypes(reqPB.FilterExpr, timeConstTypes)
	require.Equalf(t, 2, timeConstTypes[int32(tp)], "time const counts=%v expr=%s", timeConstTypes, reqPB.FilterExpr)
}

func TestSPFreshVectorSearchExec_RequestAndDecode(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]
	var gotClusterID atomic.Value // string

	// Distances based on vec_l2_distance(vec, '[1,2,3]').
	rows := []struct {
		handle int64
		vec    string
		dist   float32
	}{
		{handle: 1, vec: `[1,2,3]`, dist: 0},
		{handle: 2, vec: `[1,2,4]`, dist: 1},
		{handle: 3, vec: `[1,2,5]`, dist: 2},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		// This test server is only for /spfresh/search; ignore other DDL-related requests if any.
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotClusterID.Store(r.URL.Query().Get("cluster_id"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		gotReq.Store(decodeSPFreshSearchRequestForTest(t, body))

		result := &tipb.SPFreshSearchResult{}
		for _, row := range rows {
			vec, err := types.ParseVectorFloat32(row.vec)
			require.NoError(t, err)
			vecBytes, err := codec.EncodeValue(nil, nil, types.NewVectorFloat32Datum(vec))
			require.NoError(t, err)

			var colVals [][]byte
			for _, col := range gotReq.Load().RequiredColumns {
				switch {
				case len(col.GetDefaultVal()) > 0:
					colVals = append(colVals, col.GetDefaultVal())
				case col.GetFlag()&int32(mysql.NotNullFlag) == 0:
					nullBytes, err := codec.EncodeValue(nil, nil, types.Datum{})
					require.NoError(t, err)
					colVals = append(colVals, nullBytes)
				case col.GetTp() == int32(mysql.TypeTiDBVectorFloat32):
					colVals = append(colVals, vecBytes)
				default:
					t.Fatalf("required column id=%d tp=%d has neither a default nor nullable metadata", col.GetColumnId(), col.GetTp())
				}
			}

			result.Rows = append(result.Rows, &tipb.SPFreshSearchRow{
				HandleBytes:  kv.IntHandle(row.handle).Encoded(),
				Distance:     row.dist,
				ColumnValues: colVals,
			})
		}

		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(result))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_exec")
	tk.MustExec("create table t_spfresh_exec(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_exec(id, vec) values (1,'[1,2,3]'), (2,'[1,2,4]'), (3,'[1,2,5]')")
	tk.MustExec("alter table t_spfresh_exec add column a int default 42, add column b int null")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_exec, idx_vec) */ id, a, b from t_spfresh_exec order by vec_l2_distance(vec, '[1,2,3]') limit 1 offset 2").
		Check(testkit.Rows("3 42 <nil>"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)

	// LIMIT 1 OFFSET 2 => TopK = 3
	require.Equal(t, uint64(3), reqPB.TopK)
	require.Zero(t, reqPB.OversampleFactor)
	require.Equal(t, uint64(64<<20), reqPB.MaxResponseBytes)
	require.NotZero(t, reqPB.StartTs)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.VectorSearchIsANNQuery)
	require.Equal(t, uint64(3), tk.Session().GetSessionVars().StmtCtx.VectorSearchTopK)
	require.False(t, reqPB.ReadOnly)

	clusterIDStr, _ := gotClusterID.Load().(string)
	require.NotEmpty(t, clusterIDStr)

	// Query vector bytes should be serialized without dims prefix.
	qv, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)
	expected := qv.ZeroCopySerialize()[4:]
	require.True(t, bytes.Equal(expected, reqPB.QueryVectorF32Le), fmt.Sprintf("query_vector_f32_le mismatch: got=%v want=%v", reqPB.QueryVectorF32Le, expected))

	// id (PK handle) should be skipped from required_columns.
	// vec should also be skipped after distance projection rewrite.
	require.Len(t, reqPB.RequiredColumns, 2)
	require.Equal(t, int32(mysql.TypeLong), reqPB.RequiredColumns[0].Tp)
	expectedDefault, err := codec.EncodeValue(nil, nil, types.NewIntDatum(42))
	require.NoError(t, err)
	require.Equal(t, expectedDefault, reqPB.RequiredColumns[0].GetDefaultVal())
	require.Empty(t, reqPB.RequiredColumns[1].GetDefaultVal())
	require.Zero(t, reqPB.RequiredColumns[1].GetFlag()&int32(mysql.NotNullFlag))
	require.Equal(t, uint32(0), reqPB.SearchBeamSize)
	require.False(t, reqPB.NeedDistance)

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_exec, idx_vec) */ id, vec_l2_distance(vec, '[1,2,3]') from t_spfresh_exec order by vec_l2_distance(vec, '[1,2,3]') limit 1 offset 2").
		Check(testkit.Rows("3 2"))
	reqPB = gotReq.Load()
	require.NotNil(t, reqPB)
	require.True(t, reqPB.NeedDistance)

	tk.MustExec(`alter table t_spfresh_exec alter index idx_vec set vector_index_param "read_only=true"`)
	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_exec, idx_vec) */ id, a from t_spfresh_exec order by vec_l2_distance(vec, '[1,2,3]') limit 1 offset 2").
		Check(testkit.Rows("3 42"))
	reqPB = gotReq.Load()
	require.NotNil(t, reqPB)
	require.True(t, reqPB.ReadOnly)
	require.Equal(t, uint32(0), reqPB.SearchBeamSize)
	require.Zero(t, reqPB.OversampleFactor)

	tk.MustExec(`alter table t_spfresh_exec alter index idx_vec set vector_index_param "search_beam_size=123,oversample_factor=1.5,read_only=false"`)
	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_exec, idx_vec) */ id, a from t_spfresh_exec order by vec_l2_distance(vec, '[1,2,3]') limit 1 offset 2").
		Check(testkit.Rows("3 42"))
	reqPB = gotReq.Load()
	require.NotNil(t, reqPB)
	require.False(t, reqPB.ReadOnly)
	require.Equal(t, uint32(123), reqPB.SearchBeamSize)
	require.Equal(t, 1.5, reqPB.OversampleFactor)
}

func TestSPFreshVectorSearchExec_RequestIncludesFilterExpr(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		gotReq.Store(decodeSPFreshSearchRequestForTest(t, body))

		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{},
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = 'Asia/Shanghai'")
	tk.MustExec("set @@div_precision_increment = 7")
	tk.MustExec("set @@sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_UNSIGNED_SUBTRACTION,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ANSI_QUOTES'")
	tk.Session().GetSessionVars().MaxAllowedPacket = 4096
	tk.MustExec("drop table if exists t_spfresh_filter_req")
	tk.MustExec("create table t_spfresh_filter_req(id int primary key, tenant_id bigint unsigned not null, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
	tk.MustExec("insert into t_spfresh_filter_req values (1, 1, '[1,2,3]')")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_filter_req, idx_vec) */ id from t_spfresh_filter_req where tenant_id - 2 < 0 order by vec_l2_distance(vec, '[1,2,3]') limit 1").
		Check(testkit.Rows())

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
	require.NotNil(t, reqPB.FilterExpr)
	require.Len(t, reqPB.FilterExprColumns, 1)
	require.Equal(t, int64(2), reqPB.FilterExprColumns[0].GetColumnId())
	require.Equal(t, tipb.SPFreshFilterExprColumnSource_Stored, reqPB.FilterExprColumns[0].GetSource())
	require.NotZero(t, reqPB.FilterExprColumns[0].GetSource())
	require.NotNil(t, reqPB.FilterExprColumns[0].GetFieldType())
	require.Equal(t, int32(mysql.TypeLonglong), reqPB.FilterExprColumns[0].GetFieldType().Tp)
	require.NotZero(t, reqPB.FilterExprColumns[0].GetFieldType().Flag&uint32(mysql.UnsignedFlag))
	require.NotNil(t, reqPB.EvalContext)
	require.Equal(t, "Asia/Shanghai", reqPB.EvalContext.GetTimeZoneName())
	require.Equal(t, int64(8*60*60), reqPB.EvalContext.GetTimeZoneOffset())
	require.Equal(t, tipb.SPFreshTruncateMode_TruncateWarning, reqPB.EvalContext.GetTruncateMode())
	require.True(t, reqPB.EvalContext.GetOverflowAsWarning())
	require.True(t, reqPB.EvalContext.GetIgnoreZeroInDate())
	require.True(t, reqPB.EvalContext.GetDividedByZeroAsWarning())
	require.Equal(t, uint64(mysql.ModeStrictTransTables|mysql.ModeErrorForDivisionByZero|mysql.ModeNoUnsignedSubtraction|mysql.ModeNoZeroInDate|mysql.ModeNoZeroDate|mysql.ModeInvalidDates), reqPB.EvalContext.GetSqlMode())
	require.Equal(t, uint64(4096), reqPB.EvalContext.GetMaxAllowedPacket())
	require.Equal(t, uint32(7), reqPB.EvalContext.GetDivPrecisionIncrement())
	require.Equal(t, uint64(64), reqPB.EvalContext.GetMaxWarningCount())
	require.Len(t, reqPB.RequiredColumns, 0)
}

func TestSPFreshVectorSearchExec_RequestIncludesExtraHandleFilterExpr(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := decodeSPFreshSearchRequestForTest(t, body)
		gotReq.Store(reqPB)

		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{{
				HandleBytes:  kv.IntHandle(1001).Encoded(),
				ColumnValues: make([][]byte, len(reqPB.RequiredColumns)),
			}},
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_rowid_filter_req")
	tk.MustExec("create table t_spfresh_rowid_filter_req(id int, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("set @@tidb_opt_write_row_id = 1")
	tk.MustExec("insert into t_spfresh_rowid_filter_req(_tidb_rowid, id, vec) values (1001, 7, '[1,2,3]')")

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_rowid_filter_req, idx_vec) */ _tidb_rowid
		from t_spfresh_rowid_filter_req
		where _tidb_rowid > 1000
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`).Check(testkit.Rows("1001"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
	require.NotNil(t, reqPB.FilterExpr)
	require.Len(t, reqPB.FilterExprColumns, 1)
	require.Equal(t, int64(model.ExtraHandleID), reqPB.FilterExprColumns[0].GetColumnId())
	require.Equal(t, tipb.SPFreshFilterExprColumnSource_Handle, reqPB.FilterExprColumns[0].GetSource())
	require.NotZero(t, reqPB.FilterExprColumns[0].GetSource())
	require.NotNil(t, reqPB.FilterExprColumns[0].GetFieldType())
	require.Equal(t, int32(mysql.TypeLonglong), reqPB.FilterExprColumns[0].GetFieldType().Tp)
	require.NotZero(t, reqPB.FilterExprColumns[0].GetFieldType().Flag&uint32(mysql.NotNullFlag))
	require.Len(t, reqPB.RequiredColumns, 0)
}

func TestSPFreshVectorSearchExec_RequestIncludesTemporalFilterExpr(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		gotReq.Store(decodeSPFreshSearchRequestForTest(t, body))

		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{},
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = 'Asia/Shanghai'")

	tk.MustExec("drop table if exists t_spfresh_datetime_filter_req")
	tk.MustExec(`
		create table t_spfresh_datetime_filter_req(
			id int primary key,
			created_at datetime not null,
			vec vector(3),
			vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (created_at)
		)`)
	tk.MustExec(`
		insert into t_spfresh_datetime_filter_req
		values (1, '2026-01-05 12:00:00', '[1,2,3]')`)

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_datetime_filter_req, idx_vec) */ id
		from t_spfresh_datetime_filter_req
		where created_at >= '2026-01-05 10:00:00'
		  and created_at <  '2026-01-05 17:00:00'
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`).Check(testkit.Rows())
	requireSPFreshTemporalFilterRequest(t, gotReq.Load(), mysql.TypeDatetime)

	tk.MustExec("drop table if exists t_spfresh_timestamp_filter_req")
	tk.MustExec(`
		create table t_spfresh_timestamp_filter_req(
			id int primary key,
			updated_at timestamp not null,
			vec vector(3),
			vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (updated_at)
		)`)
	tk.MustExec(`
		insert into t_spfresh_timestamp_filter_req
		values (1, '2026-01-05 12:00:00', '[1,2,3]')`)

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_timestamp_filter_req, idx_vec) */ id
		from t_spfresh_timestamp_filter_req
		where updated_at >= '2026-01-05 10:00:00'
		  and updated_at <  '2026-01-05 17:00:00'
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`).Check(testkit.Rows())
	requireSPFreshTemporalFilterRequest(t, gotReq.Load(), mysql.TypeTimestamp)
}

func TestSPFreshVectorSearchExec_ExplainAnalyzeStats(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	const (
		readOnly             bool    = false
		oversampleFactor     float64 = 2.5
		partitionsScanned    uint64  = 7
		vectorsScanned       uint64  = 123
		partitionCacheHits   uint64  = 17
		partitionCacheMisses uint64  = 18
		tableLookupKeys      uint64  = 45
		tableLookupBytes     uint64  = 6789
		permitMicros         uint64  = 11_000
		configMicros         uint64  = 12_000
		indexOpenMicros      uint64  = 13_000
		searchMicros         uint64  = 14_000
		tableLookupMicros    uint64  = 15_000
		tikvClientRPCCount   uint64  = 16
		tikvClientRPCMicros  uint64  = 16_000
		annSearchMicros      uint64  = 5_800
		annPartitions        uint64  = 18
		annCentroids         uint64  = 120
		annLeafVectors       uint64  = 1460
		annRoughCandidates   uint64  = 200
		annBoundPruned       uint64  = 76
		annRerankCandidates  uint64  = 124
		annExactEvaluated    uint64  = 124
		annCacheHits         uint64  = 15
		annCacheMisses       uint64  = 3
		annCacheLookupMicros uint64  = 400
		annCacheLoadMicros   uint64  = 2_800
		annTableLookupMicros uint64  = 2_400
		annRerankMicros      uint64  = 700
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &tipb.SPFreshSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))

		respPB := spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{
				{
					HandleBytes:  kv.IntHandle(1).Encoded(),
					Distance:     0,
					ColumnValues: make([][]byte, len(reqPB.RequiredColumns)),
				},
			},
			Stats: &tipb.SPFreshSearchStats{
				ReadOnly:             readOnly,
				OversampleFactor:     oversampleFactor,
				PartitionsScanned:    partitionsScanned,
				VectorsScanned:       vectorsScanned,
				PartitionCacheHits:   partitionCacheHits,
				PartitionCacheMisses: partitionCacheMisses,
				TableLookupKeys:      tableLookupKeys,
				TableLookupBytes:     tableLookupBytes,
				PermitMicros:         permitMicros,
				ConfigMicros:         configMicros,
				IndexOpenMicros:      indexOpenMicros,
				SearchMicros:         searchMicros,
				TableLookupMicros:    tableLookupMicros,
				TikvClientRpcCount:   tikvClientRPCCount,
				TikvClientRpcMicros:  tikvClientRPCMicros,
				Ann: &tipb.SPFreshANNStats{
					ExactFallback:                false,
					SearchMicros:                 annSearchMicros,
					PartitionsScanned:            annPartitions,
					CentroidsScanned:             annCentroids,
					LeafVectorsScanned:           annLeafVectors,
					RoughCandidates:              annRoughCandidates,
					BoundPrunedCandidates:        annBoundPruned,
					RerankCandidates:             annRerankCandidates,
					ExactEvaluated:               annExactEvaluated,
					PartitionCacheHits:           annCacheHits,
					PartitionCacheMisses:         annCacheMisses,
					PartitionCacheLookupMicros:   annCacheLookupMicros,
					PartitionCacheMissLoadMicros: annCacheLoadMicros,
					TableLookupMicros:            annTableLookupMicros,
					RerankMicros:                 annRerankMicros,
				},
			},
		})
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_stats")
	tk.MustExec("create table t_spfresh_stats(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_stats values (1,'[0,0,0]')")

	rows := tk.MustQuery("explain analyze select /*+ USE_INDEX(t_spfresh_stats, idx_vec) */ id from t_spfresh_stats order by vec_l2_distance(vec, '[0,0,0]') limit 1").Rows()
	found := false
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		id, ok := row[0].(string)
		require.True(t, ok)
		if !strings.Contains(id, "SPFreshVectorScan") {
			continue
		}

		found = true
		require.GreaterOrEqual(t, len(row), 3)
		require.Equal(t, "1", row[2])
		var cols []string
		for _, c := range row {
			cols = append(cols, c.(string))
		}
		all := strings.Join(cols, " | ")
		require.Contains(t, all, "ann: {exact_fallback: false, spfresh: {search: {time: 5.8ms, partitions_scanned: 18, centroids_scanned: 120, leaf_vectors_scanned: 1460}, candidates: {rough: 200, bound_pruned: 76, rerank: 124, exact_evaluated: 124}, partition_cache: {hit: 15, miss: 3, lookup_time: 400µs, miss_load_time: 2.8ms}, table_lookup_time: 2.4ms, rerank_time: 700µs}}")
		require.Contains(t, all, "vector_search: {read_only: false, oversample: 2.5, table_lookup_keys: 45, table_lookup_bytes: 6789, permit: 11ms, config: 12ms, index_open: 13ms, tikv_client_rpc_count: 16, tikv_client_rpc: 16ms}")
		require.NotContains(t, all, fmt.Sprintf("partitions_scanned: %d", partitionsScanned))
		require.NotContains(t, all, fmt.Sprintf("vectors_scanned: %d", vectorsScanned))
		require.NotContains(t, all, fmt.Sprintf("partition_cache_hits: %d", partitionCacheHits))
		require.NotContains(t, all, fmt.Sprintf("partition_cache_misses: %d", partitionCacheMisses))
		require.NotContains(t, all, fmt.Sprintf("search: %dms", searchMicros/1_000))
		require.NotContains(t, all, fmt.Sprintf("table_lookup: %dms", tableLookupMicros/1_000))
	}
	require.True(t, found)
}

func TestSPFreshVectorSearchExec_P2CDistributesHotIndexSearch(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		firstHits.Add(1)
		writeEmptySPFreshSearchResponse(t, w, r)
	}))
	t.Cleanup(first.Close)

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		secondHits.Add(1)
		writeEmptySPFreshSearchResponse(t, w, r)
	}))
	t.Cleanup(second.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{first.URL, second.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_p2c")
	tk.MustExec("create table t_spfresh_p2c(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_p2c values (1,'[0,0,0]')")

	for i := 0; i < 12; i++ {
		tk.MustQuery("select /*+ USE_INDEX(t_spfresh_p2c, idx_vec) */ id from t_spfresh_p2c order by vec_l2_distance(vec, '[0,0,0]') limit 1").
			Check(testkit.Rows())
	}

	require.Positive(t, firstHits.Load())
	require.Positive(t, secondHits.Load())
}

func TestSPFreshVectorSearchExec_HTTPErrorMapping(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	cases := []struct {
		name       string
		statusCode int
		wantSubstr string
	}{
		{"bad_request", http.StatusBadRequest, "bad request"},
		{"not_found", http.StatusNotFound, "not found"},
		{"conflict", http.StatusConflict, "not ready"},
		{"unavailable", http.StatusServiceUnavailable, "unavailable"},
		{"internal", http.StatusInternalServerError, "failed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor.ResetSPFreshVectorSearchForTest()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handleSPFreshSearchTestBootstrap(t, w, r) {
					return
				}
				require.Equal(t, "/spfresh/search", r.URL.Path)
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte("mock error"))
			}))
			t.Cleanup(srv.Close)

			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddrs = []string{srv.URL}
			})

			store, _ := testkit.CreateMockStoreAndDomain(t)
			t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t_spfresh_err")
			tk.MustExec("create table t_spfresh_err(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
			tk.MustExec("insert into t_spfresh_err values (1,'[0,0,0]')")

			err := tk.QueryToErr("select /*+ USE_INDEX(t_spfresh_err, idx_vec) */ id from t_spfresh_err order by vec_l2_distance(vec, '[0,0,0]') limit 1")
			require.Error(t, err)
			require.Contains(t, err.Error(), "spfresh /spfresh/search")
			require.Contains(t, err.Error(), tc.wantSubstr)
		})
	}
}

func TestSPFreshVectorSearchExec_AppendsFilterWarnings(t *testing.T) {
	tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		reqPB := decodeSPFreshSearchRequestForTest(t, body)
		require.NotNil(t, reqPB.GetFilterExpr())
		require.Equal(t, uint64(64), reqPB.GetEvalContext().GetMaxWarningCount())

		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{},
			Warnings: []*tipb.Error{
				{Code: int32(mysql.ErrWarnAllowedPacketOverflowed), Msg: "first filter warning"},
				{Code: int32(mysql.ErrDivisionByZero), Msg: "second filter warning"},
			},
			WarningCount: 3,
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))

	tk.MustExec("drop table if exists t_spfresh_filter_warn")
	tk.MustExec("create table t_spfresh_filter_warn(id int primary key, tenant_id bigint not null, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
	tk.MustExec("insert into t_spfresh_filter_warn values (1, 7, '[1,2,3]')")

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_filter_warn, idx_vec) */ id
		from t_spfresh_filter_warn
		where tenant_id = 7
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`).Check(testkit.Rows())

	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warnings, 2)
	require.Equal(t, uint16(3), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	warningErr, ok := errors.Cause(warnings[0].Err).(*terror.Error)
	require.True(t, ok)
	require.Equal(t, uint16(mysql.ErrWarnAllowedPacketOverflowed), terror.ToSQLError(warningErr).Code)
	require.Contains(t, warnings[0].Err.Error(), "first filter warning")
	require.Contains(t, warnings[1].Err.Error(), "second filter warning")
}

func TestSPFreshVectorSearchExec_RespectsRequiredRowsAcrossChunks(t *testing.T) {
	var searchHits atomic.Int32
	tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		searchHits.Add(1)
		rows := make([]*tipb.SPFreshSearchRow, 35)
		for i := range rows {
			rows[i] = &tipb.SPFreshSearchRow{HandleBytes: kv.IntHandle(i + 1).Encoded()}
		}
		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows:         rows,
			Warnings:     []*tipb.Error{{Code: int32(mysql.ErrTruncatedWrongValue), Msg: "filter warning"}},
			WarningCount: 1,
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))

	tk.MustExec("set @@tidb_max_chunk_size = 32")
	tk.MustExec("drop table if exists t_spfresh_required_rows")
	tk.MustExec("create table t_spfresh_required_rows(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_required_rows values (1, '[1,2,3]'), (2, '[1,2,3]'), (3, '[1,2,3]'), (4, '[1,2,3]'), (5, '[1,2,3]')")

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_required_rows, idx_vec) */ id
		from t_spfresh_required_rows
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 2 offset 33`).Check(testkit.Rows("34", "35"))
	require.Equal(t, int32(1), searchHits.Load())
	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0].Err.Error(), "filter warning")
}

func TestSPFreshVectorSearchExec_DoesNotAppendWarningsForInvalidRows(t *testing.T) {
	cases := []struct {
		name         string
		handleBytes  []byte
		columnValues [][]byte
		wantSubstr   string
	}{
		{
			name:       "shape",
			wantSubstr: "mismatched column values",
		},
		{
			name:         "datum",
			columnValues: [][]byte{{0xff}},
			wantSubstr:   "invalid encoded key flag 255",
		},
		{
			name:         "handle",
			handleBytes:  []byte{0xff},
			columnValues: [][]byte{{codec.IntHandleFlag, 1}},
			wantSubstr:   "insufficient bytes to decode value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/spfresh/search" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.NoError(t, r.Body.Close())
				reqPB := decodeSPFreshSearchRequestForTest(t, body)
				require.Len(t, reqPB.RequiredColumns, 1)

				handleBytes := tc.handleBytes
				if handleBytes == nil {
					handleBytes = kv.IntHandle(1).Encoded()
				}
				validValue, err := codec.EncodeValue(nil, nil, types.NewIntDatum(7))
				require.NoError(t, err)
				respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
					Rows: []*tipb.SPFreshSearchRow{
						{HandleBytes: kv.IntHandle(1).Encoded(), ColumnValues: [][]byte{validValue}},
						{
							HandleBytes:  handleBytes,
							ColumnValues: tc.columnValues,
						},
					},
					Warnings: []*tipb.Error{
						{Code: int32(mysql.ErrTruncatedWrongValue), Msg: "must not be committed"},
					},
					WarningCount: 1,
				}))
				require.NoError(t, err)
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(respBytes)
			}))

			tk.MustExec("drop table if exists t_spfresh_invalid_row")
			tk.MustExec("create table t_spfresh_invalid_row(id int primary key, tenant_id bigint not null, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
			tk.MustExec("insert into t_spfresh_invalid_row values (1, 7, '[1,2,3]')")

			err := tk.QueryToErr(`
				select /*+ USE_INDEX(t_spfresh_invalid_row, idx_vec) */ tenant_id
				from t_spfresh_invalid_row
				order by vec_l2_distance(vec, '[1,2,3]')
				limit 2`)
			require.ErrorContains(t, err, tc.wantSubstr)
			require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
	}
}

func TestSPFreshVectorSearchExec_CapsFilterWarningDetails(t *testing.T) {
	tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		warnings := make([]*tipb.Error, 70)
		for i := range warnings {
			warnings[i] = &tipb.Error{
				Code: int32(mysql.ErrTruncatedWrongValue),
				Msg:  fmt.Sprintf("filter warning %d", i),
			}
		}
		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows:         []*tipb.SPFreshSearchRow{},
			Warnings:     warnings,
			WarningCount: uint64(len(warnings)),
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))

	tk.MustExec("drop table if exists t_spfresh_many_warn")
	tk.MustExec("create table t_spfresh_many_warn(id int primary key, tenant_id bigint not null, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
	tk.MustExec("insert into t_spfresh_many_warn values (1, 7, '[1,2,3]')")

	tk.MustQuery(`
		select /*+ USE_INDEX(t_spfresh_many_warn, idx_vec) */ id
		from t_spfresh_many_warn
		where tenant_id = 7
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`).Check(testkit.Rows())

	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warnings, 64)
	require.Equal(t, uint16(70), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	require.Contains(t, warnings[0].Err.Error(), "filter warning 0")
	require.Contains(t, warnings[63].Err.Error(), "filter warning 63")
}

func TestSPFreshVectorSearchExec_RejectsMalformedFilterWarnings(t *testing.T) {
	cases := []struct {
		name       string
		resp       *tipb.SPFreshSearchResponse
		wantSubstr string
	}{
		{
			name: "count_less_than_details",
			resp: spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
				Rows:         []*tipb.SPFreshSearchRow{},
				Warnings:     []*tipb.Error{{Code: int32(mysql.ErrTruncatedWrongValue), Msg: "detail"}},
				WarningCount: 0,
			}),
			wantSubstr: "smaller than warning details",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/spfresh/search" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				require.NoError(t, r.Body.Close())
				respBytes, err := proto.Marshal(tc.resp)
				require.NoError(t, err)
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(respBytes)
			}))

			tk.MustExec("drop table if exists t_spfresh_bad_warn")
			tk.MustExec("create table t_spfresh_bad_warn(id int primary key, tenant_id bigint not null, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
			tk.MustExec("insert into t_spfresh_bad_warn values (1, 7, '[1,2,3]')")

			err := tk.QueryToErr(`
				select /*+ USE_INDEX(t_spfresh_bad_warn, idx_vec) */ id
				from t_spfresh_bad_warn
				where tenant_id = 7
				order by vec_l2_distance(vec, '[1,2,3]')
				limit 1`)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantSubstr)
		})
	}
}

func TestSPFreshVectorSearchExec_ResponseError(t *testing.T) {
	tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		respBytes, err := proto.Marshal(&tipb.SPFreshSearchResponse{
			Result: &tipb.SPFreshSearchResponse_Error{Error: &tipb.Error{
				Code: int32(mysql.ErrWarnAllowedPacketOverflowed),
				Msg:  "worker rejected filter",
			}},
		})
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))

	tk.MustExec("drop table if exists t_spfresh_resp_err")
	tk.MustExec("create table t_spfresh_resp_err(id int primary key, vec vector(3), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_resp_err values (1, '[1,2,3]')")

	err := tk.QueryToErr(`
		select /*+ USE_INDEX(t_spfresh_resp_err, idx_vec) */ id
		from t_spfresh_resp_err
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`)
	require.Error(t, err)
	tidbErr, ok := errors.Cause(err).(*terror.Error)
	require.True(t, ok)
	require.Equal(t, uint16(mysql.ErrWarnAllowedPacketOverflowed), terror.ToSQLError(tidbErr).Code)
	require.Contains(t, err.Error(), "worker rejected filter")
}

func TestSPFreshVectorSearchExec_VirtualGeneratedColumnFill(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &tipb.SPFreshSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))
		gotReq.Store(reqPB)

		// Always return the only row.
		var colVals [][]byte
		for _, col := range reqPB.RequiredColumns {
			switch col.Tp {
			case int32(mysql.TypeTiDBVectorFloat32):
				vec, err := types.ParseVectorFloat32(`[1,2,3]`)
				require.NoError(t, err)
				vecBytes, err := codec.EncodeValue(nil, nil, types.NewVectorFloat32Datum(vec))
				require.NoError(t, err)
				colVals = append(colVals, vecBytes)
			default:
				t.Fatalf("unexpected required column tp=%v", col.Tp)
			}
		}
		respPB := spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{
				{
					HandleBytes:  kv.IntHandle(1).Encoded(),
					Distance:     0,
					ColumnValues: colVals,
				},
			},
		})
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_vgen")
	tk.MustExec("create table t_spfresh_vgen(id int primary key, vec vector(3), g int generated always as (id+1) virtual, vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_vgen values (1,'[1,2,3]', default)")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_vgen, idx_vec) */ id, g from t_spfresh_vgen order by vec_l2_distance(vec, '[1,2,3]') limit 1").
		Check(testkit.Rows("1 2"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
}

func TestSPFreshVectorSearchExec_CommonHandlePrefixColumn(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[tipb.SPFreshSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &tipb.SPFreshSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))
		gotReq.Store(reqPB)

		// Handle bytes only contains PK prefix for `a(2)` and full `b`.
		handle := testutil.MustNewCommonHandle(t, "ab", 1)

		var colVals [][]byte
		for _, col := range reqPB.RequiredColumns {
			switch col.Tp {
			case int32(mysql.TypeVarchar):
				valBytes, err := codec.EncodeValue(nil, nil, types.NewStringDatum("abcdef"))
				require.NoError(t, err)
				colVals = append(colVals, valBytes)
			default:
				t.Fatalf("unexpected required column tp=%v", col.Tp)
			}
		}

		respPB := spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{
				{
					HandleBytes:  handle.Encoded(),
					Distance:     0,
					ColumnValues: colVals,
				},
			},
		})
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_ch")
	tk.MustExec("create table t_spfresh_ch(a varchar(10), b int, vec vector(3), primary key (a(2), b), vector index idx_vec ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_ch values ('abcdef', 1, '[1,2,3]')")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_ch, idx_vec) */ a, b from t_spfresh_ch order by vec_l2_distance(vec, '[1,2,3]') limit 1").
		Check(testkit.Rows("abcdef 1"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
	require.Len(t, reqPB.RequiredColumns, 1)
	require.Equal(t, int32(mysql.TypeVarchar), reqPB.RequiredColumns[0].Tp)
}

func TestSPFreshVectorSearchExec_ValidatesUnprojectedCommonHandle(t *testing.T) {
	tk := newSPFreshSearchTestKit(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/spfresh/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		reqPB := decodeSPFreshSearchRequestForTest(t, body)
		require.Len(t, reqPB.RequiredColumns, 1)

		storedValue, err := codec.EncodeValue(nil, nil, types.NewIntDatum(7))
		require.NoError(t, err)
		respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{
				{
					HandleBytes:  []byte{0xff},
					ColumnValues: [][]byte{storedValue},
				},
			},
			Warnings: []*tipb.Error{
				{Code: int32(mysql.ErrTruncatedWrongValue), Msg: "must not be committed"},
			},
			WarningCount: 1,
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))

	tk.MustExec("drop table if exists t_spfresh_invalid_common_handle")
	tk.MustExec("create table t_spfresh_invalid_common_handle(a varchar(10), b int, tenant_id bigint, vec vector(3), primary key (a, b), vector index idx_vec ((vec_l2_distance(vec))) using spfresh storing (tenant_id))")
	tk.MustExec("insert into t_spfresh_invalid_common_handle values ('a', 1, 7, '[1,2,3]')")

	err := tk.QueryToErr(`
		select /*+ USE_INDEX(t_spfresh_invalid_common_handle, idx_vec) */ tenant_id
		from t_spfresh_invalid_common_handle
		order by vec_l2_distance(vec, '[1,2,3]')
		limit 1`)
	require.Error(t, err)
	require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
}

func handleSPFreshSearchTestBootstrap(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	if r.URL.Path != "/spfresh/bootstrap" {
		return false
	}
	require.Equal(t, http.MethodPost, r.Method)
	require.NotEmpty(t, r.URL.Query().Get("cluster_id"))
	_, err := io.Copy(io.Discard, r.Body)
	require.NoError(t, err)
	require.NoError(t, r.Body.Close())
	w.WriteHeader(http.StatusOK)
	return true
}

func newSPFreshSearchTestKit(t *testing.T, handlers ...http.Handler) *testkit.TestKit {
	t.Helper()
	executor.ResetSPFreshVectorSearchForTest()

	addrs := make([]string, 0, len(handlers))
	for _, handler := range handlers {
		handler := handler
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if handleSPFreshSearchTestBootstrap(t, w, r) {
				return
			}
			handler.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		addrs = append(addrs, srv.URL)
	}

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = addrs
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk
}

func writeEmptySPFreshSearchResponse(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	require.Equal(t, "/spfresh/search", r.URL.Path)
	reqBody, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.NoError(t, r.Body.Close())

	reqPB := &tipb.SPFreshSearchRequest{}
	require.NoError(t, proto.Unmarshal(reqBody, reqPB))

	respBytes, err := proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
		Rows: []*tipb.SPFreshSearchRow{},
	}))
	require.NoError(t, err)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

func spfreshSuccessResponseForTest(result *tipb.SPFreshSearchResult) *tipb.SPFreshSearchResponse {
	// Match the current worker producer, which always emits success stats.
	if result.Stats == nil {
		result.Stats = &tipb.SPFreshSearchStats{}
	}
	return &tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Success{Success: result},
	}
}
