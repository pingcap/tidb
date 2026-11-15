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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestSPFreshPlanCache(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()
	var request atomic.Pointer[tipb.SPFreshSearchRequest]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/search", r.URL.Path)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		req := decodeSPFreshSearchRequestForTest(t, data)
		request.Store(req)
		require.Len(t, req.QueryVectorF32Le, 12)
		value := math.Float32frombits(binary.LittleEndian.Uint32(req.QueryVectorF32Le))
		vec := types.MustCreateVectorFloat32([]float32{10, 0, 0})
		vecBytes, err := codec.EncodeValue(nil, nil, types.NewVectorFloat32Datum(vec))
		require.NoError(t, err)
		var columns [][]byte
		for range req.RequiredColumns {
			columns = append(columns, vecBytes)
		}
		data, err = proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{
			Rows: []*tipb.SPFreshSearchRow{{HandleBytes: kv.IntHandle(int64(value)).Encoded(), Distance: float32(math.Abs(float64(10 - value))), ColumnValues: columns}},
		}))
		require.NoError(t, err)
		_, err = w.Write(data)
		require.NoError(t, err)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(c *config.Config) { c.TiKVAPIServiceAddrs = []string{srv.URL} })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table spfresh_cache(id int primary key, vec vector(3), vector index vi ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into spfresh_cache values (10, '[10,0,0]')")
	for _, instance := range []int{0, 1} {
		t.Run(fmt.Sprintf("instance_%d", instance), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set global tidb_enable_instance_plan_cache=%d", instance))
			tk.MustExec("admin flush instance plan_cache")
			for session := range 2 {
				s := testkit.NewTestKit(t, store)
				s.MustExec("use test")
				s.MustExec(`prepare q from "select id from spfresh_cache order by vec_l2_distance(vec, '[1,2,3]') limit 1"`)
				for i := range 2 {
					s.MustQuery("execute q").Check(testkit.Rows("1"))
					hit := "0"
					if i > 0 || instance == 1 && session > 0 {
						hit = "1"
					}
					s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(hit))
					require.Equal(t, uint64(1), request.Load().TopK)
				}
				oldIndex := request.Load().IndexId
				beam := 321 + instance*2 + session
				tk.MustExec(fmt.Sprintf("alter table spfresh_cache alter index vi set vector_index_param 'search_beam_size=%d'", beam))
				s.MustQuery("execute q").Check(testkit.Rows("1"))
				s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
				require.Equal(t, oldIndex, request.Load().IndexId)
				require.EqualValues(t, beam, request.Load().SearchBeamSize)
			}
		})
	}
	for _, instance := range []int{0, 1} {
		t.Run(fmt.Sprintf("parameterized_instance_%d", instance), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set global tidb_enable_instance_plan_cache=%d", instance))
			tk.MustExec("admin flush instance plan_cache")
			for session := range 2 {
				s := testkit.NewTestKit(t, store)
				s.MustExec("use test")
				s.MustExec("prepare p from 'select id from spfresh_cache order by vec_l2_distance(vec, ?) limit ?'")
				s.MustExec("set @k=1")
				for i, v := range []int{1, 2, 3} {
					s.MustExec(fmt.Sprintf("set @v='[%d,0,0]'", v))
					s.MustQuery("execute p using @v,@k").Check(testkit.Rows(fmt.Sprint(v)))
					hit := "0"
					if i > 0 || instance == 1 && session > 0 {
						hit = "1"
					}
					s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(hit))
				}
				s.MustExec("set @k=2")
				s.MustQuery("execute p using @v,@k").Check(testkit.Rows("3"))
				require.Equal(t, uint64(2), request.Load().TopK)
				s.MustExec("set @k=1, @v='invalid vector'")
				require.Error(t, s.ExecToErr("execute p using @v,@k"))
				s.MustExec("set @v='[1,0]'")
				require.Error(t, s.ExecToErr("execute p using @v,@k"))
				s.MustExec("set @v='[4,0,0]'")
				s.MustQuery("execute p using @v,@k").Check(testkit.Rows("4"))
				s.MustExec("set @v=NULL")
				s.MustQuery("execute p using @v,@k").Check(testkit.Rows("10"))
				s.MustExec("set @v='[5,0,0]'")
				s.MustQuery("execute p using @v,@k").Check(testkit.Rows("5"))
			}
			s := testkit.NewTestKit(t, store)
			s.MustExec("use test")
			s.MustExec("set tidb_enable_non_prepared_plan_cache=1")
			for i, v := range []int{6, 7, 8} {
				s.MustQuery(fmt.Sprintf("select id from spfresh_cache order by vec_l2_distance(vec, '[%d,0,0]') limit 1", v)).Check(testkit.Rows(fmt.Sprint(v)))
				if i > 0 {
					s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
				}
			}
			for _, suffix := range []string{"", " group by vec"} {
				distinct := "distinct "
				if suffix != "" {
					distinct = ""
				}
				s.MustQuery("select " + distinct + "vec_l2_distance(vec, '[1,0,0]') from spfresh_cache" + suffix + " order by vec_l2_distance(vec, '[1,0,0]') limit 1").Check(testkit.Rows("9"))
				s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
			}

			s.MustExec("prepare d from 'select id,vec_l2_distance(vec, ?) from spfresh_cache order by vec_l2_distance(vec, ?) limit 1'")
			s.MustExec("set @a='[1,0,0]', @b='[1,0,0]'")
			s.MustQuery("execute d using @a,@b").Check(testkit.Rows("1 9"))
			s.MustExec("set @a='[2,0,0]', @b='[3,0,0]'")
			s.MustQuery("execute d using @a,@b").Check(testkit.Rows("3 8"))
			s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		})
	}

	tk.MustExec("set global tidb_enable_instance_plan_cache=1")
	tk.MustExec("admin flush instance plan_cache")
	var group sync.WaitGroup
	for worker := range 4 {
		s := testkit.NewTestKit(t, store)
		s.MustExec("use test")
		s.MustExec("prepare concurrent_vector from 'select id from spfresh_cache order by vec_l2_distance(vec, ?) limit 1'")
		group.Add(1)
		go func() {
			defer group.Done()
			for step := range 20 {
				value := worker*20 + step + 1
				s.MustExec(fmt.Sprintf("set @v='[%d,0,0]'", value))
				s.MustQuery("execute concurrent_vector using @v").Check(testkit.Rows(fmt.Sprint(value)))
			}
		}()
	}
	group.Wait()

	tk.MustExec("set global tidb_enable_instance_plan_cache=0")
}

func TestSPFreshPlanCacheFiltersAndWarnings(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()
	var request atomic.Pointer[tipb.SPFreshSearchRequest]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		req := decodeSPFreshSearchRequestForTest(t, data)
		request.Store(req)
		// Emulate the single stored row's integer filter so stale protobuf values
		// fail both the wire assertion and the SQL result assertion.
		matches := true
		if filter := req.FilterExpr; filter != nil {
			require.Equal(t, tipb.ScalarFuncSig_GTInt, filter.Sig)
			require.Len(t, filter.Children, 2)
			_, threshold, err := codec.DecodeInt(filter.Children[1].Val)
			require.NoError(t, err)
			matches = 3000 > threshold
		}
		result := &tipb.SPFreshSearchResult{}
		if matches {
			result.Rows = []*tipb.SPFreshSearchRow{{HandleBytes: kv.IntHandle(3000).Encoded()}}
		}
		data, err = proto.Marshal(spfreshSuccessResponseForTest(result))
		require.NoError(t, err)
		_, err = w.Write(data)
		require.NoError(t, err)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(c *config.Config) { c.TiKVAPIServiceAddrs = []string{srv.URL} })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table spfresh_filter_cache(id bigint primary key, ts bigint, vec vector(3), vector index vi ((vec_l2_distance(vec))) using spfresh storing (ts))")
	tk.MustExec("insert into spfresh_filter_cache values (3000, 3000, '[1,0,0]')")
	for _, instance := range []int{0, 1} {
		t.Run(fmt.Sprintf("instance_%d", instance), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set global tidb_enable_instance_plan_cache=%d", instance))
			tk.MustExec("admin flush instance plan_cache")
			for _, tc := range []struct {
				expr   string
				offset int64
			}{
				{expr: "1000"},
				{expr: "unix_timestamp()"},
				{expr: "unix_timestamp()+1", offset: 1},
				{expr: "?"},
			} {
				for session := range 2 {
					s := testkit.NewTestKit(t, store)
					s.MustExec("use test")
					s.MustExec(fmt.Sprintf("prepare q from \"select id from spfresh_filter_cache where ts > %s order by vec_l2_distance(vec, '[1,0,0]') limit 1\"", tc.expr))
					for i, timestamp := range []int64{1000, 4000} {
						s.MustExec(fmt.Sprintf("set timestamp=%d, @threshold=%d", timestamp, timestamp))
						threshold := timestamp + tc.offset
						hit := "0"
						if tc.expr == "1000" {
							threshold = 1000
							if i > 0 || instance == 1 && session > 0 {
								hit = "1"
							}
						}
						query := "execute q"
						if tc.expr == "?" {
							query += " using @threshold"
						}
						request.Store(nil)
						rows := s.MustQuery(query)
						if threshold < 3000 {
							rows.Check(testkit.Rows("3000"))
						} else {
							rows.Check(testkit.Rows())
						}
						s.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(hit))
						req := request.Load()
						require.NotNil(t, req, "query must still use SPFresh")
						require.NotNil(t, req.FilterExpr)
						_, actual, err := codec.DecodeInt(req.FilterExpr.Children[1].Val)
						require.NoError(t, err)
						require.Equal(t, threshold, actual)
					}
				}
			}

			s := testkit.NewTestKit(t, store)
			s.InProcedure()
			s.MustExec("use test")
			s.MustExec("set tidb_enable_sp_plan_cache=1")
			s.MustExec("drop procedure if exists spfresh_cached_warning")
			s.MustExec("create procedure spfresh_cached_warning() select id from spfresh_filter_cache order by vec_l2_distance(vec, '[1,0,0]') limit 1")
			s.MustExec("prepare warning_query from \"select id from spfresh_filter_cache order by vec_l2_distance(vec, '[1,0,0]') limit 1\"")
			for _, query := range []string{"execute warning_query", "call spfresh_cached_warning()"} {
				s.MustExec("begin")
				for i := range 3 {
					s.ClearProcedureRes()
					s.MustExec(query)
					hit := s.Session().GetSessionVars().FoundInPlanCache
					s.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 SPFresh search does not support repeatable reads in explicit transactions"))
					if i > 0 {
						require.True(t, hit, query)
					}
				}
				s.MustExec("rollback")
			}
		})
	}
	tk.MustExec("set global tidb_enable_instance_plan_cache=0")
}
