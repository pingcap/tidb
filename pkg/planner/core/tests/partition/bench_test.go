// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
	_ "github.com/pingcap/tidb/pkg/autoid_service"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func prepareBenchSession() (sessiontypes.Session, *domain.Domain, kv.Storage) {
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.Instance.EnableSlowLog.Store(false)
	})

	store, err := mockstore.NewMockStore()
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	dom, err := session.BootstrapSession(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	// TODO: use environment variable "log_level" if set?
	log.SetLevel(zapcore.ErrorLevel)
	se, err := session.CreateSession4Test(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	mustExecute(se, "use test")
	return se, dom, store
}

func mustExecute(s sessiontypes.Session, sql string, args ...any) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, sql, args...)
	defer cancel()
	if err != nil {
		logutil.BgLogger().Fatal("mustExecute error", zap.String("sql", sql), zap.Error(err), zap.Stack("stack"))
	}
}

func drainRecordSet(ctx context.Context, rs sqlexec.RecordSet, alloc chunk.Allocator) ([]chunk.Row, error) {
	var rows []chunk.Row
	var req *chunk.Chunk
	req = rs.NewChunk(alloc)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, 1024)
	}
}

func runPointSelect(b *testing.B, se sessiontypes.Session, query, expectedPlan string, enablePlanCache bool) {
	ctx := context.Background()
	alloc := chunk.NewAllocator()
	if enablePlanCache {
		mustExecute(se, "set tidb_enable_non_prepared_plan_cache = 1")
		// Make sure to flush the plan cache, so previous benchmark can not be hit.
		mustExecute(se, "set tidb_session_plan_cache_size = 0")
		mustExecute(se, "create table tTemp (id int)")
		mustExecute(se, "insert into tTemp values (1)")
		rs, err := se.Execute(ctx, "select * from tTemp where id = 1 or id = 9")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		alloc.Reset()
		rs, err = se.Execute(ctx, "select * from tTemp where id = 1 or id IN (2,5)")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		alloc.Reset()
		mustExecute(se, "drop table tTemp")
		mustExecute(se, "set tidb_session_plan_cache_size = default")
	} else {
		mustExecute(se, "set tidb_enable_non_prepared_plan_cache = 0")
	}
	rs, err := se.Execute(ctx, "explain "+query)
	if err != nil {
		b.Fatal(err)
	}
	// Note: [Batch]PointGet don't use the non-prepared plan cache,
	// since it is already using the FastPlan!
	expectHits := enablePlanCache && (expectedPlan != "Point_Get" && expectedPlan != "Batch_Point_Get")
	checkHits := true
	resStrings, err := session.ResultSetToStringSlice(ctx, se, rs[0])
	if err != nil {
		logutil.BgLogger().Error("error in ResultSetToStrinSlice", zap.String("query", query), zap.Error(err))
	}
	if !strings.HasPrefix(resStrings[0][0], expectedPlan) {
		logutil.BgLogger().Error("expected Point_Get query plan", zap.String("query", query), zap.String("expectedPlan", expectedPlan), zap.Any("explain", resStrings))
		checkHits = false
	}
	hits := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err = se.Execute(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		if checkHits && se.GetSessionVars().FoundInPlanCache {
			hits++
		}
		alloc.Reset()
	}
	b.StopTimer()
	if !expectHits && checkHits && hits > 0 {
		logutil.BgLogger().Error("Not expected Plan Cache to be used with PointGet", zap.Int("hits", hits), zap.Int("b.N", b.N))
	}
	if expectHits && checkHits && hits == 0 && b.N > 5 {
		logutil.BgLogger().Error("expected Plan Cache to be used with PointSelect", zap.Int("hits", hits), zap.Int("b.N", b.N))
	}
}

func preparePointGet(se sessiontypes.Session, partitionBy string) {
	mustExecute(se, "CREATE TABLE t (id int primary key, d varchar(255), key (d)) "+partitionBy)
	mustExecute(se, `insert into t (id) values (1), (8), (5000), (10000), (100000)`)
	mustExecute(se, "analyze table t")
}

func prepareIndexLookup(se sessiontypes.Session, partitionBy string) {
	mustExecute(se, `drop table t`)
	mustExecute(se, "CREATE TABLE t (id int, d varchar(255), key idx_id (id), key(d)) "+partitionBy)
	// Range using: 10, 1000, 100000, maxvalue
	// Create 1k unique index entries, so it is cheaper with index lookup instead of table scan
	mustExecute(se, `insert into t values (1,1),  (5000,5000), (10000,10000), (99900,99900)`)
	mustExecute(se, `insert into t select id + 1, d from t `)   // 8
	mustExecute(se, `insert into t select id + 2, d from t `)   // 16
	mustExecute(se, `insert into t select id + 4, d from t `)   // 32
	mustExecute(se, `insert into t select id + 8, d from t `)   // 64
	mustExecute(se, `insert into t select id + 16, d from t `)  // 128
	mustExecute(se, `insert into t select id + 32, d from t `)  // 256
	mustExecute(se, `insert into t select id + 64, d from t `)  // 512
	mustExecute(se, `insert into t select id + 128, d from t `) // 1024
	mustExecute(se, "analyze table t")
}

func benchmarkPointGetPlanCache(b *testing.B, partitionBy string) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()
	preparePointGet(se, partitionBy)
	pointQuery := "select * from t where id = 1"
	expectedPointPlan := "Point_Get"
	b.Run("PointGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, true)
	})
	b.Run("PointGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, false)
	})
	// IN (...) uses TryFastPlan, which does not use Plan Cache
	batchPointQuery := "select * from t where id = 1 or id = 5000 or id = 2 or id = 100000"
	expectedPlan := "Batch_Point_Get"
	if partitionBy != "" {
		// Batch_Point_Get is not yet enabled for partitioned tables!
		expectedPlan = "TableReader"
	}
	b.Run("BatchPointGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPlan, true)
	})
	b.Run("BatchPointGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPlan, false)
	})

	// Additional tests for IndexScan
	prepareIndexLookup(se, partitionBy)
	expectedPointPlan = "IndexLookUp"
	b.Run("IndexGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, true)
	})
	b.Run("IndexGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, false)
	})
	b.Run("BatchIndexGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPointPlan, true)
	})
	b.Run("BatchIndexGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPointPlan, false)
	})

	// Additional tests for TableScan
	mustExecute(se, "alter table t drop index idx_id")
	mustExecute(se, "analyze table t")
	expectedPointPlan = "TableReader"
	b.Run("TableGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, true)
	})
	b.Run("TableGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, pointQuery, expectedPointPlan, false)
	})
	b.Run("BatchTableGetPlanCacheOn", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPointPlan, true)
	})
	b.Run("BatchTableGetPlanCacheOff", func(b *testing.B) {
		runPointSelect(b, se, batchPointQuery, expectedPointPlan, false)
	})
}

func BenchmarkNonPartitionPointGet(b *testing.B) {
	benchmarkPointGetPlanCache(b, "")
}

func BenchmarkHashPartitionPointGet(b *testing.B) {
	partitionBy := `partition by hash(id) partitions 7`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkHashExprPartitionPointGet(b *testing.B) {
	partitionBy := `partition by hash(floor(id*0.5)) partitions 7`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkKeyPartitionPointGet(b *testing.B) {
	partitionBy := `partition by key(id) partitions 7`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func getListPartitionDef(expr string, useColumns bool) string {
	partitionBy := `partition by list`
	if useColumns {
		partitionBy += ` columns`
	}
	partitionBy += `(` + expr + `) (`
	ranges := []int{1, 5000, 10000, 99900}
	for partID, i := range ranges {
		vals := 256
		partVals := make([]string, 0, vals)
		for j := 0; j < vals; j++ {
			partVals = append(partVals, strconv.Itoa(i+j))
		}
		if expr != "" && i == 1 {
			// for floor(id*0.5)*2
			partVals = append(partVals, "0")
		}
		if partID > 0 {
			partitionBy += ","
		}
		partitionBy += "partition p" + strconv.Itoa(partID) + " values in (" + strings.Join(partVals, ",") + ")"
	}
	partitionBy += ")"
	return partitionBy
}

func BenchmarkListPartitionPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("id", false)
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkListExprPartitionPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("floor(id*0.5)*2", false)
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkRangePartitionPointGet(b *testing.B) {
	partitionBy := `partition by range(id)
(partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkRangeExprPartitionPointGet(b *testing.B) {
	partitionBy := `partition by range(floor(id*0.5))
(partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkRangeColumnPartitionPointGet(b *testing.B) {
	partitionBy := `partition by range columns (id)
(partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkListColumnPartitionPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("id", true)
	benchmarkPointGetPlanCache(b, partitionBy)
}

// TODO: Add benchmarks for {RANGE|LIST} COLUMNS, multi columns!!!

func runPreparedPointSelect(b *testing.B, se sessiontypes.Session, query string, enablePlanCache bool, args ...any) {
	ctx := context.Background()
	if enablePlanCache {
		mustExecute(se, "set tidb_enable_prepared_plan_cache = 1")
	} else {
		mustExecute(se, "set tidb_enable_prepared_plan_cache = 0")
	}
	// TODO: add check for actual EXPLAIN, so the plan is what is expected
	// see runPointSelect
	stmtID, _, _, err := se.PrepareStmt(query)
	if err != nil {
		b.Fatal(err)
	}

	hits := 0
	params := expression.Args2Expressions4Test(args...)
	alloc := chunk.NewAllocator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
		if err != nil {
			b.Fatal(err)
		}
		if se.GetSessionVars().FoundInPlanCache {
			hits++
		}
		_, err = drainRecordSet(ctx, rs, alloc)
		if err != nil {
			b.Fatal(err)
		}
		if enablePlanCache && i > 0 {
			if se.GetSessionVars().FoundInPlanCache {
				hits++
			} else {
				warns := se.GetSessionVars().StmtCtx.GetWarnings()
				logutil.BgLogger().Warn("No plan cache hit", zap.Any("warns", warns))
			}
		}
		alloc.Reset()
	}
	b.StopTimer()
	if enablePlanCache && hits < b.N/2 {
		logutil.BgLogger().Error("Plan cache was not used enough", zap.Int("hits", hits), zap.Int("b.N", b.N))
	}
}

func benchPreparedPointGet(b *testing.B, partitionBy string) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()

	preparePointGet(se, partitionBy)
	pointQuery := "select * from t where id = ?"
	pointArgs := 1
	b.Run("PointGetPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, false, pointArgs)
	})
	b.Run("PointGetPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, true, pointArgs)
	})
	batchPointQuery := "select * from t where id IN (?,?,?)"
	batchArgs := []any{2, 10000, 1}
	b.Run("BatchPointGetPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, false, batchArgs...)
	})
	b.Run("BatchPointGetPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, true, batchArgs...)
	})

	// Additional ones for testing Index
	prepareIndexLookup(se, partitionBy)

	b.Run("IndexPointPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, false, pointArgs)
	})
	b.Run("IndexPointPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, true, pointArgs)
	})
	b.Run("IndexBatchPointPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, false, batchArgs...)
	})
	b.Run("IndexBatchPointPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, true, batchArgs...)
	})
	// Additional ones for testing TableScan

	mustExecute(se, "alter table t drop index idx_id")
	mustExecute(se, "analyze table t")

	b.Run("TableScanPointPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, false, pointArgs)
	})
	b.Run("TableScanPointPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, pointQuery, true, pointArgs)
	})
	b.Run("TableScanBatchPlanCacheOff", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, false, batchArgs...)
	})
	b.Run("TableScanBatchPlanCacheOn", func(b *testing.B) {
		runPreparedPointSelect(b, se, batchPointQuery, true, batchArgs...)
	})
}

func BenchmarkNonPartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "")
}

func BenchmarkHashPartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "partition by hash (id) partitions 7")
}

func BenchmarkHashExprPartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "partition by hash (floor(id*0.5)) partitions 7")
}

func BenchmarkListPartitionPreparedPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("id", false)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkListExprPartitionPreparedPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("floor(id*0.5)*2", false)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkListColumnPartitionPreparedPointGet(b *testing.B) {
	partitionBy := getListPartitionDef("id", true)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkRangePartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "partition by range (id) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))")
}

func BenchmarkRangeColumnPartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "partition by range columns (id) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))")
}

func BenchmarkRangeExprPartitionPreparedPointGet(b *testing.B) {
	benchPreparedPointGet(b, "partition by range (floor(id*0.5)*2) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))")
}

// TODO: Add benchmarks for {RANGE|LIST} COLUMNS, both single and multi columns!!!

func BenchmarkHashPartitionMultiPointSelect(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()

	alloc := chunk.NewAllocator()
	mustExecute(se, `create table t (id int primary key, dt datetime) partition by hash(id) partitions 64;`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where id = 2330")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		rs, err = se.Execute(ctx, "select * from t where id = 1233 or id = 1512")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		rs, err = se.Execute(ctx, "select * from t where id in (117, 1233, 15678)")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, rs[0], alloc)
		if err != nil {
			b.Fatal(err)
		}
		alloc.Reset()
	}
	b.StopTimer()
}
