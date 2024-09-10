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
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	pointQuery         = "select * from t where id = 1"
	pointQueryPrepared = "select * from t where id = ?"
	expectedPointPlan  = "Point_Get"
	// IN (...) uses TryFastPlan, which does not use Plan Cache
	batchPointQuery             = "select * from t where id = 1 or id = 5000 or id = 2 or id = 100000"
	batchPointQueryPrepared     = "select * from t where id IN (?,?,?)"
	expectedBatchPointPlan      = "Batch_Point_Get"
	expectedIndexPlan           = "IndexLookUp"
	expectedTableScanPlan       = "TableReader"
	partitionByHash             = `partition by hash(id) partitions 7`
	partitionByHashExpr         = `partition by hash(floor(id*0.5)) partitions 7`
	partitionByKey              = `partition by key(id) partitions 7`
	partitionByRange            = `partition by range(id) (partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	partitionByRangeExpr        = `partition by range(floor(id*0.5)) (partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	partitionByRangeColumns     = `partition by range columns (id) (partition p0 values less than (10), partition p1 values less than (1000), partition p3 values less than (100000), partition pMax values less than (maxvalue))`
	pointArgs                   = 1
	batchArgs                   = []any{2, 10000, 1}
	partitionByRangePrep        = "partition by range (id) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))"
	partitionByRangeExprPrep    = "partition by range (floor(id*0.5)*2) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))"
	partitionByRangeColumnsPrep = "partition by range columns (id) (partition p0 values less than (10), partition p1 values less than (63), partition p3 values less than (100), partition pMax values less than (maxvalue))"
)

type accessType int

const (
	pointGet accessType = iota
	indexLookup
	tableScan
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
		logutil.BgLogger().Error("expected other query plan", zap.String("query", query), zap.String("expectedPlan", expectedPlan), zap.Any("explain", resStrings))
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
	mustExecute(se, `drop table if exists t`)
	mustExecute(se, "CREATE TABLE t (id int primary key, d varchar(255), key (d)) "+partitionBy)
	mustExecute(se, `insert into t (id) values (1), (8), (5000), (10000), (100000)`)
	mustExecute(se, "analyze table t")
}

func insert1kRows(se sessiontypes.Session) {
	// Create 1k unique index entries, so it is cheaper with index lookup instead of table scan
	// Range using: 10, 5000, 10000, 1000000
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

func prepareIndexLookup(se sessiontypes.Session, partitionBy string) {
	mustExecute(se, `drop table if exists t`)
	mustExecute(se, "CREATE TABLE t (id int, d varchar(255), key idx_id (id), key(d)) "+partitionBy)
	insert1kRows(se)
}

func prepareTableScan(se sessiontypes.Session, partitionBy string) {
	mustExecute(se, `drop table if exists t`)
	mustExecute(se, "CREATE TABLE t (id int, d varchar(255), key(d)) "+partitionBy)
	insert1kRows(se)
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

func runBenchmark(b *testing.B, partitionBy, query, expectedPointPlan string, access accessType, enablePlanCache bool) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()
	switch access {
	case pointGet:
		preparePointGet(se, partitionBy)
	case indexLookup:
		prepareIndexLookup(se, partitionBy)
	case tableScan:
		prepareTableScan(se, partitionBy)
	}
	runPointSelect(b, se, query, expectedPointPlan, enablePlanCache)
}

func BenchmarkNonPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkNonPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkNonPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedBatchPointPlan, pointGet, true)
}

func BenchmarkNonPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedBatchPointPlan, pointGet, false)
}

func BenchmarkNonPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkNonPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkNonPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkNonPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkNonPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkNonPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkNonPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkNonPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, "", batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkNonPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, "")
}

func BenchmarkHashPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkHashPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkHashPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkHashPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkHashPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkHashPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkHashPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkHashPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkHashPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkHashPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkHashPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkHashPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHash, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkHashPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByHash)
}

func BenchmarkHashExprPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkHashExprPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkHashExprPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkHashExprPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkHashExprPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkHashExprPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkHashExprPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkHashExprPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkHashExprPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkHashExprPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByHashExpr, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkHashExprPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByHashExpr)
}

func BenchmarkKeyPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkKeyPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkKeyPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkKeyPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkKeyPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkKeyPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkKeyPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkKeyPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkKeyPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkKeyPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkKeyPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkKeyPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByKey, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkKeyPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByKey)
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

func BenchmarkListPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkListPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkListPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkListPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkListPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkListPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkListPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, getListPartitionDef("id", false))
}

func BenchmarkListExprPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkListExprPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkListExprPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkListExprPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkListExprPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListExprPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkListExprPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListExprPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkListExprPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListExprPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListExprPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListExprPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListExprPartition(b *testing.B) {
	partitionBy := getListPartitionDef("floor(id*0.5)*2", false)
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkListColumnsPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkListColumnsPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkListColumnsPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkListColumnsPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkListColumnsPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListColumnsPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkListColumnsPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListColumnsPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListColumnsPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkListColumnsPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkListColumnsPartition(b *testing.B) {
	partitionBy := getListPartitionDef("id", true)
	benchmarkPointGetPlanCache(b, partitionBy)
}

func BenchmarkRangePartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkRangePartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkRangePartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkRangePartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkRangePartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangePartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkRangePartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangePartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkRangePartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangePartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangePartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangePartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRange, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangePartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByRange)
}

func BenchmarkRangeExprPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkRangeExprPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkRangeExprPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkRangeExprPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkRangeExprPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangeExprPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkRangeExprPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangeExprPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangeExprPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangeExprPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeExpr, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangeExprPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByRangeExpr)
}

func BenchmarkRangeColumnsPartitionPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedPointPlan, pointGet, true)
}

func BenchmarkRangeColumnsPartitionPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedPointPlan, pointGet, false)
}

func BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedTableScanPlan, pointGet, true)
}

func BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedTableScanPlan, pointGet, false)
}

func BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedIndexPlan, indexLookup, false)
}

func BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedIndexPlan, indexLookup, true)
}

func BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedIndexPlan, indexLookup, false)
}
func BenchmarkRangeColumnsPartitionTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangeColumnsPartitionTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, pointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedTableScanPlan, tableScan, true)
}

func BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmark(b, partitionByRangeColumns, batchPointQuery, expectedTableScanPlan, tableScan, false)
}

func BenchmarkRangeColumnsPartition(b *testing.B) {
	benchmarkPointGetPlanCache(b, partitionByRangeColumns)
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

func runBenchmarkPrepared(b *testing.B, partitionBy, query string, access accessType, enablePlanCache bool, qArgs ...any) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()
	switch access {
	case pointGet:
		preparePointGet(se, partitionBy)
	case indexLookup:
		prepareIndexLookup(se, partitionBy)
	case tableScan:
		prepareTableScan(se, partitionBy)
	}
	runPreparedPointSelect(b, se, query, enablePlanCache, qArgs...)
}

func BenchmarkNonPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkNonPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkNonPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkNonPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkNonPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkNonPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, "", batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkNonPartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, "")
}

func BenchmarkHashPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkHashPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkHashPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkHashPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkHashPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkHashPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHash, batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkHashPartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, partitionByHash)
}

func BenchmarkHashExprPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkHashExprPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkHashExprPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkHashExprPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByHashExpr, batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkHashExprPartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, partitionByHashExpr)
}

func BenchmarkListPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkListPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkListPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkListPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkListPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkListPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkListPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkListPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkListPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkListPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", false), batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkListPartitionPrepared(b *testing.B) {
	partitionBy := getListPartitionDef("id", false)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkListExprPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkListExprPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkListExprPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkListExprPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkListExprPartitionPrepared(b *testing.B) {
	partitionBy := getListPartitionDef("floor(id*0.5)*2", false)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, getListPartitionDef("id", true), batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkListColumnPartitionPrepared(b *testing.B) {
	partitionBy := getListPartitionDef("id", true)
	benchPreparedPointGet(b, partitionBy)
}

func BenchmarkRangePartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkRangePartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkRangePartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkRangePartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkRangePartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkRangePartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangePrep, batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkRangePartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, partitionByRangePrep)
}

func BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeExprPrep, batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkRangeExprPartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, partitionByRangeExprPrep)
}

func BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, pointGet, true, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, pointGet, false, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, pointGet, true, batchArgs...)
}

func BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, pointGet, false, batchArgs...)
}

func BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, indexLookup, true, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, indexLookup, false, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, indexLookup, true, batchArgs...)
}

func BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, indexLookup, false, batchArgs...)
}
func BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, tableScan, true, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, pointQueryPrepared, tableScan, false, pointArgs)
}

func BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOn(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, tableScan, true, batchArgs...)
}

func BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOff(b *testing.B) {
	runBenchmarkPrepared(b, partitionByRangeColumnsPrep, batchPointQueryPrepared, tableScan, false, batchArgs...)
}

func BenchmarkRangeColumnPartitionPrepared(b *testing.B) {
	benchPreparedPointGet(b, partitionByRangeColumnsPrep)
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
	mustExecute(se, `create table t (id int primary key, dt datetime) partition by hash(id) partitions 64`)
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

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkNonPartitionPointGetPlanCacheOn,
		//BenchmarkNonPartitionPointGetPlanCacheOff,
		BenchmarkNonPartitionBatchPointGetPlanCacheOn,
		//BenchmarkNonPartitionBatchPointGetPlanCacheOff,
		BenchmarkNonPartitionIndexLookupPlanCacheOn,
		//BenchmarkNonPartitionIndexLookupPlanCacheOff,
		BenchmarkNonPartitionBatchIndexLookupPlanCacheOn,
		//BenchmarkNonPartitionBatchIndexLookupPlanCacheOff,
		//BenchmarkNonPartitionTableScanPlanCacheOn,
		//BenchmarkNonPartitionTableScanPlanCacheOff,
		//BenchmarkNonPartitionBatchTableScanPlanCacheOn,
		//BenchmarkNonPartitionBatchTableScanPlanCacheOff,
		BenchmarkHashPartitionPointGetPlanCacheOn,
		//BenchmarkHashPartitionPointGetPlanCacheOff,
		BenchmarkHashPartitionBatchPointGetPlanCacheOn,
		//BenchmarkHashPartitionBatchPointGetPlanCacheOff,
		BenchmarkHashPartitionIndexLookupPlanCacheOn,
		//BenchmarkHashPartitionIndexLookupPlanCacheOff,
		BenchmarkHashPartitionBatchIndexLookupPlanCacheOn,
		//BenchmarkHashPartitionBatchIndexLookupPlanCacheOff,
		//BenchmarkHashPartitionTableScanPlanCacheOn,
		//BenchmarkHashPartitionTableScanPlanCacheOff,
		//BenchmarkHashPartitionBatchTableScanPlanCacheOn,
		//BenchmarkHashPartitionBatchTableScanPlanCacheOff,
		/*
			BenchmarkHashExprPartitionPointGetPlanCacheOn,
			BenchmarkHashExprPartitionPointGetPlanCacheOff,
			BenchmarkHashExprPartitionBatchPointGetPlanCacheOn,
			BenchmarkHashExprPartitionBatchPointGetPlanCacheOff,
			BenchmarkHashExprPartitionIndexLookupPlanCacheOn,
			BenchmarkHashExprPartitionIndexLookupPlanCacheOff,
			BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkHashExprPartitionTableScanPlanCacheOn,
			BenchmarkHashExprPartitionTableScanPlanCacheOff,
			BenchmarkHashExprPartitionBatchTableScanPlanCacheOn,
			BenchmarkHashExprPartitionBatchTableScanPlanCacheOff,
			BenchmarkKeyPartitionPointGetPlanCacheOn,
			BenchmarkKeyPartitionPointGetPlanCacheOff,
			BenchmarkKeyPartitionBatchPointGetPlanCacheOn,
			BenchmarkKeyPartitionBatchPointGetPlanCacheOff,
			BenchmarkKeyPartitionIndexLookupPlanCacheOn,
			BenchmarkKeyPartitionIndexLookupPlanCacheOff,
			BenchmarkKeyPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkKeyPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkKeyPartitionTableScanPlanCacheOn,
			BenchmarkKeyPartitionTableScanPlanCacheOff,
			BenchmarkKeyPartitionBatchTableScanPlanCacheOn,
			BenchmarkKeyPartitionBatchTableScanPlanCacheOff,
			BenchmarkListPartitionPointGetPlanCacheOn,
			BenchmarkListPartitionPointGetPlanCacheOff,
			BenchmarkListPartitionBatchPointGetPlanCacheOn,
			BenchmarkListPartitionBatchPointGetPlanCacheOff,
			BenchmarkListPartitionIndexLookupPlanCacheOn,
			BenchmarkListPartitionIndexLookupPlanCacheOff,
			BenchmarkListPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkListPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkListPartitionTableScanPlanCacheOn,
			BenchmarkListPartitionTableScanPlanCacheOff,
			BenchmarkListPartitionBatchTableScanPlanCacheOn,
			BenchmarkListPartitionBatchTableScanPlanCacheOff,
			BenchmarkListExprPartitionPointGetPlanCacheOn,
			BenchmarkListExprPartitionPointGetPlanCacheOff,
			BenchmarkListExprPartitionBatchPointGetPlanCacheOn,
			BenchmarkListExprPartitionBatchPointGetPlanCacheOff,
			BenchmarkListExprPartitionIndexLookupPlanCacheOn,
			BenchmarkListExprPartitionIndexLookupPlanCacheOff,
			BenchmarkListExprPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkListExprPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkListExprPartitionTableScanPlanCacheOn,
			BenchmarkListExprPartitionTableScanPlanCacheOff,
			BenchmarkListExprPartitionBatchTableScanPlanCacheOn,
			BenchmarkListExprPartitionBatchTableScanPlanCacheOff,
			BenchmarkListColumnsPartitionPointGetPlanCacheOn,
			BenchmarkListColumnsPartitionPointGetPlanCacheOff,
			BenchmarkListColumnsPartitionBatchPointGetPlanCacheOn,
			BenchmarkListColumnsPartitionBatchPointGetPlanCacheOff,
			BenchmarkListColumnsPartitionIndexLookupPlanCacheOn,
			BenchmarkListColumnsPartitionIndexLookupPlanCacheOff,
			BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkListColumnsPartitionTableScanPlanCacheOn,
			BenchmarkListColumnsPartitionTableScanPlanCacheOff,
			BenchmarkListColumnsPartitionBatchTableScanPlanCacheOn,
			BenchmarkListColumnsPartitionBatchTableScanPlanCacheOff,
		*/
		BenchmarkRangePartitionPointGetPlanCacheOn,
		//BenchmarkRangePartitionPointGetPlanCacheOff,
		BenchmarkRangePartitionBatchPointGetPlanCacheOn,
		//BenchmarkRangePartitionBatchPointGetPlanCacheOff,
		BenchmarkRangePartitionIndexLookupPlanCacheOn,
		//BenchmarkRangePartitionIndexLookupPlanCacheOff,
		BenchmarkRangePartitionBatchIndexLookupPlanCacheOn,
		//BenchmarkRangePartitionBatchIndexLookupPlanCacheOff,
		//BenchmarkRangePartitionTableScanPlanCacheOn,
		//BenchmarkRangePartitionTableScanPlanCacheOff,
		//BenchmarkRangePartitionBatchTableScanPlanCacheOn,
		//BenchmarkRangePartitionBatchTableScanPlanCacheOff,
		/*
			BenchmarkRangeExprPartitionPointGetPlanCacheOn,
			BenchmarkRangeExprPartitionPointGetPlanCacheOff,
			BenchmarkRangeExprPartitionBatchPointGetPlanCacheOn,
			BenchmarkRangeExprPartitionBatchPointGetPlanCacheOff,
			BenchmarkRangeExprPartitionIndexLookupPlanCacheOn,
			BenchmarkRangeExprPartitionIndexLookupPlanCacheOff,
			BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkRangeExprPartitionTableScanPlanCacheOn,
			BenchmarkRangeExprPartitionTableScanPlanCacheOff,
			BenchmarkRangeExprPartitionBatchTableScanPlanCacheOn,
			BenchmarkRangeExprPartitionBatchTableScanPlanCacheOff,
			BenchmarkRangeColumnsPartitionPointGetPlanCacheOn,
			BenchmarkRangeColumnsPartitionPointGetPlanCacheOff,
			BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOn,
			BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOff,
			BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOn,
			BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOff,
			BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOn,
			BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOff,
			BenchmarkRangeColumnsPartitionTableScanPlanCacheOn,
			BenchmarkRangeColumnsPartitionTableScanPlanCacheOff,
			BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOn,
			BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOff,
			BenchmarkNonPartitionPreparedPointGetPlanCacheOn,
			BenchmarkNonPartitionPreparedPointGetPlanCacheOff,
			BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkNonPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkNonPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkNonPartitionPreparedTableScanPlanCacheOn,
			BenchmarkNonPartitionPreparedTableScanPlanCacheOff,
			BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkHashPartitionPreparedPointGetPlanCacheOn,
			BenchmarkHashPartitionPreparedPointGetPlanCacheOff,
			BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkHashPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkHashPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkHashPartitionPreparedTableScanPlanCacheOn,
			BenchmarkHashPartitionPreparedTableScanPlanCacheOff,
			BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkHashExprPartitionPreparedPointGetPlanCacheOn,
			BenchmarkHashExprPartitionPreparedPointGetPlanCacheOff,
			BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkHashExprPartitionPreparedTableScanPlanCacheOn,
			BenchmarkHashExprPartitionPreparedTableScanPlanCacheOff,
			BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkListPartitionPreparedPointGetPlanCacheOn,
			BenchmarkListPartitionPreparedPointGetPlanCacheOff,
			BenchmarkListPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkListPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkListPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkListPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkListPartitionPreparedTableScanPlanCacheOn,
			BenchmarkListPartitionPreparedTableScanPlanCacheOff,
			BenchmarkListPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkListPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkListExprPartitionPreparedPointGetPlanCacheOn,
			BenchmarkListExprPartitionPreparedPointGetPlanCacheOff,
			BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkListExprPartitionPreparedTableScanPlanCacheOn,
			BenchmarkListExprPartitionPreparedTableScanPlanCacheOff,
			BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOff,
			BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkRangePartitionPreparedPointGetPlanCacheOn,
			BenchmarkRangePartitionPreparedPointGetPlanCacheOff,
			BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkRangePartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkRangePartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkRangePartitionPreparedTableScanPlanCacheOn,
			BenchmarkRangePartitionPreparedTableScanPlanCacheOff,
			BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOff,
			BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOff,
			BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOn,
			BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOff,
		*/
	)
}
