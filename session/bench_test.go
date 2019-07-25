// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var smallCount = 100
var bigCount = 10000

func prepareBenchSession() (Session, *domain.Domain, kv.Storage) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	domain, err := BootstrapSession(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	log.SetLevel(zapcore.ErrorLevel)
	se, err := CreateSession4Test(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	mustExecute(se, "use test")
	return se, domain, store
}

func prepareBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s, index idx (col))", colType))
	mustExecute(se, "begin")
	for i := 0; i < valueCount; i++ {
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, i)+")")
	}
	mustExecute(se, "commit")
}

func prepareSortBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s)", colType))
	mustExecute(se, "begin")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < valueCount; i++ {
		if i%1000 == 0 {
			mustExecute(se, "commit")
			mustExecute(se, "begin")
		}
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, r.Intn(valueCount))+")")
	}
	mustExecute(se, "commit")
}

func prepareJoinBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s)", colType))
	mustExecute(se, "begin")
	for i := 0; i < valueCount; i++ {
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, i)+")")
	}
	mustExecute(se, "commit")
}

func readResult(ctx context.Context, rs sqlexec.RecordSet, count int) {
	req := rs.NewChunk()
	for count > 0 {
		err := rs.Next(ctx, req)
		if err != nil {
			logutil.Logger(ctx).Fatal("read result failed", zap.Error(err))
		}
		if req.NumRows() == 0 {
			logutil.Logger(ctx).Fatal(strconv.Itoa(count))
		}
		count -= req.NumRows()
	}
	rs.Close()
}

func BenchmarkBasic(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select 1")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkTableScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkExplainTableScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkTableLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%d", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where pk = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkExplainTableLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%d", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t where pk = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkStringIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col > 'hello'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkExplainStringIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t where col > 'hello'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkStringIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 'hello 64'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkIntegerIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col >= 0")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkIntegerIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkDecimalIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "decimal(32,6)", "%v.1234", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col >= 0")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkDecimalIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "decimal(32,6)", "%v.1234", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 64.1234")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkInsertWithIndex(b *testing.B) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	mustExecute(se, "drop table if exists t")
	mustExecute(se, "create table t (pk int primary key, col int, index idx (col))")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecute(se, fmt.Sprintf("insert t values (%d, %d)", i, i))
	}
	b.StopTimer()
}

func BenchmarkInsertNoIndex(b *testing.B) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	mustExecute(se, "drop table if exists t")
	mustExecute(se, "create table t (pk int primary key, col int)")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecute(se, fmt.Sprintf("insert t values (%d, %d)", i, i))
	}
	b.StopTimer()
}

func BenchmarkSort(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareSortBenchData(se, "int", "%v", bigCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t order by col limit 50")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 50)
	}
	b.StopTimer()
}

func BenchmarkJoin(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareJoinBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t a join t b on a.col = b.col")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkJoinLimit(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareJoinBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t a join t b on a.col = b.col limit 1")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}
