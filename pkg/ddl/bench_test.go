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

package ddl_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func BenchmarkExtractDatumByOffsets(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")
	copCtx, err := copr.NewCopContextSingleIndex(tblInfo, idxInfo, tk.Session(), "")
	require.NoError(b, err)
	startKey := tbl.RecordPrefix()
	endKey := startKey.PrefixNext()
	txn, err := store.Begin()
	require.NoError(b, err)
	copChunk := ddl.FetchChunk4Test(copCtx, tbl.(table.PhysicalTable), startKey, endKey, store, 10)
	require.NoError(b, err)
	require.NoError(b, txn.Rollback())

	handleDataBuf := make([]types.Datum, len(copCtx.GetBase().HandleOutputOffsets))

	iter := chunk.NewIterator4Chunk(copChunk)
	row := iter.Begin()
	c := copCtx.GetBase()
	offsets := copCtx.IndexColumnOutputOffsets(idxInfo.ID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ddl.ExtractDatumByOffsetsForTest(row, offsets, c.ExprColumnInfos, handleDataBuf)
	}
}

type myint int64

type Inccer interface {
	inc()
}

func (i *myint) inc() {
	*i = *i + 1
}

func BenchmarkIntmethod(b *testing.B) {
	i := new(myint)
	incnIntmethod(i, b.N)
}

func BenchmarkInterface(b *testing.B) {
	i := new(myint)
	incnInterface(i, b.N)
}

func BenchmarkTypeSwitch(b *testing.B) {
	i := new(myint)
	incnSwitch(i, b.N)
}

func BenchmarkTypeAssertion(b *testing.B) {
	i := new(myint)
	incnAssertion(i, b.N)
}

func incnIntmethod(i *myint, n int) {
	for k := 0; k < n; k++ {
		i.inc()
	}
}

func incnInterface(any Inccer, n int) {
	for k := 0; k < n; k++ {
		any.inc()
	}
}

func incnSwitch(any Inccer, n int) {
	for k := 0; k < n; k++ {
		switch v := any.(type) {
		case *myint:
			v.inc()
		}
	}
}

func incnAssertion(any Inccer, n int) {
	for k := 0; k < n; k++ {
		if newint, ok := any.(*myint); ok {
			newint.inc()
		}
	}
}

func add(a int64) int64 {
	return a + 1
}
func add2(a int64) int64 {
	return a + 2
}

func addSwitch(value int64, a int64) int64 {
	switch value {
	case 1:
		return a + 1
	case 0:
		return a + 2
	case 2:
		return a + 3
	default:
		panic("not supported")

	}
}
func addGeneric[T any](value T, a int64) int64 {
	switch any(value).(type) {
	case int64:
		return a + 1
	case float64:
		return a + 2
	case int8:
		return a + 3
	case int16:
		return a + 4
	default:
		panic("not supported")
	}
}

func BenchmarkTest1(b *testing.B) {
	sum := int64(0)
	b.ResetTimer()
	//a := int64(rand.Int())
	for i := 0; i < 1000000000; i++ {
		//if (a)%2 == 1 {
		sum += add(int64(i))
		//} else {
		//	sum += add2(int64(i))
		//}
	}
	fmt.Println(sum)
	//} else {
	//	for i := 0; i < 1000000000; i++ {
	//		sum += add2(int64(i))
	//	}
	//}
}
func BenchmarkTest(b *testing.B) {
	sum := int64(0)
	b.ResetTimer()
	a := rand.Int()
	aaa := int64(a % 3)
	for i := 0; i < 1000000000; i++ {
		sum += addSwitch(aaa, int64(i))
		//sum += addGeneric(int64(1), int64(i))
	}
	/*
		if a%3 == 1 {
			for i := 0; i < 1000000000; i++ {
				sum += addSwitch(1, int64(i))
				//sum += addGeneric(int64(1), int64(i))
			}
		} else if a%3 == 2 {
			for i := 0; i < 1000000000; i++ {
				sum += addSwitch(2, int64(i))
				//sum += addGeneric(float64(0), int64(i))
			}
		} else {
			for i := 0; i < 1000000000; i++ {
				sum += addSwitch(0, int64(i))
				//sum += addGeneric(int8(0), int64(i))
			}
		}
	*/
	fmt.Println(sum)
}

func BenchmarkGenerateIndexKV(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")

	index := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sctx := tk.Session().GetSessionVars().StmtCtx
	idxDt := []types.Datum{types.NewIntDatum(10)}
	buf := make([]byte, 0, 64)
	handle := kv.IntHandle(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		iter := index.GenIndexKVIter(sctx.ErrCtx(), sctx.TimeZone(), idxDt, handle, nil)
		_, _, _, err = iter.Next(buf, nil)
		if err != nil {
			break
		}
	}
	require.NoError(b, err)
}
