package chunk

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"testing"
)

var manyField = []*types.FieldType{
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
	{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong},
}

func BenchmarkNormalNewWideChunk(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chk := New(manyField, 1, 1)
		_ = chk
	}
}

func BenchmarkFastNewWideChunk(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chk := WideNew(manyField, 1, 1)
		_ = chk
	}
}
