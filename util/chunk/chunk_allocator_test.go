package chunk

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func prepareChunkType() ([]int, []*types.FieldType) {
	return []int{32, 64, 128, 256, 512, 1024, 4096}, []*types.FieldType{
		{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
			Charset: charset.CharsetBin,
			Collate: charset.CollationBin,
		},
	}
}

func BenchmarkNewChunkWithAllocator(b *testing.B) {
	bufSize, colTypes := prepareChunkType()
	m := NewMultiBufAllocator(8, 15, 32)
	runtime.GC()
	b.ResetTimer()
	var k uint32
	for i := 0; i < b.N; i++ {
		c := bufSize[int(atomic.AddUint32(&k, 1))%len(bufSize)]
		chk := NewChunkWithAllocator(m, colTypes, c, c)
		chk.Release()
	}
}

func BenchmarkNewChunk(b *testing.B) {
	bufSize, colTypes := prepareChunkType()
	runtime.GC()
	b.ResetTimer()
	var k uint32
	for i := 0; i < b.N; i++ {
		c := bufSize[int(atomic.AddUint32(&k, 1))%len(bufSize)]
		chk := New(colTypes, c, c)
		chk.Release()
	}
}

func TestCapIndex(t *testing.T) {
	for i := 1; i < len(allocIndex); i++ {
		b := allocIndex[i]
		if !(i <= (1<<uint(b)) && i > (1<<uint(b-1))) {
			t.Fatal("alloc", i)
		}
	}
	for i := 1; i < len(freeIndex); i++ {
		b := freeIndex[i]
		if !(i >= (1<<uint(b)) && i < (1<<uint(b+1))) {
			t.Fatal("free", i)
		}
	}
}
