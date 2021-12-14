package functional_dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/tools/container/intsets"
)

func TestAddStrictFunctionalDependency(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   NewFastIntSet(1), // AB -> CDEFG
		to:     NewFastIntSet(3, 4, 5, 6, 7),
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> CD
		to:     NewFastIntSet(3, 4),
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> EF
		to:     NewFastIntSet(5, 6),
		strict: true,
		equiv:  false,
	}
	// fd: AB -> CDEFG implies all of others.
	assertF := func() {
		ass.Equal(len(fd.fdEdges), 1)
		from := fd.fdEdges[0].from.SortedArray()
		ass.Equal(len(from), 1)
		ass.Equal(from[0], 1)
		to := fd.fdEdges[0].to.SortedArray()
		ass.Equal(len(to), 5)
		ass.Equal(to[0], 3)
		ass.Equal(to[1], 4)
		ass.Equal(to[2], 5)
		ass.Equal(to[3], 6)
		ass.Equal(to[4], 7)
	}
	fd.AddStrictFunctionalDependency(fe1.from, fe1.to)
	fd.AddStrictFunctionalDependency(fe2.from, fe2.to)
	fd.AddStrictFunctionalDependency(fe3.from, fe3.to)
	assertF()

	fd.fdEdges = fd.fdEdges[:0]
	fd.AddStrictFunctionalDependency(fe2.from, fe2.to)
	fd.AddStrictFunctionalDependency(fe1.from, fe1.to)
	fd.AddStrictFunctionalDependency(fe3.from, fe3.to)
	assertF()

	// TODO:
	// test reduce col
	// test more edges
}

// Preface Notice:
// For test convenience, we add fdEdge to fdSet directly which is not valid in the procedure.
// Because two difference fdEdge in the fdSet may imply each other which is strictly not permitted in the procedure.
// Use `AddStrictFunctionalDependency` to add the fdEdge to the fdSet in the formal way .
func TestFDSet_ClosureOf(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> CD
		to:     NewFastIntSet(3, 4),
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> EF
		to:     NewFastIntSet(5, 6),
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   NewFastIntSet(2), // B -> FG
		to:     NewFastIntSet(6, 7),
		strict: true,
		equiv:  false,
	}
	fe4 := &fdEdge{
		from:   NewFastIntSet(1), // A -> DEH
		to:     NewFastIntSet(4, 5, 8),
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	// A -> ADEH
	closure := fd.closureOf(NewFastIntSet(1)).SortedArray()
	ass.Equal(len(closure), 4)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1], 4)
	ass.Equal(closure[2], 5)
	ass.Equal(closure[3], 8)
	// AB -> ABCDEFGH
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	closure = fd.closureOf(NewFastIntSet(1, 2)).SortedArray()
	ass.Equal(len(closure), 8)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1], 2)
	ass.Equal(closure[2], 3)
	ass.Equal(closure[3], 4)
	ass.Equal(closure[4], 5)
	ass.Equal(closure[5], 6)
	ass.Equal(closure[6], 7)
	ass.Equal(closure[7], 8)
}

func TestFDSet_ReduceCols(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   NewFastIntSet(1), // A -> CD
		to:     NewFastIntSet(3, 4),
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   NewFastIntSet(3), // C -> DE
		to:     NewFastIntSet(4, 5),
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   NewFastIntSet(3, 5), // CE -> B
		to:     NewFastIntSet(2),
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3)
	res := fd.ReduceCols(NewFastIntSet(1, 2)).SortedArray()
	ass.Equal(len(res), 1)
	ass.Equal(res[0], 1)
}

func TestFDSet_InClosure(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> CD
		to:     NewFastIntSet(3, 4),
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   NewFastIntSet(1, 2), // AB -> EF
		to:     NewFastIntSet(5, 6),
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   NewFastIntSet(2), // B -> FG
		to:     NewFastIntSet(6, 7),
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3)
	// A -> F : false (determinants should not be torn apart)
	ass.False(fd.inClosure(NewFastIntSet(1), NewFastIntSet(6)))
	// B -> G : true (dependency can be torn apart)
	ass.True(fd.inClosure(NewFastIntSet(2), NewFastIntSet(7)))
	// AB -> E : true (dependency can be torn apart)
	ass.True(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(5)))
	// AB -> FG: true (in closure node set)
	ass.True(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(6, 7)))
	// AB -> DF: true (in closure node set)
	ass.True(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(4, 6)))
	// AB -> EG: true (in closure node set)
	ass.True(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7)))
	// AB -> EGH: false (H is not in closure node set)
	ass.False(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7, 8)))

	fe4 := &fdEdge{
		from:   NewFastIntSet(2), // B -> CH
		to:     NewFastIntSet(3, 8),
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe4)
	// AB -> EGH: true (in closure node set)
	ass.True(fd.inClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7, 8)))
}

func BenchmarkMapIntSet_Difference(b *testing.B) {
	intSetA := NewIntSet()
	for i := 0; i < 200000; i++ {
		intSetA[i] = struct{}{}
	}
	intSetB := NewIntSet()
	for i := 100000; i < 300000; i++ {
		intSetB[i] = struct{}{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := NewIntSet()
		tmp.Difference2(intSetA, intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkIntSet_Difference(b *testing.B) {
	intSetA := &intsets.Sparse{}
	for i := 0; i < 200000; i++ {
		intSetA.Insert(i)
	}
	intSetB := &intsets.Sparse{}
	for i := 100000; i < 300000; i++ {
		intSetA.Insert(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := &intsets.Sparse{}
		tmp.Difference(intSetA, intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkFastIntSet_Difference(b *testing.B) {
	intSetA := NewFastIntSet()
	for i := 0; i < 200000; i++ {
		intSetA.Insert(i)
	}
	intSetB := NewFastIntSet()
	for i := 100000; i < 300000; i++ {
		intSetA.Insert(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSetA.Difference(intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkIntSet_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := NewIntSet()
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

func BenchmarkSparse_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := &intsets.Sparse{}
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

func BenchmarkFastIntSet_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := NewFastIntSet()
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

// BenchMarkResult
//
// Test with Difference (traverse and allocation) (size means the intersection size)
// +--------------------------------------------------------------------------------+
// |     size   |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +------------+-----------------+------------------------+------------------------+
// |       64   |     3203 ns/op  |         64 ns/op       |        5.750 ns/op     |
// |     1000   |   244284 ns/op  |        822 ns/op       |        919.8 ns/op     |
// |    10000   |  2940130 ns/op  |       8071 ns/op       |         8686 ns/op     |
// |   100000   | 41283606 ns/op  |      83320 ns/op       |        85563 ns/op     |
// +------------+-----------------+------------------------+------------------------+
//
// This test is under same operation with same data with following analysis:
// MapInt and Sparse are all temporarily allocated for intermediate result. Since
// MapInt need to reallocate the capacity with unit as int(64) which is expensive
// than a bit of occupation in Sparse reallocation.
//
// From the space utilization and allocation times, here in favour of sparse.
//
// Test with Insert (allocation)
// +----------------------------------------------------------------------------+
// |  size  |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +--------+-----------------+------------------------+------------------------+
// |     64 |     5705 ns/op  |         580 ns/op      |           234 ns/op    |
// |   1000 |   122906 ns/op  |        7991 ns/op      |         10606 ns/op    |
// |  10000 |   845961 ns/op  |      281134 ns/op      |        303006 ns/op    |
// | 100000 | 15529859 ns/op  |    31273573 ns/op      |      30752177 ns/op    |
// +--------------------------+------------------------+------------------------+
//
// From insert, map insert take much time than sparse does when insert size is under
// 100 thousand. While when the size grows bigger, map insert take less time to do that
// (maybe cost more memory usage), that because sparse need to traverse the chain to
// find the suitable block to insert.
//
// From insert, if set size is larger than 100 thousand, map-set is preferred, otherwise, sparse is good.
//
// Test with Subset (traverse) (sizes means the size A / B)
// +---------------------------------------------------------------------------------+
// |  size  |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +--------+-----------------+------------------------+------------------------+
// |     64 |    59.47 ns/op  |       3.775 ns/op      |        2.727 ns/op     |
// |   1000 |    68.9 ns/op   |       3.561 ns/op      |        22.64 ns/op     |
// |  20000 |   104.7 ns/op   |       3.502 ns/op      |        23.92 ns/op     |
// | 200000 |   249.8 ns/op   |       3.504 ns/op      |        22.11 ns/op     |
// +--------------------------+------------------------+------------------------+
//
// This is the most amazing part we have been tested. Map set need to compute the equality
// about the every key int in the map with others. While sparse only need to do is just to
// fetch every bitmap (every bit implies a number) and to the bit operation together.
//
// FastIntSet's performance is quite like Sparse IntSet, because they have the same implementation
// inside. While FastIntSet have some optimizations with small range (0-63), so we add an
// extra test for size with 64, from the number above, they did have some effects, especially
// in computing the difference (bit | technically).
//
// From all above, we are in favour of sparse. sparse to store fast-int-set instead of using map.
