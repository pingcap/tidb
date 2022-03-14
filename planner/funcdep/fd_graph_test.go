package funcdep

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	closure := fd.closureOfStrict(NewFastIntSet(1)).SortedArray()
	ass.Equal(len(closure), 4)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1], 4)
	ass.Equal(closure[2], 5)
	ass.Equal(closure[3], 8)
	// AB -> ABCDEFGH
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	closure = fd.closureOfStrict(NewFastIntSet(1, 2)).SortedArray()
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
	ass.False(fd.InClosure(NewFastIntSet(1), NewFastIntSet(6)))
	// B -> G : true (dependency can be torn apart)
	ass.True(fd.InClosure(NewFastIntSet(2), NewFastIntSet(7)))
	// AB -> E : true (dependency can be torn apart)
	ass.True(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(5)))
	// AB -> FG: true (in closure node set)
	ass.True(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(6, 7)))
	// AB -> DF: true (in closure node set)
	ass.True(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(4, 6)))
	// AB -> EG: true (in closure node set)
	ass.True(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7)))
	// AB -> EGH: false (H is not in closure node set)
	ass.False(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7, 8)))

	fe4 := &fdEdge{
		from:   NewFastIntSet(2), // B -> CH
		to:     NewFastIntSet(3, 8),
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe4)
	// AB -> EGH: true (in closure node set)
	ass.True(fd.InClosure(NewFastIntSet(1, 2), NewFastIntSet(5, 7, 8)))
}

func TestFDSet_AddConstant(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{}
	ass.Equal("()", fd.ConstantCols().String())

	fd.AddConstants(NewFastIntSet(1, 2)) // {} --> {a,b}
	ass.Equal(len(fd.fdEdges), 1)
	ass.True(fd.fdEdges[0].strict)
	ass.False(fd.fdEdges[0].equiv)
	ass.Equal("()", fd.fdEdges[0].from.String())
	ass.Equal("(1,2)", fd.fdEdges[0].to.String())
	ass.Equal("(1,2)", fd.ConstantCols().String())

	fd.AddConstants(NewFastIntSet(3)) // c, {} --> {a,b,c}
	ass.Equal(len(fd.fdEdges), 1)
	ass.True(fd.fdEdges[0].strict)
	ass.False(fd.fdEdges[0].equiv)
	ass.Equal("()", fd.fdEdges[0].from.String())
	ass.Equal("(1-3)", fd.fdEdges[0].to.String())
	ass.Equal("(1-3)", fd.ConstantCols().String())

	fd.AddStrictFunctionalDependency(NewFastIntSet(3, 4), NewFastIntSet(5, 6)) // {c,d} --> {e,f}
	ass.Equal(len(fd.fdEdges), 2)
	ass.True(fd.fdEdges[0].strict)
	ass.False(fd.fdEdges[0].equiv)
	ass.Equal("()", fd.fdEdges[0].from.String())
	ass.Equal("(1-3)", fd.fdEdges[0].to.String())
	ass.Equal("(1-3)", fd.ConstantCols().String())
	ass.True(fd.fdEdges[1].strict)
	ass.False(fd.fdEdges[1].equiv)
	ass.Equal("(4)", fd.fdEdges[1].from.String()) // determinant 3 reduced as constant, leaving FD {d} --> {f,g}.
	ass.Equal("(5,6)", fd.fdEdges[1].to.String())

	fd.AddLaxFunctionalDependency(NewFastIntSet(7), NewFastIntSet(5, 6), false) // {g} ~~> {e,f}
	ass.Equal(len(fd.fdEdges), 3)
	ass.False(fd.fdEdges[2].strict)
	ass.False(fd.fdEdges[2].equiv)
	ass.Equal("(7)", fd.fdEdges[2].from.String())
	ass.Equal("(5,6)", fd.fdEdges[2].to.String())

	fd.AddConstants(NewFastIntSet(4)) // add d, {} --> {a,b,c,d}, and FD {d} --> {f,g} is transferred to constant closure.
	ass.Equal(1, len(fd.fdEdges))     // => {} --> {a,b,c,d,e,f}, for lax FD {g} ~~> {e,f}, dependencies are constants, removed.
	ass.True(fd.fdEdges[0].strict)
	ass.False(fd.fdEdges[0].equiv)
	ass.Equal("()", fd.fdEdges[0].from.String())
	ass.Equal("(1-6)", fd.fdEdges[0].to.String())
	ass.Equal("(1-6)", fd.ConstantCols().String())
}

func TestFDSet_LaxImplies(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{}
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(2, 3), false)
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(2), false)
	// lax FD won't imply each other once they have the different to side.
	ass.Equal("(1)~~>(2,3), (1)~~>(2)", fd.String())

	fd = FDSet{}
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(2), false)
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(2, 3), false)
	ass.Equal("(1)~~>(2), (1)~~>(2,3)", fd.String())

	fd = FDSet{}
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(3), false)
	fd.AddLaxFunctionalDependency(NewFastIntSet(1, 2), NewFastIntSet(3), false)
	// lax FD can imply each other once they have the same to side. {1,2} ~~> {3} implies {1} ~~> {3}
	ass.Equal("(1)~~>(3)", fd.String())

	fd = FDSet{}
	fd.AddLaxFunctionalDependency(NewFastIntSet(1), NewFastIntSet(3, 4), false)
	fd.AddLaxFunctionalDependency(NewFastIntSet(1, 2), NewFastIntSet(3), false)
	// lax FD won't imply each other once they have the different to side. {1,2} ~~> {3} implies {1} ~~> {3}
	ass.Equal("(1)~~>(3,4), (1,2)~~>(3)", fd.String())
}

func TestFDSet_AddEquivalence(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{}
	ass.Equal(0, len(fd.EquivalenceCols()))

	fd.AddEquivalence(NewFastIntSet(1), NewFastIntSet(2)) // {a} == {b}
	ass.Equal(1, len(fd.fdEdges))                         // res: {a} == {b}
	ass.Equal(1, len(fd.EquivalenceCols()))
	ass.True(fd.fdEdges[0].strict)
	ass.True(fd.fdEdges[0].equiv)
	ass.Equal("(1,2)", fd.fdEdges[0].from.String())
	ass.Equal("(1,2)", fd.fdEdges[0].to.String())
	ass.Equal("(1,2)", fd.EquivalenceCols()[0].String())

	fd.AddEquivalence(NewFastIntSet(3), NewFastIntSet(4)) // {c} == {d}
	ass.Equal(2, len(fd.fdEdges))                         // res: {a,b} == {a,b}, {c,d} == {c,d}
	ass.Equal(2, len(fd.EquivalenceCols()))
	ass.True(fd.fdEdges[0].strict)
	ass.True(fd.fdEdges[0].equiv)
	ass.Equal("(1,2)", fd.fdEdges[0].from.String())
	ass.Equal("(1,2)", fd.fdEdges[0].to.String())
	ass.Equal("(1,2)", fd.EquivalenceCols()[0].String())
	ass.True(fd.fdEdges[1].strict)
	ass.True(fd.fdEdges[1].equiv)
	ass.Equal("(3,4)", fd.fdEdges[1].from.String())
	ass.Equal("(3,4)", fd.fdEdges[1].to.String())
	ass.Equal("(3,4)", fd.EquivalenceCols()[1].String())

	fd.AddConstants(NewFastIntSet(4, 5)) // {} --> {d,e}
	ass.Equal(3, len(fd.fdEdges))        // res: {a,b} == {a,b}, {c,d} == {c,d},{} --> {c,d,e}
	ass.True(fd.fdEdges[2].strict)       // explain: constant closure is extended by equivalence {c,d} == {c,d}
	ass.False(fd.fdEdges[2].equiv)
	ass.Equal("()", fd.fdEdges[2].from.String())
	ass.Equal("(3-5)", fd.fdEdges[2].to.String())
	ass.Equal("(3-5)", fd.ConstantCols().String())

	fd.AddStrictFunctionalDependency(NewFastIntSet(2, 3), NewFastIntSet(5, 6)) // {b,c} --> {e,f}
	ass.Equal(4, len(fd.fdEdges))                                              // res: {a,b} == {a,b}, {c,d} == {c,d},{} --> {c,d,e}, {b} --> {e,f}
	ass.True(fd.fdEdges[3].strict)                                             // explain: strict FD's from side c is eliminated by constant closure.
	ass.False(fd.fdEdges[3].equiv)
	ass.Equal("(2)", fd.fdEdges[3].from.String())
	ass.Equal("(5,6)", fd.fdEdges[3].to.String())

	fd.AddEquivalence(NewFastIntSet(2), NewFastIntSet(3)) // {b} == {d}
	// res: {a,b,c,d} == {a,b,c,d}, {} --> {a,b,c,d,e,f}
	// explain:
	// b = d build the connection between {a,b} == {a,b}, {c,d} == {c,d}, make the superset of equivalence closure.
	// the superset equivalence closure extend the existed constant closure in turn, resulting {} --> {a,b,c,d,e}
	// the superset constant closure eliminate existed strict FD, since determinants is constant, so the dependencies must be constant as well.
	// so extending the current constant closure as to {} --> {a,b,c,d,e,f}
	ass.Equal(2, len(fd.fdEdges))
	ass.Equal(1, len(fd.EquivalenceCols()))
	ass.Equal("(1-4)", fd.EquivalenceCols()[0].String())
	ass.Equal("(1-6)", fd.ConstantCols().String())
}
