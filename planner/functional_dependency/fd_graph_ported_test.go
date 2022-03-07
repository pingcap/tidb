package functional_dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncDeps_ConstCols(t *testing.T) {
	fd := &FDSet{}
	ass := assert.New(t)
	require.Equal(t, "()", fd.ConstantCols().String())
	fd.AddConstants(NewFastIntSet(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())

	fd2 := makeAbcdeFD(ass)
	require.Equal(t, "()", fd2.ConstantCols().String())
	fd2.AddConstants(NewFastIntSet(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
//   CREATE UNIQUE INDEX ON abcde (b, c)
func makeAbcdeFD(ass *assert.Assertions) *FDSet {
	// Set Key to all cols to start, and ensure it's overridden in AddStrictKey.
	allCols := NewFastIntSet(1, 2, 3, 4, 5)
	abcde := &FDSet{}
	abcde.AddStrictFunctionalDependency(NewFastIntSet(1), allCols)
	abcde.AddLaxFunctionalDependency(NewFastIntSet(2, 3), allCols)

	testColsAreStrictKey(ass, abcde, NewFastIntSet(1), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(2, 3), allCols, false)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(1, 2), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(1, 2, 3, 4, 5), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(4, 5), allCols, false)
	testColsAreLaxKey(ass, abcde, NewFastIntSet(2, 3), allCols, true)
	return abcde
}

func testColsAreStrictKey(ass *assert.Assertions, fd *FDSet, cols, allCols FastIntSet, is bool) {
	closure := fd.closureOfStrict(cols)
	ass.Equal(allCols.SubsetOf(closure), is)
}

func testColsAreLaxKey(ass *assert.Assertions, fd *FDSet, cols, allCols FastIntSet, is bool) {
	closure := fd.closureOfLax(cols)
	ass.Equal(allCols.SubsetOf(closure), is)
}

// Other tests also exercise the ColsAreKey methods.
func TestFuncDeps_ColsAreKey(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// This case wouldn't actually happen with a real world query.
	ass := assert.New(t)
	var loj FDSet
	preservedCols := NewFastIntSet(1, 2, 3, 4, 5)
	nullExtendedCols := NewFastIntSet(10, 11, 12, 13, 14)
	abcde := makeAbcdeFD(ass)
	mnpq := makeMnpqFD(ass)
	mnpq.AddStrictFunctionalDependency(NewFastIntSet(12, 13), NewFastIntSet(14))
	loj = *abcde
	loj.MakeCartesianProduct(mnpq)
	loj.AddConstants(NewFastIntSet(3))
	loj.MakeOuterJoin(&FDSet{}, &FDSet{}, preservedCols, nullExtendedCols)
	loj.AddEquivalence(NewFastIntSet(1), NewFastIntSet(10))

	testcases := []struct {
		cols   FastIntSet
		strict bool
		lax    bool
	}{
		{cols: NewFastIntSet(1, 2, 3, 4, 5, 10, 11, 12, 13, 14), strict: true, lax: true},
		{cols: NewFastIntSet(1, 2, 3, 4, 5, 10, 12, 13, 14), strict: false, lax: false},
		{cols: NewFastIntSet(1, 11), strict: true, lax: true},
		{cols: NewFastIntSet(10, 11), strict: true, lax: true},
		{cols: NewFastIntSet(1), strict: false, lax: false},
		{cols: NewFastIntSet(10), strict: false, lax: false},
		{cols: NewFastIntSet(11), strict: false, lax: false},
		{cols: NewFastIntSet(), strict: false, lax: false},

		// This case is interesting: if we take into account that 3 is a constant,
		// we could put 2 and 3 together and use (2,3)~~>(1,4,5) and (1)==(10) to
		// prove that (2,3) is a lax key. But this is only true when that constant
		// value for 3 is not NULL. We would have to pass non-null information to
		// the check.
		{cols: NewFastIntSet(2, 11), strict: false, lax: false},
	}

	for _, tc := range testcases {
		testColsAreStrictKey(ass, &loj, tc.cols, loj.AllCols(), tc.strict)
		testColsAreLaxKey(ass, &loj, tc.cols, loj.AllCols(), tc.lax)
	}
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
func makeMnpqFD(ass *assert.Assertions) *FDSet {
	allCols := NewFastIntSet(10, 11, 12, 13)
	mnpq := &FDSet{}
	mnpq.AddStrictFunctionalDependency(NewFastIntSet(10, 11), allCols)
	mnpq.MakeNotNull(NewFastIntSet(10, 11))
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10), allCols, false)
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10, 11), allCols, true)
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10, 11, 12), allCols, true)
	return mnpq
}

// Construct cartesian product FD from figure 3.6, page 122:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
//   SELECT * FROM abcde, mnpq
func makeProductFD(ass *assert.Assertions) *FDSet {
	product := makeAbcdeFD(ass)
	product.MakeCartesianProduct(makeMnpqFD(ass))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)", product.String())
	testColsAreStrictKey(ass, product, NewFastIntSet(1), product.AllCols(), false)
	testColsAreStrictKey(ass, product, NewFastIntSet(10, 11), product.AllCols(), false)
	testColsAreStrictKey(ass, product, NewFastIntSet(1, 10, 11), product.AllCols(), true)
	testColsAreStrictKey(ass, product, NewFastIntSet(1, 2, 3, 10, 11, 12), product.AllCols(), true)
	testColsAreStrictKey(ass, product, NewFastIntSet(2, 3, 10, 11), product.AllCols(), false)
	return product
}

// Test ProjectionCols.
func TestFuncDeps_ProjectCols(t *testing.T) {
	ass := assert.New(t)
	foo := &FDSet{}
	all := NewFastIntSet(1, 2, 3, 4)
	foo.AddStrictFunctionalDependency(NewFastIntSet(1), all)
	foo.AddLaxFunctionalDependency(NewFastIntSet(2, 3), all)
	foo.AddLaxFunctionalDependency(NewFastIntSet(4), all)
	ass.Equal("(1)-->(2-4), (2,3)~~>(1,4), (4)~~>(1-3)", foo.String())
	foo.MakeNotNull(NewFastIntSet(1, 4))
	//  nothing change.
	ass.Equal("(1)-->(2-4), (2,3)~~>(1,4), (4)~~>(1-3)", foo.String())
	foo.ProjectCols(NewFastIntSet(2, 3, 4))
	ass.Equal("(2,3)~~>(4), (4)~~>(2,3)", foo.String())
	foo.MakeNotNull(NewFastIntSet(2, 3, 4))
	// not null cols can strict the lax FD.
	ass.Equal("(2,3)-->(4), (4)-->(2,3)", foo.String())

	x := makeAbcdeFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", x.String())
	x.ProjectCols(NewFastIntSet(2, 3))
	ass.Equal("", x.String())

	x = makeAbcdeFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", x.String())
	x.MakeNotNull(NewFastIntSet(2, 3))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", x.String())
	x.MakeNotNull(NewFastIntSet(2, 3, 1, 4, 5))
	ass.Equal("(1)-->(2-5), (2,3)-->(1,4,5)", x.String())
	x.ProjectCols(NewFastIntSet(2, 3))
	ass.Equal("", x.String())

	// Remove column from lax dependency.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   SELECT a, c, d, e FROM abcde
	abde := makeAbcdeFD(ass)
	abde.ProjectCols(NewFastIntSet(1, 3, 4, 5))
	ass.Equal("(1)-->(3-5)", abde.String())

	// Try removing columns that are only dependants (i.e. never determinants).
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, b, c, m, n FROM abcde, mnpq WHERE a=m
	abcmn := makeJoinFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", abcmn.String())
	abcmn.MakeNotNull(NewFastIntSet(1, 4, 5))
	abcmn.ProjectCols(NewFastIntSet(1, 2, 3, 10, 11))
	ass.Equal("(1)-->(2,3), (2,3)~~>(1), (1,10)==(1,10)", abcmn.String())
	testColsAreStrictKey(ass, abcmn, NewFastIntSet(1, 11), abcmn.AllCols(), true)
	testColsAreStrictKey(ass, abcmn, NewFastIntSet(2, 3), abcmn.AllCols(), false)

	// Remove column that is constant and part of multi-column determinant.
	//   SELECT a, c, d, e FROM abcde WHERE b=1
	abcde := makeAbcdeFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", abcde.String())
	abcde.AddConstants(NewFastIntSet(2))
	ass.Equal("(1)-->(3-5), (2,3)~~>(1,4,5), ()-->(2)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(2, 3))
	ass.Equal("(1)-->(3-5), (2,3)~~>(1,4,5), ()-->(2)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(1, 4, 5))
	// with not-null columns {2,3,1,4,5}, lax (2,3)~~>(1,4,5) can be improved as strict one,
	// and determinant 2 is a constant which eliminated.
	ass.Equal("(1)-->(3-5), ()-->(2), (3)-->(1,4,5)", abcde.String())
	abcde.ProjectCols(NewFastIntSet(1, 3, 4, 5))
	ass.Equal("(1)-->(3-5), (3)-->(1,4,5)", abcde.String())

	// Remove key columns, but expect another key to be found.
	//   SELECT b, c, n FROM abcde, mnpq WHERE a=m AND b IS NOT NULL AND c IS NOT NULL
	switchKey := makeJoinFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", switchKey.String())
	switchKey.MakeNotNull(NewFastIntSet(2, 3))
	// only with not-null columns {2,3}, nothing changed.
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", switchKey.String())
	switchKey.MakeNotNull(NewFastIntSet(1, 4, 5))
	// with not-null columns {2,3,1,4,5}, lax FD changed.
	ass.Equal("(1)-->(2-5), (10,11)-->(12,13), (1,10)==(1,10), (2,3)-->(1,4,5)", switchKey.String())
	switchKey.ProjectCols(NewFastIntSet(2, 3, 11))
	ass.Equal("", switchKey.String())

	// Remove column from every determinant and ensure that all FDs go away.
	//   SELECT d FROM abcde, mnpq WHERE a=m
	noKey := makeJoinFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", noKey.String())
	noKey.ProjectCols(NewFastIntSet(2, 11))
	ass.Equal("", noKey.String())

	// Remove columns so that there is no longer a key.
	//   SELECT b, c, d, e, n, p, q FROM abcde, mnpq WHERE a=m
	bcden := makeJoinFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", bcden.String())
	bcden.MakeNotNull(NewFastIntSet(1))
	bcden.ProjectCols(NewFastIntSet(2, 3, 4, 5, 11, 12, 13))
	ass.Equal("(2,3)~~>(4,5)", bcden.String())

	// Remove remainder of columns (N rows, 0 cols projected).
	bcden.ProjectCols(NewFastIntSet())
	ass.Equal("", bcden.String())

	// Project single column.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND a=1 AND n=1
	oneRow := makeJoinFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", oneRow.String())
	oneRow.AddConstants(NewFastIntSet(1, 11))
	ass.Equal("(1,10)==(1,10), ()-->(1-5,10-13)", oneRow.String())
	oneRow.ProjectCols(NewFastIntSet(4))
	ass.Equal("()-->(4)", oneRow.String())

	// Remove column that has equivalent substitute.
	//   SELECT e, one FROM (SELECT *, d+1 AS one FROM abcde) WHERE d=e
	abcde = makeAbcdeFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", abcde.String())
	// generated column 6(one) is depend on 4(d).
	abcde.AddStrictFunctionalDependency(NewFastIntSet(4), NewFastIntSet(6))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (4)-->(6)", abcde.String())
	abcde.AddEquivalence(NewFastIntSet(4), NewFastIntSet(5))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (4)-->(6), (4,5)==(4,5)", abcde.String())
	abcde.ProjectCols(NewFastIntSet(5, 6))
	ass.Equal("(5)-->(6)", abcde.String())

	// Remove column that has equivalent substitute and is part of composite
	// determinant.
	//   SELECT c, d, e FROM abcde WHERE b=d
	abcde = makeAbcdeFD(ass)
	abcde.AddEquivalence(NewFastIntSet(2), NewFastIntSet(4))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (2,4)==(2,4)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(1))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (2,4)==(2,4)", abcde.String())
	abcde.ProjectCols(NewFastIntSet(3, 4, 5))
	ass.Equal("(3,4)~~>(5)", abcde.String())

	// Equivalent substitution results in (4,5)~~>(4,5), which is eliminated.
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(ass)
	abcde.AddEquivalence(NewFastIntSet(2), NewFastIntSet(4))
	abcde.AddEquivalence(NewFastIntSet(3), NewFastIntSet(5))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (2,4)==(2,4), (3,5)==(3,5)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(1))
	abcde.ProjectCols(NewFastIntSet(4, 5))
	ass.Equal("", abcde.String())

	// Use ProjectCols to add columns (make sure key is extended).
	//   SELECT expr(b) f, expr(c) g FROM abcde;
	abcde = makeAbcdeFD(ass)
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(1))
	// 6,7 is two extended column, either constant eg: f= const(f) or expr eg: g = expr(c).
	abcde.AddStrictFunctionalDependency(NewFastIntSet(3), NewFastIntSet(7))
	abcde.AddStrictFunctionalDependency(NewFastIntSet(2), NewFastIntSet(6))
	abcde.ProjectCols(NewFastIntSet(1, 2, 3, 4, 5, 6, 7))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (3)-->(7), (2)-->(6)", abcde.String())

	// Verify lax keys are retained (and can later become keys) when the key is
	// projected away.
	abcde = &FDSet{}
	abcde.AddStrictFunctionalDependency(NewFastIntSet(1), NewFastIntSet(1, 2, 3, 4, 5))
	abcde.AddLaxFunctionalDependency(NewFastIntSet(2), NewFastIntSet(1, 2, 3, 4, 5))
	abcde.AddLaxFunctionalDependency(NewFastIntSet(3, 4), NewFastIntSet(1, 2, 3, 4, 5))
	ass.Equal("(1)-->(2-5), (2)~~>(1,3-5), (3,4)~~>(1,2,5)", abcde.String())
	abcde.MakeNotNull(NewFastIntSet(1))
	abcde.ProjectCols(NewFastIntSet(2, 3, 4, 5))
	ass.Equal("(2)~~>(3-5), (3,4)~~>(2,5)", abcde.String())
	// 2 on its own is not necessarily a lax key: even if it determines the other
	// columns, any of them can still be NULL.
	testColsAreLaxKey(ass, abcde, NewFastIntSet(2), abcde.AllCols(), true)
	testColsAreLaxKey(ass, abcde, NewFastIntSet(3, 4), abcde.AllCols(), true)

	// Verify that lax keys convert to strong keys.
	abcde.MakeNotNull(NewFastIntSet(2, 3, 4, 5))
	ass.Equal("(2)-->(3-5), (3,4)-->(2,5)", abcde.String())

	// ProjectCols was creating FD relations with overlapping from/to sets.
	fd := FDSet{}
	fd.AddConstants(NewFastIntSet(2, 3))
	ass.Equal("()-->(2,3)", fd.String())
	fd.AddStrictFunctionalDependency(NewFastIntSet(4), NewFastIntSet(1))
	ass.Equal("()-->(2,3), (4)-->(1)", fd.String())
	fd.AddStrictFunctionalDependency(NewFastIntSet(1), NewFastIntSet(1, 2, 3, 4))
	// todo: add strict fd should feel the existed equiv and eliminate itself. eg:  (1)-->(2-4) below can be stored as (1)-->(4)
	ass.Equal("()-->(2,3), (4)-->(1), (1)-->(2-4)", fd.String())
	fd.AddEquivalence(NewFastIntSet(2), NewFastIntSet(3))
	ass.Equal("()-->(2,3), (4)-->(1), (1)-->(2-4), (2,3)==(2,3)", fd.String())
	// Now project away column 3, and make sure we don't end up with (1)->(1,4).
	fd.ProjectCols(NewFastIntSet(1, 2, 4))
	ass.Equal("(4)-->(1), (1)-->(4), ()-->(2)", fd.String())
}

// Construct inner join FD:
//   SELECT * FROM abcde, mnpq WHERE a=m
func makeJoinFD(ass *assert.Assertions) *FDSet {
	// Start with cartesian product FD and add equivalency to it.
	join := makeProductFD(ass)
	join.AddEquivalence(NewFastIntSet(1), NewFastIntSet(10))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", join.String())
	join.ProjectCols(NewFastIntSet(1, 2, 3, 4, 5, 10, 11, 12, 13))
	ass.Equal("(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10)==(1,10)", join.String())
	testColsAreStrictKey(ass, join, NewFastIntSet(1, 11), join.AllCols(), true)
	testColsAreStrictKey(ass, join, NewFastIntSet(1, 10), join.AllCols(), false)
	testColsAreStrictKey(ass, join, NewFastIntSet(1, 10, 11), join.AllCols(), true)
	testColsAreStrictKey(ass, join, NewFastIntSet(1), join.AllCols(), false)
	testColsAreStrictKey(ass, join, NewFastIntSet(10, 11), join.AllCols(), true)
	testColsAreStrictKey(ass, join, NewFastIntSet(2, 3, 11), join.AllCols(), false)
	// lax to can be null, so it could be passive with strict FD.
	testColsAreLaxKey(ass, join, NewFastIntSet(2, 3, 11), join.AllCols(), false)
	return join
}

func TestFuncDeps_OuterJoin(t *testing.T) {

}
