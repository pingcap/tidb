// Copyright 2022 PingCAP, Inc.
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

package funcdep

import (
	"errors"
	"fmt"
	"strings"
)

type fdEdge struct {
	// functional dependency = determinants -> dependencies
	// determinants = from
	// dependencies = to
	from FastIntSet
	to   FastIntSet
	// The value of the strict and eq bool forms the four kind of edges:
	// functional dependency, lax functional dependency, strict equivalence constraint, lax equivalence constraint.
	// And if there's a functional dependency `const` -> `column` exists. We would let the from side be empty.
	strict bool
	equiv  bool
}

// FDSet is the main portal of functional dependency, it stores the relationship between (extended table / physical table)'s
// columns. For more theory about this design, ref the head comments in the funcdep/doc.go.
type FDSet struct {
	fdEdges []*fdEdge
	// NotNullCols is used to record the columns with not-null attributes applied.
	// eg: {1} ~~> {2,3}, when {2,3} not null is applied, it actually does nothing.
	// but we should record {2,3} as not-null down for the convenience of transferring
	// Lax FD: {1} ~~> {2,3} to strict FD: {1} --> {2,3} with {1} as not-null next time.
	NotNullCols FastIntSet
	// HashCodeToUniqueID map the expression's hashcode to a statement allocated unique
	// ID quite like the unique ID bounded with column. It's mainly used to add the expr
	// to the fdSet as an extended column. <NOT CONCURRENT SAFE FOR NOW>
	HashCodeToUniqueID map[string]int
	// GroupByCols is used to record columns / expressions that under the group by phrase.
	GroupByCols FastIntSet
	HasAggBuilt bool
	// after left join, according to rule 3.3.3, it may create a lax FD from inner equivalence
	// cols pointing to outer equivalence cols.  eg: t left join t1 on t.a = t1.b, leading a
	// lax FD from t1.b ~> t.a, this lax attribute is coming from supplied null value to all
	// left rows, once there is a null-refusing predicate on the inner side on upper layer, this
	// can be equivalence again. (the outer rows left are all coming from equal matching)
	//
	// why not just makeNotNull of them, because even a non-equiv-related inner col can also
	// refuse supplied null values.
	Rule333Equiv struct {
		Edges     []*fdEdge
		InnerCols FastIntSet
	}
}

// ClosureOfStrict is exported for outer usage.
func (s *FDSet) ClosureOfStrict(colSet FastIntSet) FastIntSet {
	return s.closureOfStrict(colSet)
}

// closureOfStrict is to find strict fd closure of X with respect to F.
// A -> B  =  colSet -> { resultIntSet }
// eg: considering closure F: {A-> CD, B -> E}, and input is {AB}
// res: AB -> {CDE} (AB is included in trivial FD)
// The time complexity is O(n^2).
func (s *FDSet) closureOfStrict(colSet FastIntSet) FastIntSet {
	resultSet := NewFastIntSet()
	// self included.
	resultSet.UnionWith(colSet)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(resultSet) && !fd.to.SubsetOf(resultSet) {
			resultSet.UnionWith(fd.to)
			// If the closure is updated, we redo from the beginning.
			i = -1
		}
		// why this? it is caused by our definition of equivalence as {superset} == {superset},
		// which is also seen as {superset} --> {superset}. but when we compute the transitive
		// closure, `fd.from.SubsetOf(resultSet)` is not transitive here. Actually, we can also
		// see the equivalence as {single element} == {superset} / {single element} --> {superset}.
		if fd.equiv && fd.from.Intersects(resultSet) && !fd.to.SubsetOf(resultSet) {
			resultSet.UnionWith(fd.to)
			i = -1
		}
	}
	return resultSet
}

// ClosureOfLax is exported for outer usage.
func (s *FDSet) ClosureOfLax(colSet FastIntSet) FastIntSet {
	return s.closureOfLax(colSet)
}

// ClosureOfLax is used to find lax fd closure of X with respect to F.
func (s *FDSet) closureOfLax(colSet FastIntSet) FastIntSet {
	// Lax dependencies are not transitive (see figure 2.1 in the paper for
	// properties that hold for lax dependencies), so only include them if they
	// are reachable in a single lax dependency step from the input set.
	laxOneStepReached := NewFastIntSet()
	// self included.
	laxOneStepReached.UnionWith(colSet)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		// A ~~> B && A == C && C ~~> D: given A, BD can be included via Lax FDs (plus self A and equiv C).
		// A ~~> B && B == C: given A, BC can be included via Lax FDs (plus self A).
		// which means both dependency and determinant can extend Lax exploration via equivalence.
		//
		// Besides, strict is a kind of strong lax FDs, result computed above should be unionised with strict FDs closure.
		if fd.equiv && fd.from.Intersects(laxOneStepReached) && !fd.to.SubsetOf(laxOneStepReached) {
			// equiv can extend the lax-set's access paths.
			laxOneStepReached.UnionWith(fd.to)
			i = -1
		}
		if !fd.strict && !fd.equiv && fd.from.SubsetOf(laxOneStepReached) && !fd.to.SubsetOf(laxOneStepReached) {
			// lax FDs.
			laxOneStepReached.UnionWith(fd.to)
		}
	}
	// Unionised strict FDs
	laxOneStepReached.UnionWith(s.closureOfStrict(colSet))
	return laxOneStepReached
}

// closureOfEquivalence is to find strict equivalence closure of X with respect to F.
func (s *FDSet) closureOfEquivalence(colSet FastIntSet) FastIntSet {
	resultSet := NewFastIntSet()
	// self included.
	resultSet.UnionWith(colSet)
	for i := 0; i < len(s.fdEdges); i++ {
		// equivalence is maintained as {superset} == {superset}, we don't need to do transitive computation.
		// but they may multi equivalence closure, eg: {a,b}=={a,b}, {c,d} =={c,d}, when adding b=c, we need traverse them all.
		fd := s.fdEdges[i]
		if fd.equiv {
			if fd.from.Intersects(resultSet) && !fd.to.SubsetOf(resultSet) {
				resultSet.UnionWith(fd.to)
			}
		}
	}
	return resultSet
}

// InClosure is used to judge whether fd: setA -> setB can be inferred from closure s.
// It's a short-circuit version of the `closureOf`.
func (s *FDSet) InClosure(setA, setB FastIntSet) bool {
	if setB.SubsetOf(setA) {
		return true
	}
	currentClosure := NewFastIntSet()
	// self included.
	currentClosure.UnionWith(setA)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(currentClosure) && !fd.to.SubsetOf(currentClosure) {
			// once fd.from is subset of setA, which means fd is part of our closure;
			// when fd.to is not subset of setA itself, it means inference result is necessary to add; (closure extending)
			currentClosure.UnionWith(fd.to)
			if setB.SubsetOf(currentClosure) {
				return true
			}
			// If the closure is updated, we redo from the beginning.
			i = -1
		}
		// why this? it is caused by our definition of equivalence as {superset} == {superset},
		// which is also seen as {superset} --> {superset}. but when we compute the transitive
		// closure, `fd.from.SubsetOf(resultSet)` is not transitive here. Actually, we can also
		// see the equivalence as {single element} == {superset} / {single element} --> {superset}.
		if fd.equiv && fd.from.Intersects(currentClosure) && !fd.to.SubsetOf(currentClosure) {
			currentClosure.UnionWith(fd.to)
			if setB.SubsetOf(currentClosure) {
				return true
			}
			i = -1
		}
	}
	return false
}

// ReduceCols is used to minimize the determinants in one fd input.
// function dependency = determinants -> dependencies
// given: AB -> XY, once B can be inferred from current closure when inserting, take A -> XY instead.
func (s *FDSet) ReduceCols(colSet FastIntSet) FastIntSet {
	// Suppose the colSet is A and B, we have A --> B. Then we only need A since B' value is always determined by A.
	var removed, result = NewFastIntSet(), NewFastIntSet()
	result.CopyFrom(colSet)
	for k, ok := colSet.Next(0); ok; k, ok = colSet.Next(k + 1) {
		removed.Insert(k)
		result.Remove(k)
		// If the removed one is not dependent with the result. We add the bit back.
		if !s.InClosure(result, removed) {
			removed.Remove(k)
			result.Insert(k)
		}
	}
	return result
}

// AddStrictFunctionalDependency is to add `STRICT` functional dependency to the fdGraph.
func (s *FDSet) AddStrictFunctionalDependency(from, to FastIntSet) {
	s.addFunctionalDependency(from, to, true, false)
}

// AddLaxFunctionalDependency is to add `LAX` functional dependency to the fdGraph.
func (s *FDSet) AddLaxFunctionalDependency(from, to FastIntSet) {
	s.addFunctionalDependency(from, to, false, false)
}

// addFunctionalDependency will add strict/lax functional dependency to the fdGraph.
// eg:
// CREATE TABLE t (a int key, b int, c int, d int, e int, UNIQUE (b,c))
// strict FD: {a} --> {a,b,c,d,e} && lax FD: {b,c} ~~> {a,b,c,d,e} will be added.
// stored FD: {a} --> {b,c,d,e} && lax FD: {b,c} ~~> {a,d,e} is determinant eliminated.
//
// To reduce the edge number, we limit the functional dependency when we insert into the
// set. The key code of insert is like the following codes.
func (s *FDSet) addFunctionalDependency(from, to FastIntSet, strict, equiv bool) {
	// trivial FD, refused.
	if to.SubsetOf(from) {
		return
	}

	// exclude the intersection part.
	if to.Intersects(from) {
		to.DifferenceWith(from)
	}

	// reduce the determinants.
	from = s.ReduceCols(from)

	newFD := &fdEdge{
		from:   from,
		to:     to,
		strict: strict,
		equiv:  equiv,
	}

	swapPointer := 0
	added := false
	// the newFD couldn't be superSet of existed one A and be subset of the other existed one B at same time.
	// Because the subset relationship between A and B will be replaced previously.
	for i := range s.fdEdges {
		fd := s.fdEdges[i]
		// If the new one is stronger than the old one. Just replace it.
		if newFD.implies(fd) {
			if added {
				continue
			}
			fd.from = from
			fd.to = to
			fd.strict = strict
			fd.equiv = equiv
			added = true
		} else if !added {
			// There's a strong one. No need to add.
			if fd.implies(newFD) {
				added = true
			} else if fd.strict && !fd.equiv && fd.from.Equals(from) {
				// We can use the new FD to extend the current one.
				// eg:  A -> BC, A -> CE, they couldn't be the subset of each other, union them.
				// res: A -> BCE
				fd.to.UnionWith(to)
				added = true
			}
		}
		// If the current one is not eliminated, add it to the result.
		s.fdEdges[swapPointer] = s.fdEdges[i]
		swapPointer++
	}
	s.fdEdges = s.fdEdges[:swapPointer]

	// If it's still not added.
	if !added {
		s.fdEdges = append(s.fdEdges, newFD)
	}
}

// implies is used to shrink the edge size, keeping the minimum of the functional dependency set size.
func (e *fdEdge) implies(otherEdge *fdEdge) bool {
	// The given one's from should be larger than the current one and the current one's to should be larger than the given one.
	// STRICT FD:
	// A --> C is stronger than AB --> C. --- YES
	// A --> BC is stronger than A --> C. --- YES
	//
	// LAX FD:
	// 1: A ~~> C is stronger than AB ~~> C. --- YES
	// 2: A ~~> BC is stronger than A ~~> C. --- NO
	// The precondition for 2 to be strict FD is much easier to satisfied than 1, only to
	// need {a,c} is not null. So we couldn't merge this two to be one lax FD.
	// but for strict/equiv FD implies lax FD, 1 & 2 is implied both reasonably.
	lhsIsLax := !e.equiv && !e.strict
	rhsIsLax := !otherEdge.equiv && !otherEdge.strict
	if lhsIsLax && rhsIsLax {
		if e.from.SubsetOf(otherEdge.from) && e.to.Equals(otherEdge.to) {
			return true
		}
		return false
	}
	if e.from.SubsetOf(otherEdge.from) && otherEdge.to.SubsetOf(e.to) {
		// The given one should be weaker than the current one.
		// So the given one should not be more strict than the current one.
		// The given one should not be equivalence constraint if the given one is not equivalence constraint.
		if (e.strict || !otherEdge.strict) && (e.equiv || !otherEdge.equiv) {
			return true
		}
		// 1: e.strict   + e.equiv       => e > o
		// 2: e.strict   + !o.equiv      => e >= o
		// 3: !o.strict  + e.equiv       => e > o
		// 4: !o.strict  + !o.equiv      => o.lax
	}
	return false
}

// addEquivalence add an equivalence set to fdSet.
// when adding an equivalence between column a and b:
// 1: they may be integrated into the origin equivalence superset if this two enclosure have some id in common.
// 2: they can be used to extend the existed constant closure, consequently leading some reduce work: see addConstant.
// 3: they self can be used to eliminate existed strict/lax FDs, see comments below.
func (s *FDSet) addEquivalence(eqs FastIntSet) {
	var addConst bool
	// get equivalence closure.
	eqClosure := s.closureOfEquivalence(eqs)
	s.fdEdges = append(s.fdEdges, &fdEdge{from: eqClosure.Copy(), to: eqClosure.Copy(), strict: true, equiv: true})

	for i := 0; i < len(s.fdEdges)-1; i++ {
		fd := s.fdEdges[i]

		if fd.isConstant() {
			// If any equivalent column is a constant, then all are constants.
			if fd.to.Intersects(eqClosure) && !eqClosure.SubsetOf(fd.to) {
				// new non-constant-subset columns need to be merged to the constant closure.
				// Since adding constant wil induce extra FD reshapes, we call addConstant directly.
				addConst = true
			}
		} else if fd.from.SubsetOf(eqClosure) {
			if fd.equiv {
				// this equivalence is enclosed in the super closure appended above, remove it.
				s.fdEdges = append(s.fdEdges[:i], s.fdEdges[i+1:]...)
				i--
			} else {
				// Since from side are all in equivalence closure, we can eliminate some
				// columns of dependencies in equivalence closure. because equivalence is
				// a stronger relationship than a strict or lax dependency.
				// eg: {A,B} --> {C,D}, {A,B,D} in equivalence, FD can be shortly as {A,B} --> {C}
				if fd.removeColumnsToSide(eqClosure) {
					// Once the to side is empty, remove the FD.
					s.fdEdges = append(s.fdEdges[:i], s.fdEdges[i+1:]...)
					i--
				}
			}
		}
	}
	// addConstant logic won't induce recomputing of the logic above recursively, so do it here.
	if addConst {
		// {} --> {a} + {a,b,c} --> {a,b,c} leading to extension {} --> {a,b,c}
		s.AddConstants(eqClosure)
	}
}

// AddEquivalence take two column id as parameters, establish a strict equivalence between
// this two column which may be enclosed a superset of equivalence closure in the fdSet.
//
// For the equivalence storage, for simplicity, we only keep the superset relationship of
// equivalence since equivalence has the reflexivity.
//
// eg: a==b, b==c, a==e
//
// in our fdSet, the equivalence will be stored like: {a, b, c, e} == {a, b, c,e}
// According and characteristic and reflexivity, each element in the equivalence enclosure
// can derive whatever in the same enclosure.
func (s *FDSet) AddEquivalence(from, to FastIntSet) {
	// trivial equivalence, refused.
	if to.SubsetOf(from) {
		return
	}
	s.addEquivalence(from.Union(to))
}

// AddConstants adds a strict FD to the source which indicates that each of the given column
// have the same constant value for all rows, or same null value for all rows if it's nullable.
//
// {} --> {a}
//
// there are several usage for this kind of FD, for example, constant FD column can exist in the
// select fields not matter whatever the group by column is; distinct predicate can be eliminated
// for these columns as well.
//
// when adding columns, be cautious that
// 1: constant can be propagated in the equivalence/strict closure, turning strict FD as a constant one.
// 2: constant can simplify the strict FD both in determinants side and dependencies side.
// 3: constant can simplify the lax FD in the dependencies side.
func (s *FDSet) AddConstants(cons FastIntSet) {
	if cons.IsEmpty() {
		return
	}
	// 1: {} --> {a}, {} --> {b}, when counting the closure, it will collect all constant FD if it has.
	// 2: {m,n} --> {x, y}, once the m,n is subset of constant closure, x,y must be constant as well.
	// 3: {a,b,c} == {a,b,c}, equiv dependency is also strict FD included int the closure computation here.
	cols := s.closureOfStrict(cons)
	s.fdEdges = append(s.fdEdges, &fdEdge{to: cols.Copy(), strict: true})

	// skip the last, newly append one.
	for i := 0; i < len(s.fdEdges)-1; i++ {
		shouldRemoved := false
		fd := s.fdEdges[i]

		if !fd.equiv {
			if fd.strict {
				// Constant columns can be removed from the determinant of a strict
				// FD. If all determinant columns are constant, then the entire FD
				// can be removed, since this means that the dependant columns must
				// also be constant (and were part of constant closure added to the
				// constant FD above).
				if fd.removeColumnsFromSide(cols) {
					// transfer to constant FD which is enclosed in cols above.
					shouldRemoved = true
				}
			}
			// for strict or lax FDs, both can reduce the dependencies side columns with constant closure.
			if fd.removeColumnsToSide(cols) {
				shouldRemoved = true
			}
		}
		if shouldRemoved {
			s.fdEdges = append(s.fdEdges[:i], s.fdEdges[i+1:]...)
			i--
		}
	}
}

// removeColumnsFromSide remove the columns from determinant side of FDs in source.
//
// eg: {A B} --> {C}
//
// once B is a constant, whether values of c in difference rows have the same values or are all
// null is only determined by A, which can be used to simplify the strict FDs.
//
// Attention: why this reduction can not be applied to lax FDs?
//
// According to the lax definition, once determinant side have the null value, whatever dependencies
// would be. So let B be null value, once two row like: <1, null> and <1, null> (false interpreted),
// their dependencies may would be <3>, <4>, once we eliminate B here, FDs looks like: <1>,<1> lax
// determinate <3>,<4>.
func (e *fdEdge) removeColumnsFromSide(cons FastIntSet) bool {
	if e.from.Intersects(cons) {
		e.from = e.from.Difference(cons)
	}
	return e.isConstant()
}

// isConstant returns whether this FD indicates a constant FD which means {} --> {...}
func (e *fdEdge) isConstant() bool {
	return e.from.IsEmpty()
}

// isEquivalence returns whether this FD indicates an equivalence FD which means {xyz...} == {xyz...}
func (e *fdEdge) isEquivalence() bool {
	return e.equiv && e.from.Equals(e.to)
}

// removeColumnsToSide remove the columns from dependencies side of FDs in source.
//
// eg: {A} --> {B, C}
//
// once B is a constant, only the C's value can be determined by A, this kind of irrelevant coefficient
// can be removed in the to side both for strict and lax FDs.
func (e *fdEdge) removeColumnsToSide(cons FastIntSet) bool {
	if e.to.Intersects(cons) {
		e.to = e.to.Difference(cons)
	}
	return e.to.IsEmpty()
}

// ConstantCols returns the set of columns that will always have the same value for all rows in table.
func (s *FDSet) ConstantCols() FastIntSet {
	for i := 0; i < len(s.fdEdges); i++ {
		if s.fdEdges[i].isConstant() {
			return s.fdEdges[i].to
		}
	}
	return FastIntSet{}
}

// EquivalenceCols returns the set of columns that are constrained to equal to each other.
func (s *FDSet) EquivalenceCols() (eqs []*FastIntSet) {
	for i := 0; i < len(s.fdEdges); i++ {
		if s.fdEdges[i].isEquivalence() {
			// return either side is the same.
			eqs = append(eqs, &s.fdEdges[i].from)
		}
	}
	return eqs
}

// MakeNotNull modify the FD set based the listed column with NOT NULL flags.
// Most of the case is used in the derived process after predicate evaluation,
// which can upgrade lax FDs to strict ones.
func (s *FDSet) MakeNotNull(notNullCols FastIntSet) {
	notNullCols.UnionWith(s.NotNullCols)
	notNullColsSet := s.closureOfEquivalence(notNullCols)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict {
			continue
		}
		// unique lax can be made strict only if determinant are not null.
		if fd.from.SubsetOf(notNullColsSet) {
			// we don't need to clean the old lax FD because when adding the corresponding strict one, the lax
			// one will be implied by that and itself is removed.
			s.AddStrictFunctionalDependency(fd.from, fd.to)
			// add strict FDs will cause reconstruction of FDSet, re-traverse it.
			i = -1
		}
	}
	s.NotNullCols = notNullColsSet
}

// MakeNullable make the fd's NotNullCols to be cleaned, after the both side fds are handled it can be nullable.
func (s *FDSet) MakeNullable(nullableCols FastIntSet) {
	s.NotNullCols.DifferenceWith(nullableCols)
}

// MakeCartesianProduct records fdSet after the impact of Cartesian Product of (T1 x T2) is made.
// 1: left FD is reserved.
// 2: right FD is reserved.
// Actually, for two independent table, FDs won't affect (implies or something) each other, appending
// them together is adequate. But for constant FDs, according to our definition, we should merge them
// as a larger superset pointing themselves.
func (s *FDSet) MakeCartesianProduct(rhs *FDSet) {
	for i := 0; i < len(rhs.fdEdges); i++ {
		fd := rhs.fdEdges[i]
		if fd.isConstant() {
			// both from or to side is ok since {superset} --> {superset}.
			s.AddConstants(fd.to)
		} else {
			s.fdEdges = append(s.fdEdges, fd)
		}
	}
	// todo: add strict FD: (left key + right key) -> all cols.
	// maintain a key?
}

// MakeOuterJoin generates the records the fdSet of the outer join.
//
// We always take the left as the row-supplying side, and right side as the null-supplying side. (swap it if not)
// As we know, the outer join would generate null extended rows compared with inner join.
// So we cannot directly do the same thing with the inner join. This function deals with the special cases of the outer join.
//
// Knowledge:
// 1: the filter condition related to lhs column won't filter predicate-allowed rows and refuse null rows (left rows are always exist for all)
// 2: the filter condition related to rhs column won't filter NULL rows although filter has not null attribute on it. (null-appending happened after that)
//
// Notification:
// 1: the origin FD from left side (rows-supplying) over the result of outer join filtered are preserved, because
//		it may be duplicated by multi matching, but actually they are same left rows (don't violate FD definition).
//
// 2: the origin FD from right side (nulls-supplying) over the result of outer join filtered may not be valid anymore.
//
//		<1> strict FD may be wakened as a lax one. But if at least one non-NULL column is part of the determinant, the
//			strict FD can be preserved.
//	        a  b  |  c     d     e
//			------+----------------
//		 	1  1  |  1    NULL   1
//		    1  2  | NULL  NULL  NULL
//		    2  1  | NULL  NULL  NULL
//			left join with (a,b) * (c,d,e) on (a=c and b=1), if there is a strict FD {d} -> {e} on the rhs. After supplied
//			with null values, {d} -> {e} are degraded to a lax one {d} ~~> {e} as you see. the origin and supplied null value
//			for d column determine different dependency.  NULL -> 1 and NULL -> NULL which breaks strict FD definition.
//
//			Unless the determinant contains at least a not null column for example c here, FD like {c,d} -> {e} can survive
//			after the left join. Because you can not find two same key, one from the origin rows and the other one from the
//			supplied rows.
//
//			for lax FD, the supplied rows of null values doesn't take effect of lax FD itself. So it can be kept.
//
//		<2> constant FD should be removed, since null values may be substituted for some unmatched left rows. NULL is not a
//			constant anymore.
//
//      <3> equivalence FD should be removed, since substituted null value are not equal to the other substituted null value.
//
// 3: the newly added FD from filters, should take some considerations as below:
//
//	 	<1> strict/lax FD: join key filter conditions can not produce new strict/lax FD yet (knowledge: 1&2).
//
//		<2> constant FD from the join conditions is only used for the matching mechanism judgement.
//			a   b  |  c     d
//          -------+---------
//          1   1  |  1     1
//          1   2  | NULL NULL
//          left join with (a,b) * (c,d) on (a=c and d=1), some rhs rows will be substituted with null values, and FD on rhs
//          {d=1} are lost.
//
//          a   b  |  c     d
//  		-------+---------
//		    1   1  |  1     1
//          1   2  | NULL NULL
//          left join with (a,b) * (c,d) on (a=c and b=1), it only gives the pass to the first matching, lhs other rows are still
//          kept and appended with null values. So the FD on rhs {b=1} are not applicable to lhs rows.
//
//          above all: constant FD are lost
//
//		<3.1> equivalence FD: when the left join conditions only contain equivalence FD (EFD for short below) across left and right
//			cols and no other `LEFT` condition on (left cols - RED's from) to filter the left join results. We can maintain the strict
//			FD from EFD's `from` side to EFD's `to` side over the left join result.
//			a  b  |  c     d     e
//			------+----------------
//		 	1  1  |  1    NULL   1
//		    1  2  | NULL  NULL  NULL
//		    2  1  | NULL  NULL  NULL
//			Neg eg: left join with (a,b) * (c,d,e) on (a=c and b=1), other b=1 will filter the result, causing the left row (1, 2)
//			miss matched with right row (1, null 1) by a=c, consequently leading the left row appended as (1,2,null,null,null), which
//			will break the FD: {a} -> {c} for key a=1 with different c=1/null.
//			a  b  |  c     d     e
//			------+----------------
//		 	1  1  | NULL  NULL  NULL
//		    2  1  | NULL  NULL  NULL
//			Pos eg: if the filter is on EFD's `from` cols, it's ok. Let's say: (a,b) * (c,d,e) on (a=c and a=2), a=2 only won't leading
//			same key a with matched c and mismatched NULL, neg case result is changed as above, so strict FD {a} -> {c} can exist.
//			a  b  |  c     d     e
//			------+----------------
//		 	1  1  |  1    NULL   1
//		    1  2  | NULL  NULL  NULL
//			2  1  | NULL  NULL  NULL
//			Neg eg: left join with (a,b) * (c,d,e) on (a=c and b=c), two EFD here, where b=c can also refuse some rows joined by a=c,
//			consequently applying it with NULL as (1  2  | NULL  NULL  NULL), leading the same key a has different value 1/NULL. But
//			macroscopically, we can combine {a,b} together as the strict FD's from side, so new FD {a,b} -> {c} is secured. For case
//			of (a=c and b=ce), the FD is {a, b} -> {c, e}
//
// 			conclusion: without this kind of limited left conditions to judge the join match, we can say: FD {a} -> {c} exists.
//
//		<3.2> equivalence FD: when the determinant and dependencies from a equivalence FD of join condition are each covering a strict
//			FD of the left / right side. The left side strict FD's dependencies can be extended to all cols after join result.
//			a  b  |  c     d     e
//			------+----------------
//		 	1  1  |  1    NULL   1
//		    2  2  | NULL  NULL  NULL
//		    3  1  | NULL  NULL  NULL
//			left join with (a,b) * (c,d,e) on (a=c and b=1). Supposing that left `a` are strict Key and right `c` are strict Key too.
//			Key means the strict FD can determine all cols from that table.
//			case 1: left join matched
//				one left row match one / multi rows from right side, since `c` is strict determine all cols from right table, so
//              {a} == {b} --> {all cols in right}, according to the transitive rule of strict FD, we get {a} --> {all cols in right}
//			case 2: left join miss match
//				miss matched rows from left side are unique originally, even appended with NULL value from right side, they are still
//				strictly determine themselves and even the all rows after left join.
//			conclusion combined:
//				equivalence with both strict Key from right and left, the left side's strict FD can be extended to all rows after left join.
//
//		<3.3> equivalence FD: let's see equivalence FD as double-directed strict FD from join equal conditions, and we  only keep the
//			rhs ~~> lhs.
//			a  b  |  c     d     e
//			------+----------------
//		 	1  1  |  1    NULL   1
//		    1  2  | NULL  NULL  NULL
//		    2  1  | NULL  NULL  NULL
//			left join with (a,b) * (c,d,e) on (a=c and b=1). From the join equivalence condition can derive a new FD {ac} == {ac}.
//			while since there are some supplied null value in the c column, we don't guarantee {ac} == {ac} yet, so do {a} -> {c}
//			because two same determinant key {1} can point to different dependency {1} & {NULL}. But in return, FD like {c} -> {a}
//			are degraded to the corresponding lax one.
//
// 4: the new formed FD {left key, right key} -> {all columns} are preserved in spite of the null-supplied rows.
// 5: When there's no join key and no filters from the outer side. The join case is a cartesian product. In this case,
//    the strict equivalence classes is still exists.
//      - If the right side has no row, we would supply null-extended rows, then the value of any column is NULL, the equivalence class exists.
//      - If the right side has rows, no row is filtered out after the filters since no row of the outer side is filtered out. Hence, the equivalence class is still remained.
//
func (s *FDSet) MakeOuterJoin(innerFDs, filterFDs *FDSet, outerCols, innerCols FastIntSet, opt *ArgOpts) {
	//  copy down the left PK and right PK before the s has changed for later usage.
	leftPK, ok1 := s.FindPrimaryKey()
	rightPK, ok2 := innerFDs.FindPrimaryKey()
	copyLeftFDSet := &FDSet{}
	copyLeftFDSet.AddFrom(s)
	copyRightFDSet := &FDSet{}
	copyRightFDSet.AddFrom(innerFDs)

	for _, edge := range innerFDs.fdEdges {
		// Rule #2.2, constant FD are removed from right side of left join.
		if edge.isConstant() {
			continue
		}
		// Rule #2.3, equivalence FD are removed from right side of left join.
		if edge.equiv {
			continue
		}
		// Rule #2.1, lax FD can be kept after the left join.
		if !edge.strict {
			s.addFunctionalDependency(edge.from, edge.to, edge.strict, edge.equiv)
			continue
		}
		// Rule #2.1, strict FD can be kept when determinant contains not null column, otherwise, downgraded to the lax one.
		//
		// If the one of the column from the inner child's functional dependency's left side is not null, this FD can be remained.
		// This is because that the outer join would generate null-extended rows. So if at least one row from the left side
		// is not null. We can guarantee that the there's no same part between the original rows and the generated rows.
		// So the null extended rows would not break the original functional dependency.
		if edge.from.Intersects(innerFDs.NotNullCols) {
			// One of determinant are not null column, strict FD are kept.
			// According knowledge #2, we can't take use of right filter's not null attribute.
			s.addFunctionalDependency(edge.from, edge.to, edge.strict, edge.equiv)
		} else {
			// Otherwise, the strict FD are downgraded to a lax one.
			s.addFunctionalDependency(edge.from, edge.to, false, edge.equiv)
		}
	}
	leftCombinedFDFrom := NewFastIntSet()
	leftCombinedFDTo := NewFastIntSet()
	for _, edge := range filterFDs.fdEdges {
		// Rule #3.2, constant FD are removed from right side of left join.
		if edge.isConstant() {
			s.Rule333Equiv.Edges = append(s.Rule333Equiv.Edges, &fdEdge{
				from:   edge.from,
				to:     edge.to,
				strict: edge.strict,
				equiv:  edge.equiv,
			})
			s.Rule333Equiv.InnerCols = innerCols
			continue
		}
		// Rule #3.3, we only keep the lax FD from right side pointing the left side.
		if edge.equiv {
			// equivalence: {superset} --> {superset}, either `from` or `to` side is ok here.
			// Rule 3.3.1
			if !opt.SkipFDRule331 {
				leftCover := edge.from.Intersection(outerCols)
				rightCover := edge.from.Intersection(innerCols)
				if leftCover.Len() > 0 && rightCover.Len() > 0 {
					if opt.TypeFDRule331 == CombinedFD {
						leftCombinedFDFrom.UnionWith(leftCover)
						leftCombinedFDTo.UnionWith(rightCover)
					} else {
						// singleFD case.
						s.addFunctionalDependency(leftCover, rightCover, true, false)
					}
				}
			}

			// Rule 3.3.2
			equivColsRight := edge.from.Intersection(innerCols)
			equivColsLeft := edge.from.Intersection(outerCols)
			rightAllCols := copyRightFDSet.AllCols()
			leftAllCols := copyLeftFDSet.AllCols()
			coveringStrictKeyRight := rightAllCols.SubsetOf(copyRightFDSet.ClosureOfStrict(equivColsRight))
			coveringStrictKeyLeft := leftAllCols.SubsetOf(copyLeftFDSet.closureOfStrict(equivColsLeft))
			if coveringStrictKeyLeft && coveringStrictKeyRight {
				// find the minimum strict Key set, and add
				s.addFunctionalDependency(copyLeftFDSet.ReduceCols(equivColsLeft), rightAllCols.Union(leftAllCols), true, false)
			}

			// Rule 3.3.3
			// need to break down the superset of equivalence, adding each lax FD of them.
			laxFDFrom := equivColsRight
			laxFDTo := equivColsLeft
			for i, ok := laxFDFrom.Next(0); ok; i, ok = laxFDFrom.Next(i + 1) {
				for j, ok := laxFDTo.Next(0); ok; j, ok = laxFDTo.Next(j + 1) {
					s.addFunctionalDependency(NewFastIntSet(i), NewFastIntSet(j), false, false)
				}
			}
			s.Rule333Equiv.Edges = append(s.Rule333Equiv.Edges, &fdEdge{
				from:   laxFDFrom,
				to:     laxFDTo,
				strict: true,
				equiv:  true,
			})
			s.Rule333Equiv.InnerCols = innerCols
		}
		// Rule #3.1, filters won't produce any strict/lax FDs.
	}
	// Rule #3.3.1 combinedFD case
	if !opt.SkipFDRule331 && opt.TypeFDRule331 == CombinedFD {
		s.addFunctionalDependency(leftCombinedFDFrom, leftCombinedFDTo, true, false)
	}

	// Rule #4, add new FD {left key + right key} -> {all columns} if it could.
	if ok1 && ok2 {
		s.addFunctionalDependency(leftPK.Union(*rightPK), outerCols.Union(innerCols), true, false)
	}

	// Rule #5, adding the strict equiv edges if there's no join key and no filters from outside.
	if opt.OnlyInnerFilter {
		if opt.InnerIsFalse {
			s.AddConstants(innerCols)
		} else {
			for _, edge := range filterFDs.fdEdges {
				// keep filterFD's constant and equivalence.
				if edge.strict && (edge.equiv || edge.from.IsEmpty()) {
					s.addFunctionalDependency(edge.from, edge.to, edge.strict, edge.equiv)
				}
			}
			// keep all FDs from inner side.
			for _, edge := range innerFDs.fdEdges {
				s.addFunctionalDependency(edge.from, edge.to, edge.strict, edge.equiv)
			}
		}
	}

	// merge the not-null-cols/registered-map from both side together.
	s.NotNullCols.UnionWith(innerFDs.NotNullCols)
	s.NotNullCols.UnionWith(filterFDs.NotNullCols)
	// inner cols can be nullable since then.
	s.NotNullCols.DifferenceWith(innerCols)
	if s.HashCodeToUniqueID == nil {
		s.HashCodeToUniqueID = innerFDs.HashCodeToUniqueID
	} else {
		for k, v := range innerFDs.HashCodeToUniqueID {
			if _, ok := s.HashCodeToUniqueID[k]; ok {
				panic("shouldn't be here, children has same expr while registered not only once")
			}
			s.HashCodeToUniqueID[k] = v
		}
	}
	for i, ok := innerFDs.GroupByCols.Next(0); ok; i, ok = innerFDs.GroupByCols.Next(i + 1) {
		s.GroupByCols.Insert(i)
	}
	s.HasAggBuilt = s.HasAggBuilt || innerFDs.HasAggBuilt
}

func (s *FDSet) MakeRestoreRule333() {
	for _, eg := range s.Rule333Equiv.Edges {
		if eg.isConstant() {
			s.AddConstants(eg.to)
		} else {
			s.AddEquivalence(eg.from, eg.to)
		}
	}
	s.Rule333Equiv.Edges = nil
	s.Rule333Equiv.InnerCols.Clear()
}

type ArgOpts struct {
	SkipFDRule331   bool
	TypeFDRule331   TypeFilterFD331
	OnlyInnerFilter bool
	InnerIsFalse    bool
}

type TypeFilterFD331 byte

const (
	SingleFD   TypeFilterFD331 = 0
	CombinedFD TypeFilterFD331 = 1
)

func (s FDSet) FindPrimaryKey() (*FastIntSet, bool) {
	allCols := s.AllCols()
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		// Since we haven't maintained the key column, let's traverse every strict FD to judge with.
		if fd.strict && !fd.equiv {
			closure := s.closureOfStrict(fd.from)
			if allCols.SubsetOf(closure) {
				pk := NewFastIntSet()
				pk.CopyFrom(fd.from)
				return &pk, true
			}
		}
	}
	return nil, false
}

func (s FDSet) AllCols() FastIntSet {
	allCols := NewFastIntSet()
	for i := 0; i < len(s.fdEdges); i++ {
		allCols.UnionWith(s.fdEdges[i].from)
		if !s.fdEdges[i].equiv {
			allCols.UnionWith(s.fdEdges[i].to)
		}
	}
	return allCols
}

// AddFrom merges two FD sets by adding each FD from the given set to this set.
// Since two different tables may have some column ID overlap, we better use
// column unique ID to build the FDSet instead.
func (s *FDSet) AddFrom(fds *FDSet) {
	for i := range fds.fdEdges {
		fd := fds.fdEdges[i]
		if fd.equiv {
			s.addEquivalence(fd.from)
		} else if fd.isConstant() {
			s.AddConstants(fd.to)
		} else if fd.strict {
			s.AddStrictFunctionalDependency(fd.from, fd.to)
		} else {
			s.AddLaxFunctionalDependency(fd.from, fd.to)
		}
	}
	s.NotNullCols.UnionWith(fds.NotNullCols)
	if s.HashCodeToUniqueID == nil {
		s.HashCodeToUniqueID = fds.HashCodeToUniqueID
	} else {
		for k, v := range fds.HashCodeToUniqueID {
			if _, ok := s.HashCodeToUniqueID[k]; ok {
				panic("shouldn't be here, children has same expr while registered not only once")
			}
			s.HashCodeToUniqueID[k] = v
		}
	}
	for i, ok := fds.GroupByCols.Next(0); ok; i, ok = fds.GroupByCols.Next(i + 1) {
		s.GroupByCols.Insert(i)
	}
	s.HasAggBuilt = fds.HasAggBuilt
	s.Rule333Equiv = fds.Rule333Equiv
}

// MaxOneRow will regard every column in the fdSet as a constant. Since constant is stronger that strict FD, it will
// take over all existed strict/lax FD, only keeping the equivalence. Because equivalence is stronger than constant.
//
//   f:      {a}--> {b,c}, {abc} == {abc}
//   cols:   {a,c}
//   result: {} --> {a,c}, {a,c} == {a,c}
func (s *FDSet) MaxOneRow(cols FastIntSet) {
	cnt := 0
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		// non-equivalence FD, skip it.
		if !fd.equiv {
			continue
		}
		// equivalence: {superset} --> {superset}
		if cols.Intersects(fd.from) {
			s.fdEdges[cnt] = &fdEdge{
				from:   fd.from.Intersection(cols),
				to:     fd.to.Intersection(cols),
				strict: true,
				equiv:  true,
			}
			cnt++
		}
	}
	s.fdEdges = s.fdEdges[:cnt]
	// At last, add the constant FD, {} --> {cols}
	if !cols.IsEmpty() {
		s.fdEdges = append(s.fdEdges, &fdEdge{
			to:     cols,
			strict: true,
		})
	}
}

// ProjectCols projects FDSet to the target columns
// Formula:
// Strict decomposition FD4A: If X −→ Y Z then X −→ Y and X −→ Z.
// Lax decomposition FD4B: If X ~→ Y Z and I(R) is Y-definite then X ~→ Z.
func (s *FDSet) ProjectCols(cols FastIntSet) {
	// **************************************** START LOOP 1 ********************************************
	// Ensure the transitive relationship between remaining columns won't be lost.
	// 1: record all the constant columns
	// 2: if an FD's to side contain un-projected column, substitute it with its closure.
	// 		fd1: {a} --> {b,c}
	//	    fd2: {b} --> {d}
	//      when b is un-projected, the fd1 should be {a} --> {b,c's closure} which is {a} --> {b,c,d}
	// 3: track all columns that have equivalent alternates that are part of the projection.
	//		fd1: {a} --> {c}
	//      fd2: {a,b} == {a,b}
	//      if only a is un-projected, the fd1 can actually be kept as {b} --> {c}.
	var constCols, detCols, equivCols FastIntSet
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]

		if fd.isConstant() {
			constCols = fd.to
		}

		if !fd.to.SubsetOf(cols) {
			// equivalence FD has been the closure as {superset} == {superset}.
			if !fd.equiv && fd.strict {
				// extended the `to` as it's complete closure, in case of missing some transitive FDs.
				fd.to = s.closureOfStrict(fd.to.Union(fd.from))
				fd.to.DifferenceWith(fd.from)
			}
		}

		// {a,b} --> {c}, when b is un-projected, this FD should be handled latter, recording `b` here.
		if !fd.equiv && !fd.from.SubsetOf(cols) {
			detCols.UnionWith(fd.from.Difference(cols))
		}

		// equivalence {superset} == {superset}
		if fd.equiv && fd.from.Intersects(cols) {
			equivCols.UnionWith(fd.from)
		}
	}
	// ****************************************** END LOOP 1 ********************************************

	// find deleted columns with equivalence.
	detCols.IntersectionWith(equivCols)
	equivMap := s.makeEquivMap(detCols, cols)

	// it's actually maintained already.
	if !constCols.IsEmpty() {
		s.AddConstants(constCols)
	}

	// **************************************** START LOOP 2 ********************************************
	// leverage the information collected in the loop1 above and try to do some FD substitution.
	var (
		cnt    int
		newFDs []*fdEdge
	)
	for i := range s.fdEdges {
		fd := s.fdEdges[i]

		// step1: clear the `to` side
		// subtract out un-projected columns from dependants.
		// subtract out strict constant columns from dependants.
		if !fd.to.SubsetOf(cols) {
			// since loop 1 has computed the complete transitive closure for strict FD, now as:
			// 1: equivalence FD: {superset} == {superset}
			// 2: strict FD: {xxx} --> {complete closure}
			// 3: lax FD: {xxx} ~~> {yyy}
			if fd.equiv {
				// As formula FD4A above, delete from un-projected column from `to` side directly.
				fd.to = fd.to.Intersection(cols)
				// Since from are the same, delete it too here.
				fd.from = fd.from.Intersection(cols)
			} else if fd.strict {
				// As formula FD4A above, delete from un-projected column from `to` side directly.
				fd.to = fd.to.Intersection(cols)
			} else {
				// As formula FD4B above, only if the deleted columns are definite, then we can keep it.
				deletedCols := fd.to.Difference(cols)
				if deletedCols.SubsetOf(constCols) {
					fd.to = fd.to.Intersection(cols)
				} else if deletedCols.SubsetOf(s.NotNullCols) {
					fd.to = fd.to.Intersection(cols)
				} else {
					continue
				}
			}

			if !fd.isConstant() {
				// clear the constant columns in the dependency of FD.
				if fd.removeColumnsToSide(constCols) {
					continue
				}
			}
			if fd.removeColumnsToSide(fd.from) {
				// fd.to side is empty, remove this FD.
				continue
			}
		}

		// step2: clear the `from` side
		// substitute the equivalence columns for removed determinant columns.
		if !fd.from.SubsetOf(cols) {
			// equivalence and constant FD couldn't be here.
			deletedCols := fd.from.Difference(cols)
			substitutedCols := NewFastIntSet()
			foundAll := true
			for c, ok := deletedCols.Next(0); ok; c, ok = deletedCols.Next(c + 1) {
				// For every un-projected column, try to found their substituted column in projection list.
				var id int
				if id, foundAll = equivMap[c]; !foundAll {
					break
				}
				substitutedCols.Insert(id)
			}
			if foundAll {
				// deleted columns can be remapped using equivalencies.
				from := fd.from.Union(substitutedCols)
				from.DifferenceWith(deletedCols)
				newFDs = append(newFDs, &fdEdge{
					from:   from,
					to:     fd.to,
					strict: fd.strict,
					equiv:  fd.equiv,
				})
			}
			continue
		}

		if cnt != i {
			s.fdEdges[cnt] = s.fdEdges[i]
		}
		cnt++
	}
	s.fdEdges = s.fdEdges[:cnt]
	// ****************************************** END LOOP 2 ********************************************

	for i := range newFDs {
		fd := newFDs[i]
		if fd.equiv {
			s.addEquivalence(fd.from)
		} else if fd.isConstant() {
			s.AddConstants(fd.to)
		} else if fd.strict {
			s.AddStrictFunctionalDependency(fd.from, fd.to)
		} else {
			s.AddLaxFunctionalDependency(fd.from, fd.to)
		}
	}
}

// makeEquivMap try to find the equivalence column of every deleted column in the project list.
func (s *FDSet) makeEquivMap(detCols, projectedCols FastIntSet) map[int]int {
	var equivMap map[int]int
	for i, ok := detCols.Next(0); ok; i, ok = detCols.Next(i + 1) {
		var oneCol FastIntSet
		oneCol.Insert(i)
		closure := s.closureOfEquivalence(oneCol)
		closure.IntersectionWith(projectedCols)
		// the column to be deleted has an equivalence column exactly in the project list.
		if !closure.IsEmpty() {
			if equivMap == nil {
				equivMap = make(map[int]int)
			}
			id, _ := closure.Next(0) // 不应该只记录一个
			equivMap[i] = id
		}
	}
	return equivMap
}

// String returns format string of this FDSet.
func (s *FDSet) String() string {
	var builder strings.Builder

	for i := range s.fdEdges {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(s.fdEdges[i].String())
	}
	return builder.String()
}

// String returns format string of this FD.
func (e *fdEdge) String() string {
	var b strings.Builder
	if e.equiv {
		if !e.strict {
			panic(errors.New("lax equivalent columns are not supported"))
		}
		_, _ = fmt.Fprintf(&b, "%s==%s", e.from, e.to)
	} else {
		if e.strict {
			_, _ = fmt.Fprintf(&b, "%s-->%s", e.from, e.to)
		} else {
			_, _ = fmt.Fprintf(&b, "%s~~>%s", e.from, e.to)
		}
	}
	return b.String()
}

// RegisterUniqueID is used to record the map relationship between expr and allocated uniqueID.
func (s *FDSet) RegisterUniqueID(hashCode string, uniqueID int) {
	if len(hashCode) == 0 {
		// shouldn't be here.
		panic("map empty expr hashcode to uniqueID")
	}
	if _, ok := s.HashCodeToUniqueID[hashCode]; ok {
		// shouldn't be here.
		panic("hashcode has been registered")
	}
	s.HashCodeToUniqueID[hashCode] = uniqueID
}

func (s *FDSet) IsHashCodeRegistered(hashCode string) (int, bool) {
	if uniqueID, ok := s.HashCodeToUniqueID[hashCode]; ok {
		return uniqueID, true
	}
	return -1, false
}
