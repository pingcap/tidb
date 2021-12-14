package functional_dependency

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

type FDSet struct {
	fdEdges []*fdEdge
}

// closureOf is to find closure of X with respect to F.
// A -> B  =  colSet -> { resultIntSet }
// eg: considering closure F: {A-> CD, B -> E}, and input is {AB}
// res: AB -> {CDE} (AB is included in trivial FD)
// The time complexity is O(n^2).
func (s *FDSet) closureOf(colSet FastIntSet) FastIntSet {
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
	}
	return resultSet
}

// inClosure is used to judge whether fd: setA -> setB can be inferred from closure s.
// It's a short-circuit version of the `closureOf`.
func (s *FDSet) inClosure(setA, setB FastIntSet) bool {
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
		if !s.inClosure(result, removed) {
			removed.Remove(k)
			result.Insert(k)
		}
	}
	return result
}

// AddStrictFunctionalDependency is to add functional dependency to the fdGraph, to reduce the edge number,
// we limit the functional dependency when we insert into the set. The key code of insert is like the following codes.
func (s *FDSet) AddStrictFunctionalDependency(from, to FastIntSet) {
	// trivial FD, refused.
	if to.SubsetOf(from) {
		return
	}

	// exclude the intersection part.
	if to.Intersects(from) {
		to.Difference(from)
	}

	// reduce the determinants.
	from = s.ReduceCols(from)

	newFD := &fdEdge{
		from:   from,
		to:     to,
		strict: true,
		equiv:  false,
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
			fd.strict = true
			fd.equiv = false
			added = true
		} else if !added {
			// There's a strong one. No need to add.
			if fd.implies(newFD) {
				added = true
			} else if fd.strict == true && fd.equiv == false && fd.from.Equals(from) {
				// We can use the new FD to extend the current one.
				// eg:  A -> BC, A -> CE, they couldn't be the subset of each other, union them.
				// res: A -> BCE
				fd.to.Union(to)
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
	// A --> C is stronger than AB --> C
	// A --> BC is stronger than A --> C.
	if e.from.SubsetOf(otherEdge.from) && otherEdge.to.SubsetOf(e.to) {
		// The given one should be weaker than the current one.
		// So the given one should not be more strict than the current one.
		// The given one should not be equivalence constraint if the given one is not equivalence constraint.
		if (e.strict || !otherEdge.strict) && (e.equiv || !otherEdge.equiv) {
			return true
		}
	}
	return false
}
