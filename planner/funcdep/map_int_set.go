package funcdep

import "sort"

// IntSet is used to hold set of vertexes of one side of an edge.
type IntSet map[int]struct{}

// SubsetOf is used to judge whether IntSet itself is a subset of others.
func (is IntSet) SubsetOf(target IntSet) bool {
	for i := range is {
		if _, ok := target[i]; ok {
			continue
		}
		return false
	}
	return true
}

// Intersects is used to judge whether IntSet itself intersects with others.
func (is IntSet) Intersects(target IntSet) bool {
	for i := range is {
		if _, ok := target[i]; ok {
			return true
		}
	}
	return false
}

// Difference is used to exclude the intersection sector away from itself.
func (is IntSet) Difference(target IntSet) {
	for i := range target {
		if _, ok := is[i]; ok {
			delete(is, i)
		}
	}
}

func (is IntSet) Difference2(target1, target2 IntSet) {
	for i := range target1 {
		if _, ok := target2[i]; ok {
			delete(is, i)
		} else {
			is[i] = struct{}{}
		}
	}
}

// Union is used to union the IntSet itself with others
func (is IntSet) Union(target IntSet) {
	// deduplicate
	for i := range target {
		if _, ok := is[i]; ok {
			continue
		}
		is[i] = struct{}{}
	}
}

// Equals is used to judge whether two IntSet are semantically equal.
func (is IntSet) Equals(target IntSet) bool {
	if len(is) != len(target) {
		return false
	}
	for i := range target {
		if _, ok := is[i]; !ok {
			return false
		}
	}
	return true
}

func (is *IntSet) CopyFrom(target IntSet) {
	*is = NewIntSetWithCap(len(target))
	for k, v := range target {
		(*is)[k] = v
	}
}

func (is IntSet) SortedArray() []int {
	arr := make([]int, 0, len(is))
	for k := range is {
		arr = append(arr, k)
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

func (is IntSet) Insert(k int) {
	is[k] = struct{}{}
}

func NewIntSet() IntSet {
	return make(map[int]struct{})
}

func NewIntSetWithCap(cap int) IntSet {
	return make(map[int]struct{}, cap)
}
