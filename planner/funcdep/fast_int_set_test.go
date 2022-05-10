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
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/tools/container/intsets"
)

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
		delete(is, i)
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
	maps.Copy(*is, target)
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

func NewIntSetWithCap(c int) IntSet {
	return make(map[int]struct{}, c)
}

func TestFastIntSetBasic(t *testing.T) {
	// Test Insert, Remove, Len, Has.
	fis := FastIntSet{}
	fis.Insert(1)
	fis.Insert(2)
	fis.Insert(3)
	require.Equal(t, fis.Len(), 3)
	require.True(t, fis.Has(1))
	require.True(t, fis.Has(2))
	require.True(t, fis.Has(3))
	fis.Remove(2)
	require.Equal(t, fis.Len(), 2)
	require.True(t, fis.Has(1))
	require.True(t, fis.Has(3))
	fis.Remove(3)
	require.Equal(t, fis.Len(), 1)
	require.True(t, fis.Has(1))
	fis.Remove(1)
	require.Equal(t, fis.Len(), 0)

	// Test Next (only seek non-neg)
	fis.Insert(6)
	fis.Insert(3)
	fis.Insert(0)
	fis.Insert(-1)
	fis.Insert(77)
	n, ok := fis.Next(intsets.MinInt)
	require.True(t, ok)
	require.Equal(t, n, 0)
	n, ok = fis.Next(n + 1)
	require.True(t, ok)
	require.Equal(t, n, 3)
	n, ok = fis.Next(n + 1)
	require.True(t, ok)
	require.Equal(t, n, 6)
	n, ok = fis.Next(n + 1)
	require.True(t, ok)
	require.Equal(t, n, 77)
	n, ok = fis.Next(n + 1)
	require.False(t, ok)
	require.Equal(t, n, intsets.MaxInt)

	// Test Clear and IsEmpty.
	fis.Clear()
	require.Equal(t, fis.Len(), 0)
	require.True(t, fis.IsEmpty())

	// Test ForEach (seek all) and SortedArray.
	fis.Insert(1)
	fis.Insert(-1)
	fis.Insert(77)
	var res []int
	fis.ForEach(func(i int) {
		res = append(res, i)
	})
	res1 := fis.SortedArray()
	require.Equal(t, len(res), 3)
	require.Equal(t, len(res1), 3)
	require.Equal(t, res, res1)

	// Test Copy,  CopyFrom and Equal
	cp := fis.Copy()
	require.Equal(t, fis.Len(), cp.Len())
	require.Equal(t, fis.SortedArray(), cp.SortedArray())
	require.True(t, fis.Equals(cp))

	cpf := FastIntSet{}
	intervene := 100
	cpf.Insert(intervene)
	cpf.CopyFrom(fis)
	require.Equal(t, cpf.Len(), cp.Len())
	require.Equal(t, cpf.SortedArray(), cp.SortedArray())
	require.True(t, cpf.Equals(cp))
}

func getTestName() string {
	pcs := make([]uintptr, 10)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		fxn := frame.Function
		if strings.Contains(fxn, ".Test") {
			return fxn
		}
		if !more {
			break
		}
	}
	return ""
}

var lastTestName string
var rng *rand.Rand

func NewTestRand() (*rand.Rand, int64) {
	fxn := getTestName()
	if fxn != "" && lastTestName != fxn {
		// Re-seed rng (the source of seeds for test random number generators) with
		// the global seed so that individual tests are reproducible using the
		// random seed.
		lastTestName = fxn
		rng = rand.New(rand.NewSource(time.Now().Unix()))
	}
	seed := rng.Int63()
	return rand.New(rand.NewSource(seed)), seed
}

func TestFastIntSet(t *testing.T) {
	for _, mVal := range []int{1, 8, 30, smallCutOff, 2 * smallCutOff, 4 * smallCutOff} {
		m := mVal
		t.Run(fmt.Sprintf("%d", m), func(t *testing.T) {
			rng, _ := NewTestRand()
			in := make([]bool, m)
			forEachRes := make([]bool, m)

			var s FastIntSet
			for i := 0; i < 1000; i++ {
				v := rng.Intn(m)
				if rng.Intn(2) == 0 {
					in[v] = true
					s.Insert(v)
				} else {
					in[v] = false
					s.Remove(v)
				}
				empty := true
				for j := 0; j < m; j++ {
					empty = empty && !in[j]
					if in[j] != s.Has(j) {
						t.Fatalf("incorrect result for Contains(%d), expected %t", j, in[j])
					}
				}
				if empty != s.IsEmpty() {
					t.Fatalf("incorrect result for Empty(), expected %t", empty)
				}
				// Test ForEach
				for j := range forEachRes {
					forEachRes[j] = false
				}
				s.ForEach(func(j int) {
					forEachRes[j] = true
				})
				for j := 0; j < m; j++ {
					if in[j] != forEachRes[j] {
						t.Fatalf("incorrect ForEachResult for %d (%t, expected %t)", j, forEachRes[j], in[j])
					}
				}
				// Cross-check Ordered and Next().
				var vals []int
				for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
					vals = append(vals, i)
				}
				if o := s.SortedArray(); !reflect.DeepEqual(vals, o) {
					t.Fatalf("set built with Next doesn't match Ordered: %v vs %v", vals, o)
				}
				assertSame := func(orig, copied FastIntSet) {
					t.Helper()
					if !orig.Equals(copied) || !copied.Equals(orig) {
						t.Fatalf("expected equality: %v, %v", orig, copied)
					}
					if col, ok := copied.Next(0); ok {
						copied.Remove(col)
						if orig.Equals(copied) || copied.Equals(orig) {
							t.Fatalf("unexpected equality: %v, %v", orig, copied)
						}
						copied.Insert(col)
						if !orig.Equals(copied) || !copied.Equals(orig) {
							t.Fatalf("expected equality: %v, %v", orig, copied)
						}
					}
				}
				// Test Copy.
				s2 := s.Copy()
				assertSame(s, s2)
				// Test CopyFrom.
				var s3 FastIntSet
				s3.CopyFrom(s)
				assertSame(s, s3)
				// Make sure CopyFrom into a non-empty set still works.
				s.Shift(100)
				s.CopyFrom(s3)
				assertSame(s, s3)
			}
		})
	}
}

func TestFastIntSetTwoSetOps(t *testing.T) {
	rng, _ := NewTestRand()
	// genSet creates a set of numElem values in [minVal, minVal + valRange)
	// It also adds and then removes numRemoved elements.
	genSet := func(numElem, numRemoved, minVal, valRange int) (FastIntSet, map[int]bool) {
		var s FastIntSet
		vals := rng.Perm(valRange)[:numElem+numRemoved]
		used := make(map[int]bool, len(vals))
		for _, i := range vals {
			used[i] = true
		}
		for k := range used {
			s.Insert(k)
		}
		p := rng.Perm(len(vals))
		for i := 0; i < numRemoved; i++ {
			k := vals[p[i]]
			s.Remove(k)
			delete(used, k)
		}
		return s, used
	}

	// returns true if a is a subset of b
	subset := func(a, b map[int]bool) bool {
		for k := range a {
			if !b[k] {
				return false
			}
		}
		return true
	}

	for _, minVal := range []int{-10, -1, 0, smallCutOff, 2 * smallCutOff} {
		for _, valRange := range []int{0, 20, 200} {
			for _, num1 := range []int{0, 1, 5, 10, 20} {
				for _, removed1 := range []int{0, 1, 3, 8} {
					s1, m1 := genSet(num1, removed1, minVal, num1+removed1+valRange)
					for _, shift := range []int{-100, -10, -1, 1, 2, 10, 100} {
						shifted := s1.Shift(shift)
						failed := false
						s1.ForEach(func(i int) {
							failed = failed || !shifted.Has(i+shift)
						})
						shifted.ForEach(func(i int) {
							failed = failed || !s1.Has(i-shift)
						})
						if failed {
							t.Errorf("invalid shifted result: %s shifted by %d: %s", &s1, shift, &shifted)
						}
					}
					for _, num2 := range []int{0, 1, 5, 10, 20} {
						for _, removed2 := range []int{0, 1, 4, 10} {
							s2, m2 := genSet(num2, removed2, minVal, num2+removed2+valRange)

							subset1 := subset(m1, m2)
							if subset1 != s1.SubsetOf(s2) {
								t.Errorf("SubsetOf result incorrect: %s, %s", &s1, &s2)
							}
							subset2 := subset(m2, m1)
							if subset2 != s2.SubsetOf(s1) {
								t.Errorf("SubsetOf result incorrect: %s, %s", &s2, &s1)
							}
							eq := subset1 && subset2
							if eq != s1.Equals(s2) || eq != s2.Equals(s1) {
								t.Errorf("Equals result incorrect: %s, %s", &s1, &s2)
							}

							// Test union.

							u := s1.Copy()
							u.UnionWith(s2)

							if !u.Equals(s1.Union(s2)) {
								t.Errorf("inconsistency between UnionWith and Union on %s %s\n", s1, s2)
							}
							// Verify all elements from m1 and m2 are in u.
							for _, m := range []map[int]bool{m1, m2} {
								for x := range m {
									if !u.Has(x) {
										t.Errorf("incorrect union result %s union %s = %s", &s1, &s2, &u)
										break
									}
								}
							}
							// Verify all elements from u are in m2 or m1.
							for x, ok := u.Next(minVal); ok; x, ok = u.Next(x + 1) {
								if !(m1[x] || m2[x]) {
									t.Errorf("incorrect union result %s union %s = %s", &s1, &s2, &u)
									break
								}
							}

							// Test intersection.
							u = s1.Copy()
							u.IntersectionWith(s2)
							if s1.Intersects(s2) != !u.IsEmpty() ||
								s2.Intersects(s1) != !u.IsEmpty() {
								t.Errorf("inconsistency between IntersectionWith and Intersect on %s %s\n", s1, s2)
							}
							if !u.Equals(s1.Intersection(s2)) {
								t.Errorf("inconsistency between IntersectionWith and Intersection on %s %s\n", s1, s2)
							}
							// Verify all elements from m1 and m2 are in u.
							for x := range m1 {
								if m2[x] && !u.Has(x) {
									t.Errorf("incorrect intersection result %s union %s = %s  x=%d", &s1, &s2, &u, x)
									break
								}
							}
							// Verify all elements from u are in m2 and m1.
							for x, ok := u.Next(minVal); ok; x, ok = u.Next(x + 1) {
								if !(m1[x] && m2[x]) {
									t.Errorf("incorrect intersection result %s intersect %s = %s", &s1, &s2, &u)
									break
								}
							}

							// Test difference.
							u = s1.Copy()
							u.DifferenceWith(s2)

							if !u.Equals(s1.Difference(s2)) {
								t.Errorf("inconsistency between DifferenceWith and Difference on %s %s\n", s1, s2)
							}

							// Verify all elements in m1 but not in m2 are in u.
							for x := range m1 {
								if !m2[x] && !u.Has(x) {
									t.Errorf("incorrect difference result %s \\ %s = %s  x=%d", &s1, &s2, &u, x)
									break
								}
							}
							// Verify all elements from u are in m1.
							for x, ok := u.Next(minVal); ok; x, ok = u.Next(x + 1) {
								if !m1[x] {
									t.Errorf("incorrect difference result %s \\ %s = %s", &s1, &s2, &u)
									break
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestFastIntSetAddRange(t *testing.T) {
	assertSet := func(set *FastIntSet, from, to int) {
		t.Helper()
		// Iterate through the set and ensure that the values
		// it contain are the values from 'from' to 'to' (inclusively).
		expected := from
		set.ForEach(func(actual int) {
			t.Helper()
			if actual > to {
				t.Fatalf("expected last value in FastIntSet to be %d, got %d", to, actual)
			}
			if expected != actual {
				t.Fatalf("expected next value in FastIntSet to be %d, got %d", expected, actual)
			}
			expected++
		})
	}

	max := smallCutOff + 20
	// Test all O(n^2) sub-intervals of [from,to] in the interval
	// [-5, smallCutoff + 20].
	for from := -5; from <= max; from++ {
		for to := from; to <= max; to++ {
			var set FastIntSet
			set.AddRange(from, to)
			assertSet(&set, from, to)
		}
	}
}

func TestFastIntSetString(t *testing.T) {
	testCases := []struct {
		vals []int
		exp  string
	}{
		{
			vals: []int{},
			exp:  "()",
		},
		{
			vals: []int{-5, -3, -2, -1, 0, 1, 2, 3, 4, 5},
			exp:  "(-5,-3,-2,-1,0-5)",
		},
		{
			vals: []int{0, 1, 3, 4, 5},
			exp:  "(0,1,3-5)",
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			s := NewFastIntSet(tc.vals...)
			if str := s.String(); str != tc.exp {
				t.Errorf("expected %s, got %s", tc.exp, str)
			}
		})
	}
}
