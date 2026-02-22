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

package schemacmp

// Tuple of Lattice instances. Given two Tuples `a` and `b`, we define `a <= b`
// iff `a[i] <= b[i]` for all `i`.
type Tuple []Lattice

// Unwrap implements Lattice. The returned type is a `[]interface{}`.
func (a Tuple) Unwrap() any {
	res := make([]any, 0, len(a))
	for _, value := range a {
		res = append(res, value.Unwrap())
	}
	return res
}

// Compare implements Lattice.
func (a Tuple) Compare(other Lattice) (int, error) {
	b, ok := other.(Tuple)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case len(a) != len(b):
		return 0, tupleLengthMismatchError(len(a), len(b))
	}

	result := 0
	for i, left := range a {
		res, err := left.Compare(b[i])
		if err != nil {
			return 0, wrapTupleIndexError(i, err)
		}
		result, err = CombineCompareResult(result, res)
		if err != nil {
			return 0, wrapTupleIndexError(i, err)
		}
	}
	return result, nil
}

// CombineCompareResult combines two comparison results.
func CombineCompareResult(x int, y int) (int, error) {
	switch {
	case x == y || y == 0:
		return x, nil
	case x == 0:
		return y, nil
	default:
		return 0, &IncompatibleError{
			Msg:  ErrMsgContradictingOrders,
			Args: []any{x, y},
		}
	}
}

// Join implements Lattice
func (a Tuple) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Tuple)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case len(a) != len(b):
		return nil, tupleLengthMismatchError(len(a), len(b))
	}

	result := make(Tuple, 0, len(a))
	for i, left := range a {
		res, err := left.Join(b[i])
		if err != nil {
			return nil, wrapTupleIndexError(i, err)
		}
		result = append(result, res)
	}
	return result, nil
}

type maybe struct{ Lattice }

// Maybe includes `nil` as the universal lower bound of the original Lattice.
func Maybe(inner Lattice) Lattice {
	return maybe{Lattice: inner}
}

// MaybeSingletonInterface is a convenient function calling `Maybe(Singleton(value))`.
func MaybeSingletonInterface(value any) Lattice {
	if value == nil {
		return Maybe(nil)
	}
	return Maybe(Singleton(value))
}

// MaybeSingletonString is a convenient function calling `Maybe(Singleton(s))`.
func MaybeSingletonString(s string) Lattice {
	if len(s) == 0 {
		return Maybe(nil)
	}
	return Maybe(Singleton(s))
}

// Unwrap implements Lattice.
func (a maybe) Unwrap() any {
	if a.Lattice != nil {
		return a.Lattice.Unwrap()
	}
	return nil
}

// Compare implements Lattice.
func (a maybe) Compare(other Lattice) (int, error) {
	b, ok := other.(maybe)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a.Lattice == nil && b.Lattice == nil:
		return 0, nil
	case a.Lattice == nil:
		return -1, nil
	case b.Lattice == nil:
		return 1, nil
	default:
		return a.Lattice.Compare(b.Lattice)
	}
}

// Join implements Lattice.
func (a maybe) Join(other Lattice) (Lattice, error) {
	b, ok := other.(maybe)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a.Lattice == nil:
		return b, nil
	case b.Lattice == nil:
		return a, nil
	default:
		join, err := a.Lattice.Join(b.Lattice)
		if err != nil {
			return nil, err
		}
		return maybe{Lattice: join}, nil
	}
}

// StringList is a list of string where `a <= b` iff `a == b[:len(a)]`.
type StringList []string

// Unwrap implements Lattice.
func (a StringList) Unwrap() any {
	return []string(a)
}

// Compare implements Lattice.
func (a StringList) Compare(other Lattice) (int, error) {
	b, ok := other.(StringList)
	if !ok {
		return 0, typeMismatchError(a, other)
	}
	minLen := min(len(a), len(b))
	for i := range minLen {
		if a[i] != b[i] {
			return 0, &IncompatibleError{
				Msg:  ErrMsgStringListElemMismatch,
				Args: []any{i, a[i], b[i]},
			}
		}
	}
	switch {
	case len(a) == len(b):
		return 0, nil
	case len(a) < len(b):
		return -1, nil
	default:
		return 1, nil
	}
}

// Join implements Lattice.
func (a StringList) Join(other Lattice) (Lattice, error) {
	cmp, err := a.Compare(other)
	switch {
	case err != nil:
		return nil, err
	case cmp <= 0:
		return other, nil
	default:
		return a, nil
	}
}

// LatticeMap is a map of Lattice objects keyed by strings.
type LatticeMap interface {
	// New creates an empty LatticeMap of the same type as the receiver.
	New() LatticeMap

	// Insert inserts a key-value pair into the map.
	Insert(key string, value Lattice)

	// Get obtains the Lattice object at the given key. Returns nil if the key
	// does not exist.
	Get(key string) Lattice

	// ForEach iterates the map.
	ForEach(func(key string, value Lattice) error) error

	// CompareWithNil returns the comparison result when the value is compared
	// with a non-existing entry.
	CompareWithNil(value Lattice) (int, error)

	// JoinWithNil returns the result when the value is joined with a
	// non-existing entry. If the joined result should be non-existing, this
	// method should return nil, nil.
	JoinWithNil(value Lattice) (Lattice, error)

	// ShouldDeleteIncompatibleJoin returns true if two incompatible entries
	// should be deleted instead of propagating the error.
	ShouldDeleteIncompatibleJoin() bool
}

type latticeMap struct{ LatticeMap }

// Unwrap implements Lattice.
func (a latticeMap) Unwrap() any {
	res := make(map[string]any)
	// TODO: add err handle
	_ = a.ForEach(func(key string, value Lattice) error {
		res[key] = value.Unwrap()
		return nil
	})
	return res
}

func (a latticeMap) iter(other Lattice, action func(k string, av, bv Lattice) error) error {
	b, ok := other.(latticeMap)
	if !ok {
		return typeMismatchError(a, other)
	}

	visitedKeys := make(map[string]struct{})
	err := a.ForEach(func(k string, av Lattice) error {
		visitedKeys[k] = struct{}{}
		return action(k, av, b.Get(k))
	})
	if err != nil {
		return err
	}

	return b.ForEach(func(k string, bv Lattice) error {
		if _, ok := visitedKeys[k]; ok {
			return nil
		}
		return action(k, nil, bv)
	})
}

// Compare implements Lattice.
func (a latticeMap) Compare(other Lattice) (int, error) {
	result := 0
	err := a.iter(other, func(k string, av, bv Lattice) error {
		var (
			cmpRes int
			e      error
		)
		switch {
		case av != nil && bv != nil:
			cmpRes, e = av.Compare(bv)
		case av != nil:
			cmpRes, e = a.CompareWithNil(av)
		default:
			cmpRes, e = a.CompareWithNil(bv)
			cmpRes = -cmpRes
		}
		if e != nil {
			return wrapMapKeyError(k, e)
		}
		result, e = CombineCompareResult(result, cmpRes)
		if e != nil {
			return wrapMapKeyError(k, e)
		}
		return nil
	})
	return result, err
}

// Join implements Lattice.
func (a latticeMap) Join(other Lattice) (Lattice, error) {
	result := a.New()
	err := a.iter(other, func(k string, av, bv Lattice) error {
		var (
			joinRes Lattice
			e       error
		)
		switch {
		case av != nil && bv != nil:
			joinRes, e = av.Join(bv)
		case av != nil:
			joinRes, e = a.JoinWithNil(av)
		default:
			joinRes, e = a.JoinWithNil(bv)
		}
		if e != nil && !a.ShouldDeleteIncompatibleJoin() {
			return wrapMapKeyError(k, e)
		}
		if e == nil && joinRes != nil {
			result.Insert(k, joinRes)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return latticeMap{LatticeMap: result}, nil
}

// Map wraps a LatticeMap instance into a Lattice.
func Map(lm LatticeMap) Lattice {
	return latticeMap{LatticeMap: lm}
}
