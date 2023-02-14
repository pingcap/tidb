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

import (
	"fmt"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
)

// IncompatibleError is the error type for incompatible schema.
type IncompatibleError struct {
	Msg  string
	Args []interface{}
}

const (
	// ErrMsgTypeMismatch is the error message for type mismatch.
	ErrMsgTypeMismatch = "type mismatch (%T vs %T)"
	// ErrMsgTupleLengthMismatch is the error message for tuple length mismatch.
	ErrMsgTupleLengthMismatch = "tuple length mismatch (%d vs %d)"
	// ErrMsgDistinctSingletons is the error message for distinct singletons.
	ErrMsgDistinctSingletons = "distinct singletons (%v vs %v)"
	// ErrMsgIncompatibleType is the error message for incompatible type.
	ErrMsgIncompatibleType = "incompatible mysql type (%v vs %v)"
	// ErrMsgAtTupleIndex is the error message for at tuple index.
	ErrMsgAtTupleIndex = "at tuple index %d: %v"
	// ErrMsgAtMapKey is the error message for at map key.
	ErrMsgAtMapKey = "at map key %q: %v"
	// ErrMsgNonInclusiveBitSets is the error message for non-inclusive bit sets.
	ErrMsgNonInclusiveBitSets = "non-inclusive bit sets (%#x vs %#x)"
	// ErrMsgContradictingOrders is the error message for contradicting orders.
	ErrMsgContradictingOrders = "combining contradicting orders (%d && %d)"
	// ErrMsgStringListElemMismatch is the error message for string list elem mismatch.
	ErrMsgStringListElemMismatch = "at string list index %d: distinct values (%q vs %q)"
)

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf(e.Msg, e.Args...)
}

func typeMismatchError(a, b Lattice) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgTypeMismatch,
		Args: []interface{}{a, b},
	}
}

func tupleLengthMismatchError(a, b int) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgTupleLengthMismatch,
		Args: []interface{}{a, b},
	}
}

func distinctSingletonsErrors(a, b interface{}) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgDistinctSingletons,
		Args: []interface{}{a, b},
	}
}

func incompatibleTypeError(a, b interface{}) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgIncompatibleType,
		Args: []interface{}{a, b},
	}
}

func wrapTupleIndexError(i int, inner error) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgAtTupleIndex,
		Args: []interface{}{i, inner},
	}
}

func wrapMapKeyError(key string, inner error) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgAtMapKey,
		Args: []interface{}{key, inner},
	}
}

// Lattice is implemented for types which forms a join-semilattice.
type Lattice interface {
	// Unwrap returns the underlying object supporting the lattice. This
	// operation is deep.
	Unwrap() interface{}

	// Compare this instance with another instance.
	//
	// Returns -1 if `self < other`, 0 if `self == other`, 1 if `self > other`.
	// Returns `IncompatibleError` if the two instances are not ordered.
	Compare(other Lattice) (int, error)

	// Join finds the "least upper bound" of two Lattice instances. The result
	// is `>=` both inputs. Returns an error if the join does not exist.
	Join(other Lattice) (Lattice, error)
}

// Bool is a boolean implementing Lattice where `false < true`.
type Bool bool

// Unwrap implements Lattice
func (a Bool) Unwrap() interface{} {
	return bool(a)
}

// Compare implements Lattice.
func (a Bool) Compare(other Lattice) (int, error) {
	b, ok := other.(Bool)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case bool(a):
		return 1, nil
	default:
		return -1, nil
	}
}

// Join implements Lattice
func (a Bool) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Bool)
	if !ok {
		return nil, typeMismatchError(a, other)
	}
	return a || b, nil
}

type singleton struct{ value interface{} }

// Unwrap implements Lattice
func (a singleton) Unwrap() interface{} {
	return a.value
}

// Singleton wraps an unordered value. Distinct instances of Singleton are
// incompatible.
func Singleton(value interface{}) Lattice {
	return singleton{value: value}
}

// Compare implements Lattice.
func (a singleton) Compare(other Lattice) (int, error) {
	b, ok := other.(singleton)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a.value != b.value:
		return 0, distinctSingletonsErrors(a.value, b.value)
	default:
		return 0, nil
	}
}

// Join implements Lattice
func (a singleton) Join(other Lattice) (Lattice, error) {
	b, ok := other.(singleton)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a.value != b.value:
		return nil, distinctSingletonsErrors(a.value, b.value)
	default:
		return a, nil
	}
}

// Equality allows custom equality.
type Equality interface {
	// Equals returns true if this instance should be equal to another object.
	Equals(other Equality) bool
}

type equalitySingleton struct{ Equality }

// EqualitySingleton wraps an unordered value with equality defined by custom
// code instead of the `==` operator.
func EqualitySingleton(value Equality) Lattice {
	return equalitySingleton{Equality: value}
}

// Unwrap implements Lattice.
func (a equalitySingleton) Unwrap() interface{} {
	return a.Equality
}

// Compare implements Lattice.
func (a equalitySingleton) Compare(other Lattice) (int, error) {
	b, ok := other.(equalitySingleton)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case !a.Equals(b.Equality):
		return 0, distinctSingletonsErrors(a.Equality, b.Equality)
	default:
		return 0, nil
	}
}

// Join implements Lattice
func (a equalitySingleton) Join(other Lattice) (Lattice, error) {
	b, ok := other.(equalitySingleton)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case !a.Equals(b.Equality):
		return nil, distinctSingletonsErrors(a.Equality, b.Equality)
	default:
		return a, nil
	}
}

// BitSet is a set of bits where `a < b` iff `a` is a subset of `b`.
type BitSet uint

// Unwrap implements Lattice.
func (a BitSet) Unwrap() interface{} {
	return uint(a)
}

// Compare implements Lattice.
func (a BitSet) Compare(other Lattice) (int, error) {
	b, ok := other.(BitSet)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case a&^b == 0:
		return -1, nil
	case b&^a == 0:
		return 1, nil
	default:
		return 0, &IncompatibleError{
			Msg:  ErrMsgNonInclusiveBitSets,
			Args: []interface{}{a, b},
		}
	}
}

// Join implements Lattice.
func (a BitSet) Join(other Lattice) (Lattice, error) {
	b, ok := other.(BitSet)
	if !ok {
		return nil, typeMismatchError(a, other)
	}
	return a | b, nil
}

// Byte is a byte implementing Lattice.
type Byte byte

// Unwrap implements Lattice.
func (a Byte) Unwrap() interface{} {
	return byte(a)
}

// Compare implements Lattice.
func (a Byte) Compare(other Lattice) (int, error) {
	b, ok := other.(Byte)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case a > b:
		return 1, nil
	default:
		return -1, nil
	}
}

// Join implements Lattice.
func (a Byte) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Byte)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a >= b:
		return a, nil
	default:
		return b, nil
	}
}

// fieldTp is a mysql column field type implementing lattice.
// It is used for the column field type (`github.com/pingcap/tidb/parser/types.FieldType.Tp`).
type fieldTp struct {
	value byte
}

// FieldTp is used for the column field type (`github.com/pingcap/tidb/parser/types.FieldType.Tp`).
func FieldTp(value byte) Lattice {
	return fieldTp{value: value}
}

// Unwrap implements Lattice
func (a fieldTp) Unwrap() interface{} {
	return a.value
}

// Compare implements Lattice.
func (a fieldTp) Compare(other Lattice) (int, error) {
	b, ok := other.(fieldTp)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.value == b.value {
		return 0, nil
	}

	// TODO: add more comparable type check here.
	// maybe we can ref https://github.com/pingcap/tidb/blob/38f4d869d86c3b274e7d1998a52243a30b125c80/types/field_type.go#L325 later.
	if mysql.IsIntegerType(a.value) && mysql.IsIntegerType(b.value) {
		// special handle for integer type.
		return compareMySQLIntegerType(a.value, b.value), nil
	}

	if types.IsTypeBlob(a.value) && types.IsTypeBlob(b.value) {
		// special handle for blob type.
		return compareMySQLBlobType(a.value, b.value), nil
	}

	return 0, incompatibleTypeError(a.value, b.value)
}

// Join implements Lattice.
func (a fieldTp) Join(other Lattice) (Lattice, error) {
	b, ok := other.(fieldTp)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	if a.value == b.value {
		return a, nil
	}

	if mysql.IsIntegerType(a.value) && mysql.IsIntegerType(b.value) {
		// special handle for integer type.
		if compareMySQLIntegerType(a.value, b.value) < 0 {
			return b, nil
		}
		return a, nil
	}

	if types.IsTypeBlob(a.value) && types.IsTypeBlob(b.value) {
		// special handle for blob type.
		if compareMySQLBlobType(a.value, b.value) < 0 {
			return b, nil
		}
		return a, nil
	}

	return nil, incompatibleTypeError(a.value, b.value)
}

// Int is an int implementing Lattice.
type Int int

// Unwrap implements Lattice.
func (a Int) Unwrap() interface{} {
	return int(a)
}

// Compare implements Lattice.
func (a Int) Compare(other Lattice) (int, error) {
	b, ok := other.(Int)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case a > b:
		return 1, nil
	default:
		return -1, nil
	}
}

// Join implements Lattice.
func (a Int) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Int)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a >= b:
		return a, nil
	default:
		return b, nil
	}
}

// Int64 is an int64 implementing Lattice.
type Int64 int64

// Unwrap implements Lattice.
func (a Int64) Unwrap() interface{} {
	return int64(a)
}

// Compare implements Lattice.
func (a Int64) Compare(other Lattice) (int, error) {
	b, ok := other.(Int64)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case a > b:
		return 1, nil
	default:
		return -1, nil
	}
}

// Join implements Lattice.
func (a Int64) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Int64)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a >= b:
		return a, nil
	default:
		return b, nil
	}
}

// Uint is a uint implementing Lattice.
type Uint uint

// Unwrap implements Lattice.
func (a Uint) Unwrap() interface{} {
	return uint(a)
}

// Compare implements Lattice.
func (a Uint) Compare(other Lattice) (int, error) {
	b, ok := other.(Uint)
	switch {
	case !ok:
		return 0, typeMismatchError(a, other)
	case a == b:
		return 0, nil
	case a > b:
		return 1, nil
	default:
		return -1, nil
	}
}

// Join implements Lattice.
func (a Uint) Join(other Lattice) (Lattice, error) {
	b, ok := other.(Uint)
	switch {
	case !ok:
		return nil, typeMismatchError(a, other)
	case a >= b:
		return a, nil
	default:
		return b, nil
	}
}

// Tuple of Lattice instances. Given two Tuples `a` and `b`, we define `a <= b`
// iff `a[i] <= b[i]` for all `i`.
type Tuple []Lattice

// Unwrap implements Lattice. The returned type is a `[]interface{}`.
func (a Tuple) Unwrap() interface{} {
	res := make([]interface{}, 0, len(a))
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
			Args: []interface{}{x, y},
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
func MaybeSingletonInterface(value interface{}) Lattice {
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
func (a maybe) Unwrap() interface{} {
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
func (a StringList) Unwrap() interface{} {
	return []string(a)
}

// Compare implements Lattice.
func (a StringList) Compare(other Lattice) (int, error) {
	b, ok := other.(StringList)
	if !ok {
		return 0, typeMismatchError(a, other)
	}
	minLen := len(a)
	if minLen > len(b) {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return 0, &IncompatibleError{
				Msg:  ErrMsgStringListElemMismatch,
				Args: []interface{}{i, a[i], b[i]},
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
func (a latticeMap) Unwrap() interface{} {
	res := make(map[string]interface{})
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
