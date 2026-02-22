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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// IncompatibleError is the error type for incompatible schema.
type IncompatibleError struct {
	Msg  string
	Args []any
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
		Args: []any{a, b},
	}
}

func tupleLengthMismatchError(a, b int) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgTupleLengthMismatch,
		Args: []any{a, b},
	}
}

func distinctSingletonsErrors(a, b any) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgDistinctSingletons,
		Args: []any{a, b},
	}
}

func incompatibleTypeError(a, b any) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgIncompatibleType,
		Args: []any{a, b},
	}
}

func wrapTupleIndexError(i int, inner error) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgAtTupleIndex,
		Args: []any{i, inner},
	}
}

func wrapMapKeyError(key string, inner error) *IncompatibleError {
	return &IncompatibleError{
		Msg:  ErrMsgAtMapKey,
		Args: []any{key, inner},
	}
}

// Lattice is implemented for types which forms a join-semilattice.
type Lattice interface {
	// Unwrap returns the underlying object supporting the lattice. This
	// operation is deep.
	Unwrap() any

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
func (a Bool) Unwrap() any {
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

type singleton struct{ value any }

// Unwrap implements Lattice
func (a singleton) Unwrap() any {
	return a.value
}

// Singleton wraps an unordered value. Distinct instances of Singleton are
// incompatible.
func Singleton(value any) Lattice {
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
func (a equalitySingleton) Unwrap() any {
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
func (a BitSet) Unwrap() any {
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
			Args: []any{a, b},
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
func (a Byte) Unwrap() any {
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
// It is used for the column field type (`github.com/pingcap/tidb/pkg/parser/types.FieldType.Tp`).
type fieldTp struct {
	value byte
}

// FieldTp is used for the column field type (`github.com/pingcap/tidb/pkg/parser/types.FieldType.Tp`).
func FieldTp(value byte) Lattice {
	return fieldTp{value: value}
}

// Unwrap implements Lattice
func (a fieldTp) Unwrap() any {
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
func (a Int) Unwrap() any {
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
func (a Int64) Unwrap() any {
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
func (a Uint) Unwrap() any {
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
