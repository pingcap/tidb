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

package schemacmp_test

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	. "github.com/pingcap/tidb/util/schemacmp"
	"github.com/stretchr/testify/require"
)

// eqBytes is a sample type used for testing EqualitySingleton.
type eqBytes []byte

func (a eqBytes) Equals(other Equality) bool {
	b, ok := other.(eqBytes)
	return ok && bytes.Equal(a, b)
}

// uintMap is a sample type used for testing Map.
type uintMap map[string]uint

// New creates an empty LatticeMap of the same type as the receiver.
func (uintMap) New() LatticeMap {
	return make(uintMap)
}

func (a uintMap) Insert(key string, value Lattice) {
	a[key] = uint(value.(Uint))
}

func (a uintMap) Get(key string) Lattice {
	res, ok := a[key]
	if !ok {
		return nil
	}
	return Uint(res)
}

func (a uintMap) ForEach(f func(key string, value Lattice) error) error {
	for k, v := range a {
		if err := f(k, Uint(v)); err != nil {
			return err
		}
	}
	return nil
}

func (uintMap) CompareWithNil(Lattice) (int, error) {
	return 1, nil
}

func (uintMap) JoinWithNil(value Lattice) (Lattice, error) {
	return value, nil
}

func (uintMap) ShouldDeleteIncompatibleJoin() bool {
	return true
}

func TestCompatibilities(t *testing.T) {
	testCases := []struct {
		a             Lattice
		b             Lattice
		compareResult int
		compareError  string
		join          Lattice
		joinError     string
	}{
		{
			a:             Bool(false),
			b:             Bool(false),
			compareResult: 0,
			join:          Bool(false),
		},
		{
			a:             Bool(false),
			b:             Bool(true),
			compareResult: -1,
			join:          Bool(true),
		},
		{
			a:             Bool(true),
			b:             Bool(true),
			compareResult: 0,
			join:          Bool(true),
		},
		{
			a:             Singleton(123),
			b:             Singleton(123),
			compareResult: 0,
			join:          Singleton(123),
		},
		{
			a:            Singleton(123),
			b:            Singleton(2468),
			compareError: `distinct singletons.*`,
			joinError:    `distinct singletons.*`,
		},
		{
			a:            BitSet(0b010110),
			b:            BitSet(0b110001),
			compareError: `non-inclusive bit sets.*`,
			join:         BitSet(0b110111),
		},
		{
			a:             BitSet(0xffffffff),
			b:             BitSet(0),
			compareResult: 1,
			join:          BitSet(0xffffffff),
		},
		{
			a:             BitSet(0b10001),
			b:             BitSet(0b11011),
			compareResult: -1,
			join:          BitSet(0b11011),
		},
		{
			a:             BitSet(0x522),
			b:             BitSet(0x522),
			compareResult: 0,
			join:          BitSet(0x522),
		},
		{
			a:             Byte(123),
			b:             Byte(123),
			compareResult: 0,
			join:          Byte(123),
		},
		{
			a:             Byte(1),
			b:             Byte(23),
			compareResult: -1,
			join:          Byte(23),
		},
		{
			a:             Byte(123),
			b:             Byte(45),
			compareResult: 1,
			join:          Byte(123),
		},
		{
			a:            Tuple{Byte(123), Bool(false)},
			b:            Tuple{Byte(67), Bool(true)},
			compareError: `at tuple index 1: combining contradicting orders.*`,
			join:         Tuple{Byte(123), Bool(true)},
		},
		{
			a:             Tuple{},
			b:             Tuple{},
			compareResult: 0,
			join:          Tuple{},
		},
		{
			a:            Tuple{Singleton(6), Singleton(7)},
			b:            Tuple{Singleton(6), Singleton(8)},
			compareError: `at tuple index 1: distinct singletons.*`,
			joinError:    `at tuple index 1: distinct singletons.*`,
		},
		{
			a:            Tuple{},
			b:            Tuple{Bool(false)},
			compareError: `tuple length mismatch.*`,
			joinError:    `tuple length mismatch.*`,
		},
		{
			a:            Bool(false),
			b:            Singleton(false),
			compareError: `type mismatch.*`,
			joinError:    `type mismatch.*`,
		},
		{
			a:            Maybe(Singleton(123)),
			b:            Maybe(Singleton(678)),
			compareError: `distinct singletons.*`,
			joinError:    `distinct singletons.*`,
		},
		{
			a:             Maybe(Byte(111)),
			b:             Maybe(Byte(222)),
			compareResult: -1,
			join:          Maybe(Byte(222)),
		},
		{
			a:             Maybe(nil),
			b:             Maybe(Singleton(135)),
			compareResult: -1,
			join:          Maybe(Singleton(135)),
		},
		{
			a:             Maybe(nil),
			b:             Maybe(nil),
			compareResult: 0,
			join:          Maybe(nil),
		},
		{
			a:            Bool(false),
			b:            Maybe(Bool(false)),
			compareError: `type mismatch.*`,
			joinError:    `type mismatch.*`,
		},
		{
			a:             StringList{"one", "two", "three"},
			b:             StringList{"one", "two", "three", "four", "five"},
			compareResult: -1,
			join:          StringList{"one", "two", "three", "four", "five"},
		},
		{
			a:            StringList{"one", "two", "three"},
			b:            StringList{"two", "three"},
			compareError: `at string list index 0: distinct values.*`,
			joinError:    `at string list index 0: distinct values.*`,
		},
		{
			a:            StringList{"a", "b", "c"},
			b:            StringList{"a", "e", "i", "o", "u"},
			compareError: `at string list index 1: distinct values.*`,
			joinError:    `at string list index 1: distinct values.*`,
		},
		{
			a:             StringList{},
			b:             StringList{},
			compareResult: 0,
			join:          StringList{},
		},
		{
			a:             EqualitySingleton(eqBytes("abcdef")),
			b:             EqualitySingleton(eqBytes("abcdef")),
			compareResult: 0,
			join:          EqualitySingleton(eqBytes("abcdef")),
		},
		{
			a:            EqualitySingleton(eqBytes("abcdef")),
			b:            EqualitySingleton(eqBytes("ABCDEF")),
			compareError: `distinct singletons.*`,
			joinError:    `distinct singletons.*`,
		},
		{
			a:            EqualitySingleton(eqBytes("abcdef")),
			b:            Singleton(eqBytes("ABCDEF")),
			compareError: `type mismatch.*`,
			joinError:    `type mismatch.*`,
		},
		{
			a:             Int64(234),
			b:             Int64(-5),
			compareResult: 1,
			join:          Int64(234),
		},
		{
			a:             Uint(665544),
			b:             Uint(765),
			compareResult: 1,
			join:          Uint(665544),
		},
		{
			a:            Map(uintMap{"a": 123, "b": 678, "c": 456}),
			b:            Map(uintMap{"a": 234, "b": 567, "d": 789}),
			compareError: `.*combining contradicting orders.*`,
			join:         Map(uintMap{"a": 234, "b": 678, "c": 456, "d": 789}),
		},
		{
			a:             Map(uintMap{"a": 123, "b": 678, "c": 456}),
			b:             Map(uintMap{"a": 1, "c": 4}),
			compareResult: 1,
			join:          Map(uintMap{"a": 123, "b": 678, "c": 456}),
		},
		// TypeTiny compare/join with other integer types.
		{
			a:             FieldTp(mysql.TypeTiny),
			b:             FieldTp(mysql.TypeTiny),
			compareResult: 0,
			join:          FieldTp(mysql.TypeTiny),
		},
		{
			a:             FieldTp(mysql.TypeTiny),
			b:             FieldTp(mysql.TypeShort),
			compareResult: -1,
			join:          FieldTp(mysql.TypeShort),
		},
		{
			a:             FieldTp(mysql.TypeTiny),
			b:             FieldTp(mysql.TypeInt24),
			compareResult: -1,
			join:          FieldTp(mysql.TypeInt24),
		},
		{
			a:             FieldTp(mysql.TypeTiny),
			b:             FieldTp(mysql.TypeLong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeTiny),
			b:             FieldTp(mysql.TypeLonglong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		// TypeShort compare/join with other integer types.
		{
			a:             FieldTp(mysql.TypeShort),
			b:             FieldTp(mysql.TypeTiny),
			compareResult: 1,
			join:          FieldTp(mysql.TypeShort),
		},
		{
			a:             FieldTp(mysql.TypeShort),
			b:             FieldTp(mysql.TypeShort),
			compareResult: 0,
			join:          FieldTp(mysql.TypeShort),
		},
		{
			a:             FieldTp(mysql.TypeShort),
			b:             FieldTp(mysql.TypeInt24),
			compareResult: -1,
			join:          FieldTp(mysql.TypeInt24),
		},
		{
			a:             FieldTp(mysql.TypeShort),
			b:             FieldTp(mysql.TypeLong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeShort),
			b:             FieldTp(mysql.TypeLonglong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		// TypeInt24 compare/join with other integer types.
		{
			a:             FieldTp(mysql.TypeInt24),
			b:             FieldTp(mysql.TypeTiny),
			compareResult: 1,
			join:          FieldTp(mysql.TypeInt24),
		},
		{
			a:             FieldTp(mysql.TypeInt24),
			b:             FieldTp(mysql.TypeShort),
			compareResult: 1,
			join:          FieldTp(mysql.TypeInt24),
		},
		{
			a:             FieldTp(mysql.TypeInt24),
			b:             FieldTp(mysql.TypeInt24),
			compareResult: 0,
			join:          FieldTp(mysql.TypeInt24),
		},
		{
			a:             FieldTp(mysql.TypeInt24),
			b:             FieldTp(mysql.TypeLong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeInt24),
			b:             FieldTp(mysql.TypeLonglong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		// TypeLong compare/join with other integer types.
		{
			a:             FieldTp(mysql.TypeLong),
			b:             FieldTp(mysql.TypeTiny),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeLong),
			b:             FieldTp(mysql.TypeShort),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeLong),
			b:             FieldTp(mysql.TypeInt24),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeLong),
			b:             FieldTp(mysql.TypeLong),
			compareResult: 0,
			join:          FieldTp(mysql.TypeLong),
		},
		{
			a:             FieldTp(mysql.TypeLong),
			b:             FieldTp(mysql.TypeLonglong),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		// TypeLonglong compare/join with other integer types.
		{
			a:             FieldTp(mysql.TypeLonglong),
			b:             FieldTp(mysql.TypeTiny),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		{
			a:             FieldTp(mysql.TypeLonglong),
			b:             FieldTp(mysql.TypeShort),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		{
			a:             FieldTp(mysql.TypeLonglong),
			b:             FieldTp(mysql.TypeInt24),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		{
			a:             FieldTp(mysql.TypeLonglong),
			b:             FieldTp(mysql.TypeLong),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLonglong),
		},
		{
			a:             FieldTp(mysql.TypeLonglong),
			b:             FieldTp(mysql.TypeLonglong),
			compareResult: 0,
			join:          FieldTp(mysql.TypeLonglong),
		},
		// TypeTinyBlob compare/join with other blob types.
		{
			a:             FieldTp(mysql.TypeTinyBlob),
			b:             FieldTp(mysql.TypeTinyBlob),
			compareResult: 0,
			join:          FieldTp(mysql.TypeTinyBlob),
		},
		{
			a:             FieldTp(mysql.TypeTinyBlob),
			b:             FieldTp(mysql.TypeBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeBlob),
		},
		{
			a:             FieldTp(mysql.TypeTinyBlob),
			b:             FieldTp(mysql.TypeMediumBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeMediumBlob),
		},
		{
			a:             FieldTp(mysql.TypeTinyBlob),
			b:             FieldTp(mysql.TypeLongBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		// TypeBlob compare/join with other blob types.
		{
			a:             FieldTp(mysql.TypeBlob),
			b:             FieldTp(mysql.TypeTinyBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeBlob),
		},
		{
			a:             FieldTp(mysql.TypeBlob),
			b:             FieldTp(mysql.TypeBlob),
			compareResult: 0,
			join:          FieldTp(mysql.TypeBlob),
		},
		{
			a:             FieldTp(mysql.TypeBlob),
			b:             FieldTp(mysql.TypeMediumBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeMediumBlob),
		},
		{
			a:             FieldTp(mysql.TypeBlob),
			b:             FieldTp(mysql.TypeLongBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		// TypeMediumBlob compare/join with other blob types.
		{
			a:             FieldTp(mysql.TypeMediumBlob),
			b:             FieldTp(mysql.TypeTinyBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeMediumBlob),
		},
		{
			a:             FieldTp(mysql.TypeMediumBlob),
			b:             FieldTp(mysql.TypeBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeMediumBlob),
		},
		{
			a:             FieldTp(mysql.TypeMediumBlob),
			b:             FieldTp(mysql.TypeMediumBlob),
			compareResult: 0,
			join:          FieldTp(mysql.TypeMediumBlob),
		},
		{
			a:             FieldTp(mysql.TypeMediumBlob),
			b:             FieldTp(mysql.TypeLongBlob),
			compareResult: -1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		// TypeLongBlob compare/join with other blob types.
		{
			a:             FieldTp(mysql.TypeLongBlob),
			b:             FieldTp(mysql.TypeTinyBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		{
			a:             FieldTp(mysql.TypeLongBlob),
			b:             FieldTp(mysql.TypeBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		{
			a:             FieldTp(mysql.TypeLongBlob),
			b:             FieldTp(mysql.TypeMediumBlob),
			compareResult: 1,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		{
			a:             FieldTp(mysql.TypeLongBlob),
			b:             FieldTp(mysql.TypeLongBlob),
			compareResult: 0,
			join:          FieldTp(mysql.TypeLongBlob),
		},
		// type mismatch or incompatible.
		{
			a:            FieldTp(mysql.TypeLong),
			b:            Singleton(false),
			compareError: `type mismatch.*`,
			joinError:    `type mismatch.*`,
		},
		{
			a:            FieldTp(mysql.TypeLong),
			b:            FieldTp(mysql.TypeSet),
			compareError: `incompatible mysql type.*`,
			joinError:    `incompatible mysql type.*`,
		},
	}

	for _, tc := range testCases {

		cmp, err := tc.a.Compare(tc.b)
		if len(tc.compareError) != 0 {
			require.IsType(t, &IncompatibleError{}, err)
			require.Regexp(t, tc.compareError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.compareResult, cmp)
		}

		cmp, err = tc.b.Compare(tc.a)
		if len(tc.compareError) != 0 {
			require.IsType(t, &IncompatibleError{}, err)
			require.Regexp(t, tc.compareError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, -tc.compareResult, cmp)
		}

		join, err := tc.a.Join(tc.b)
		if len(tc.joinError) != 0 {
			require.IsType(t, &IncompatibleError{}, err)
			require.Regexp(t, tc.joinError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.join, join)
		}

		join, err = tc.b.Join(tc.a)
		if len(tc.joinError) != 0 {
			require.IsType(t, &IncompatibleError{}, err)
			require.Regexp(t, tc.joinError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.join, join)

			cmp, err = join.Compare(tc.a)
			require.NoError(t, err)
			require.GreaterOrEqual(t, cmp, 0)

			cmp, err = join.Compare(tc.b)
			require.NoError(t, err)
			require.GreaterOrEqual(t, cmp, 0)
		}
	}
}
