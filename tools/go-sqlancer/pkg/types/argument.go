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

package types

import (
	"fmt"
	"math"
	"math/rand"
)

// TODO: can it copy from tidb/types?
const (
	// Null included by any types
	// TypeNullArg               uint64 = 1 << iota
	TypeNonFormattedStringArg uint64 = 1 << iota
	TypeDatetimeLikeStringArg uint64 = 1 << iota // datetime formatted string: "1990-10-11"
	TypeNumberLikeStringArg   uint64 = 1 << iota // "1.001" " 42f_xa" start with number
	TypeIntArg                uint64 = 1 << iota
	TypeFloatArg              uint64 = 1 << iota
	TypeDatetimeArg           uint64 = 1 << iota
	TypeDatetimeLikeArg              = TypeDatetimeArg | TypeDatetimeLikeStringArg
	TypeStringArg                    = TypeNonFormattedStringArg | TypeDatetimeLikeStringArg | TypeNumberLikeStringArg
	TypeNumberArg                    = TypeIntArg | TypeFloatArg
	TypeNumberLikeArg                = TypeNumberArg | TypeNumberLikeStringArg
	// Array, Enum, Blob to be completed

	AnyArg uint64 = math.MaxUint64
)

var (
	supportArgs = []uint64{
		TypeFloatArg,
		TypeIntArg,
		TypeNonFormattedStringArg,
		TypeDatetimeLikeStringArg,
		TypeNumberLikeStringArg,
	}
)

type argTable interface {
	// reserve for variable arg length op/fn
	RandByFilter([]*uint64, *uint64) ([]uint64, error)
	// for table is [1, 3, 4] => 3, [1, 2, 5] => 3, [1, 2, 2] => 5
	// input [1, 2, nil], 3 => select where [1, 2, *] => 3
	// in previous example, is [1, 2, 5] => 3
	// returns [[1, 2, 5, 3]]
	Filter([]*uint64, *uint64) ([][]uint64, error)
	Insert(uint64, ...uint64)
}
type opFuncArg0DTable uint64 // only the return type
type opFuncArgNDTable struct {
	table [][]uint64
	n     int
}

// OpFuncArgInfTable TODO: implement me: for infinite args
type OpFuncArgInfTable struct{}

// Filter is Filter
func (t *opFuncArg0DTable) Filter([]*uint64, *uint64) ([][]uint64, error) {
	return [][]uint64{{(uint64)(*t)}}, nil
}

// RandByFilter is RandByFilter
func (t *opFuncArg0DTable) RandByFilter([]*uint64, *uint64) ([]uint64, error) {
	return []uint64{(uint64)(*t)}, nil
}

// Insert is to Insert
func (t *opFuncArg0DTable) Insert(ret uint64, _ ...uint64) {
	*t = (opFuncArg0DTable)(ret)
}

// Filter is to filter
func (t *opFuncArgNDTable) Filter(args []*uint64, ret *uint64) ([][]uint64, error) {
	realArgs := make([]*uint64, 0, len(args))
	copy(realArgs, args)
	if len(realArgs) < t.n {
		for i := len(realArgs); i < t.n; i++ {
			realArgs = append(realArgs, nil)
		}
	}
	if len(realArgs) > t.n {
		realArgs = realArgs[:t.n]
	}
	result := make([][]uint64, 0)
	for _, i := range t.table {
		for idx, j := range realArgs {
			if j == nil {
				continue
			}
			if *j != i[idx] {
				goto nextLoop
			}
		}
		if ret == nil || *ret == i[len(i)-1] {
			result = append(result, i[:])
		}
	nextLoop:
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty set after filter, args: %+v, ret: %v", args, ret)
	}
	return result, nil
}

func (t *opFuncArgNDTable) RandByFilter(args []*uint64, ret *uint64) ([]uint64, error) {
	total, err := t.Filter(args, ret)
	if err != nil {
		return nil, err
	}
	return total[rand.Intn(len(total))], nil
}

func (t *opFuncArgNDTable) Insert(ret uint64, args ...uint64) {
	// here do NOT check duplicate records
	if len(args) != t.n {
		panic(fmt.Sprintf("arguments number does not match n: %d, args: %d", t.n, len(args)))
	}
	args = append(args, ret)
	t.table = append(t.table, args)
}

// NewArgTable create argTable
func NewArgTable(dimension int) argTable {
	switch dimension {
	case 0:
		return new(opFuncArg0DTable)
	case 1, 2, 3, 4, 5:
		return &opFuncArgNDTable{n: dimension, table: make([][]uint64, 0)}
	default:
		panic(fmt.Sprintf("more args (%d) are not supported present", dimension))
	}
}
