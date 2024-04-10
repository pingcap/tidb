// Copyright 2023 PingCAP, Inc.
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

package expression

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &groupingImplFunctionClass{}
)

var (
	_ builtinFunc = &BuiltinGroupingImplSig{}
)

type groupingImplFunctionClass struct {
	baseFunctionClass
}

func (c *groupingImplFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	// grouping(x,y,z) is a singed UInt64 (while MySQL is Int64 which is unreasonable)
	bf.tp.SetFlag(bf.tp.GetFlag() | mysql.UnsignedFlag)
	// default filled meta is invalid for grouping evaluation, so the initialized flag is false.
	sig := &BuiltinGroupingImplSig{bf, 0, []map[uint64]struct{}{}, false}
	sig.setPbCode(tipb.ScalarFuncSig_GroupingSig)
	return sig, nil
}

// BuiltinGroupingImplSig grouping functions called by user is actually executed by this.
// Users will designate a column as parameter to pass into the grouping function, and tidb
// will rewrite it to convert the parameter to the meta info. Then, tidb will generate grouping id
// which is a indicator to be calculated with meta info, these grouping id are actually what
// BuiltinGroupingImplSig receives.
type BuiltinGroupingImplSig struct {
	baseBuiltinFunc

	// TODO these are two temporary fields for tests
	mode          tipb.GroupingMode
	groupingMarks []map[uint64]struct{}
	isMetaInited  bool
}

// SetMetadata will fill grouping function with comparison groupingMarks when rewriting grouping function.
func (b *BuiltinGroupingImplSig) SetMetadata(mode tipb.GroupingMode, groupingMarks []map[uint64]struct{}) error {
	b.setGroupingMode(mode)
	b.setMetaGroupingMarks(groupingMarks)
	b.isMetaInited = true
	err := b.checkMetadata()
	if err != nil {
		logutil.Logger(context.Background()).Error("grouping meta check err: " + err.Error())
		b.isMetaInited = false
		return err
	}
	return nil
}

func (b *BuiltinGroupingImplSig) setGroupingMode(mode tipb.GroupingMode) {
	b.mode = mode
}

func (b *BuiltinGroupingImplSig) setMetaGroupingMarks(groupingMarks []map[uint64]struct{}) {
	b.groupingMarks = groupingMarks
}

func (b *BuiltinGroupingImplSig) getGroupingMode() tipb.GroupingMode {
	return b.mode
}

// metadata returns the metadata of grouping functions
func (b *BuiltinGroupingImplSig) metadata() proto.Message {
	err := b.checkMetadata()
	if err != nil {
		logutil.Logger(context.Background()).Error("grouping meta check err: " + err.Error())
		return &tipb.GroupingFunctionMetadata{}
	}
	args := &tipb.GroupingFunctionMetadata{}
	args.Mode = &b.mode
	for _, groupingMark := range b.groupingMarks {
		gm := &tipb.GroupingMark{
			GroupingNums: make([]uint64, 0, len(groupingMark)),
		}
		for k := range groupingMark {
			gm.GroupingNums = append(gm.GroupingNums, k)
		}
		args.GroupingMarks = append(args.GroupingMarks, gm)
	}
	return args
}

// Clone implementing the builtinFunc interface.
func (b *BuiltinGroupingImplSig) Clone() builtinFunc {
	newSig := &BuiltinGroupingImplSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.mode = b.mode
	newSig.groupingMarks = b.groupingMarks
	// mpp task generation will clone whole plan tree, including every expression related.
	// if grouping function missed cloning this field, the ToPB check will errors.
	newSig.isMetaInited = b.isMetaInited
	return newSig
}

func (b *BuiltinGroupingImplSig) getMetaGroupingMarks() []map[uint64]struct{} {
	return b.groupingMarks
}

func (b *BuiltinGroupingImplSig) checkMetadata() error {
	if !b.isMetaInited {
		return errors.Errorf("Meta data hasn't been initialized")
	}
	mode := b.getGroupingMode()
	groupingMarks := b.getMetaGroupingMarks()
	if mode != tipb.GroupingMode_ModeBitAnd && mode != tipb.GroupingMode_ModeNumericCmp && mode != tipb.GroupingMode_ModeNumericSet {
		return errors.Errorf("Mode of meta data in grouping function is invalid. input mode: %d", mode)
	} else if mode == tipb.GroupingMode_ModeBitAnd || mode == tipb.GroupingMode_ModeNumericCmp {
		for _, groupingMark := range groupingMarks {
			if len(groupingMark) != 1 {
				return errors.Errorf("Invalid number of groupingID. mode: %d, number of groupingID: %d", mode, len(b.groupingMarks))
			}
		}
	}
	return nil
}

func (b *BuiltinGroupingImplSig) groupingImplBitAnd(groupingID uint64) int64 {
	groupingMarks := b.getMetaGroupingMarks()
	res := uint64(0)
	for _, groupingMark := range groupingMarks {
		// for Bit-And mode, there is only one element in groupingMark.
		for k := range groupingMark {
			res <<= 1
			if groupingID&k <= 0 {
				// col is not needed, being filled with null and grouped. = 1
				res += 1
			}
			// col is needed in this grouping set, meaning not being grouped. = 0
		}
	}
	return int64(res)
}

func (b *BuiltinGroupingImplSig) groupingImplNumericCmp(groupingID uint64) int64 {
	groupingMarks := b.getMetaGroupingMarks()
	res := uint64(0)
	for _, groupingMark := range groupingMarks {
		// for Num-Cmp mode, there is only one element in groupingMark.
		for k := range groupingMark {
			res <<= 1
			if groupingID <= k {
				// col is not needed, being filled with null and grouped. = 1
				res += 1
			}
			// col is needed, meaning not being grouped. = 0
		}
	}
	return int64(res)
}

func (b *BuiltinGroupingImplSig) groupingImplNumericSet(groupingID uint64) int64 {
	groupingMarks := b.getMetaGroupingMarks()
	res := uint64(0)
	for _, groupingMark := range groupingMarks {
		res <<= 1
		// for Num-Set mode, traverse the slice to find the match.
		_, ok := groupingMark[groupingID]
		if !ok {
			// in Num-Set mode, this map maintains the needed-col's grouping set (GIDs)
			// when ok is NOT true, col is not needed, being filled with null and grouped. = 1
			res += 1
		}
		// it means col is needed, meaning not being filled with null and grouped. = 0
	}
	return int64(res)
}

// since grouping function may have multi args like grouping(a,b), so the source columns may greater than 1.
// reference: https://dev.mysql.com/blog-archive/mysql-8-0-grouping-function/
// Let's say GROUPING(b,a) group by a,b with rollup. (Note the b,a sequence is reversed from gby item)
// if GROUPING (b,a) returns 3 (11 in bits), it means that NULL in column “b” and NULL in column “a” for that
// row is produced by a ROLLUP operation. If result is 2 (10 in bits), meaning NULL in column “a” alone is the
// result of ROLLUP operation.
//
// Formula: GROUPING(x,y,z) = GROUPING(x) << 2 + GROUPING(y) << 1 + GROUPING(z)
//
// so for the multi args GROUPING FUNCTION, we should return all the simple col grouping marks. When evaluating,
// after all grouping marks & with gid in sequence, the final res is derived as the formula said. This also means
// that the grouping function accepts a maximum of 64 parameters, obviously the result is an uint64.
func (b *BuiltinGroupingImplSig) grouping(groupingID uint64) int64 {
	switch b.mode {
	case tipb.GroupingMode_ModeBitAnd:
		return b.groupingImplBitAnd(groupingID)
	case tipb.GroupingMode_ModeNumericCmp:
		return b.groupingImplNumericCmp(groupingID)
	case tipb.GroupingMode_ModeNumericSet:
		return b.groupingImplNumericSet(groupingID)
	}
	return 0
}

// evalInt evals a builtinGroupingSig.
func (b *BuiltinGroupingImplSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if !b.isMetaInited {
		return 0, false, errors.Errorf("Meta data is not initialized")
	}
	// grouping function should be rewritten from raw column ref to built gid column and groupingMarks meta.
	groupingID, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return b.grouping(uint64(groupingID)), false, nil
}

func (b *BuiltinGroupingImplSig) groupingVec(groupingIds *chunk.Column, rowNum int, result *chunk.Column) {
	result.ResizeInt64(rowNum, false)
	resContainer := result.Int64s()
	switch b.mode {
	case tipb.GroupingMode_ModeBitAnd:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplBitAnd(groupingIds.GetUint64(i))
		}
	case tipb.GroupingMode_ModeNumericCmp:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericCmp(groupingIds.GetUint64(i))
		}
	case tipb.GroupingMode_ModeNumericSet:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericSet(groupingIds.GetUint64(i))
		}
	}
}

func (b *BuiltinGroupingImplSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if !b.isMetaInited {
		return errors.Errorf("Meta data is not initialized")
	}
	rowNum := input.NumRows()

	bufVal, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err = b.args[0].VecEvalInt(ctx, input, bufVal); err != nil {
		return err
	}

	b.groupingVec(bufVal, rowNum, result)

	return nil
}
