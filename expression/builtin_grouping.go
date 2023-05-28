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
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

func (c *groupingImplFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &BuiltinGroupingImplSig{bf, 0, map[uint64]struct{}{}, false}
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
	groupingMarks map[uint64]struct{}
	isMetaInited  bool
}

// SetMetadata will fill grouping function with comparison groupingMarks when rewriting grouping function.
func (b *BuiltinGroupingImplSig) SetMetadata(mode tipb.GroupingMode, groupingMarks map[uint64]struct{}) error {
	b.setGroupingMode(mode)
	b.setMetaGroupingMarks(groupingMarks)
	b.isMetaInited = true
	err := b.checkMetadata()
	if err != nil {
		b.isMetaInited = false
		return err
	}
	return nil
}

func (b *BuiltinGroupingImplSig) setGroupingMode(mode tipb.GroupingMode) {
	b.mode = mode
}

func (b *BuiltinGroupingImplSig) setMetaGroupingMarks(groupingMarks map[uint64]struct{}) {
	b.groupingMarks = groupingMarks
}

func (b *BuiltinGroupingImplSig) getGroupingMode() tipb.GroupingMode {
	return b.mode
}

// metadata returns the metadata of grouping functions
func (b *BuiltinGroupingImplSig) metadata() proto.Message {
	err := b.checkMetadata()
	if err != nil {
		return &tipb.GroupingFunctionMetadata{}
	}
	args := &tipb.GroupingFunctionMetadata{}
	*(args.Mode) = b.mode
	for groupingMark := range b.groupingMarks {
		args.GroupingMarks = append(args.GroupingMarks, groupingMark)
	}
	return args
}

// Clone implementing the builtinFunc interface.
func (b *BuiltinGroupingImplSig) Clone() builtinFunc {
	newSig := &BuiltinGroupingImplSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.mode = b.mode
	newSig.groupingMarks = b.groupingMarks
	return newSig
}

func (b *BuiltinGroupingImplSig) getMetaGroupingMarks() map[uint64]struct{} {
	return b.groupingMarks
}

func (b *BuiltinGroupingImplSig) getMetaGroupingID() uint64 {
	var metaGroupingID uint64
	groupingIDs := b.getMetaGroupingMarks()
	for key := range groupingIDs {
		metaGroupingID = key
	}
	return metaGroupingID
}

func (b *BuiltinGroupingImplSig) checkMetadata() error {
	if !b.isMetaInited {
		return errors.Errorf("Meta data hasn't been initialized")
	}
	mode := b.getGroupingMode()
	groupingIDs := b.getMetaGroupingMarks()
	if mode != tipb.GroupingMode_ModeBitAnd && mode != tipb.GroupingMode_ModeNumericCmp && mode != tipb.GroupingMode_ModeNumericSet {
		return errors.Errorf("Mode of meta data in grouping function is invalid. input mode: %d", mode)
	} else if (mode == tipb.GroupingMode_ModeBitAnd || mode == tipb.GroupingMode_ModeNumericCmp) && len(groupingIDs) != 1 {
		return errors.Errorf("Invalid number of groupingID. mode: %d, number of groupingID: %d", mode, len(b.groupingMarks))
	}
	return nil
}

func (b *BuiltinGroupingImplSig) groupingImplBitAnd(groupingID uint64, metaGroupingID uint64) int64 {
	if groupingID&metaGroupingID > 0 {
		return 1
	}
	return 0
}

func (b *BuiltinGroupingImplSig) groupingImplNumericCmp(groupingID uint64, metaGroupingID uint64) int64 {
	if groupingID > metaGroupingID {
		return 1
	}
	return 0
}

func (b *BuiltinGroupingImplSig) groupingImplNumericSet(groupingID uint64) int64 {
	groupingIDs := b.getMetaGroupingMarks()
	_, ok := groupingIDs[groupingID]
	if ok {
		return 0
	}
	return 1
}

func (b *BuiltinGroupingImplSig) grouping(groupingID uint64) int64 {
	switch b.mode {
	case tipb.GroupingMode_ModeBitAnd:
		return b.groupingImplBitAnd(groupingID, b.getMetaGroupingID())
	case tipb.GroupingMode_ModeNumericCmp:
		return b.groupingImplNumericCmp(groupingID, b.getMetaGroupingID())
	case tipb.GroupingMode_ModeNumericSet:
		return b.groupingImplNumericSet(groupingID)
	}
	return 0
}

// evalInt evals a builtinGroupingSig.
func (b *BuiltinGroupingImplSig) evalInt(row chunk.Row) (int64, bool, error) {
	if !b.isMetaInited {
		return 0, false, errors.Errorf("Meta data is not initialzied")
	}
	// grouping function should be rewritten from raw column ref to built gid column and groupingMarks meta.
	groupingID, isNull, err := b.args[0].EvalInt(b.ctx, row)
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
		metaGroupingID := b.getMetaGroupingID()
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplBitAnd(groupingIds.GetUint64(i), metaGroupingID)
		}
	case tipb.GroupingMode_ModeNumericCmp:
		metaGroupingID := b.getMetaGroupingID()
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericCmp(groupingIds.GetUint64(i), metaGroupingID)
		}
	case tipb.GroupingMode_ModeNumericSet:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericSet(groupingIds.GetUint64(i))
		}
	}
}

func (b *BuiltinGroupingImplSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if !b.isMetaInited {
		return errors.Errorf("Meta data is not initialzied")
	}
	rowNum := input.NumRows()

	bufVal, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err = b.args[0].VecEvalInt(b.ctx, input, bufVal); err != nil {
		return err
	}

	b.groupingVec(bufVal, rowNum, result)

	return nil
}
