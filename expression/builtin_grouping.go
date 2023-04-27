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
	_ builtinFunc = &builtinGroupingImplSig{}
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
	sig := &builtinGroupingImplSig{bf, 0, map[int64]struct{}{}, false}
	sig.setPbCode(tipb.ScalarFuncSig_GroupingSig)
	return sig, nil
}

// grouping functions called by user is actually executed by this builtinGroupingImplSig.
// Users will designate a column as parameter to pass into the grouping function, and tidb
// will rewrite it to convert the parameter to the meta info. Then, tidb will generate grouping id
// which is a indicator to be calculated with meta info, these grouping id are actually what
// builtinGroupingImplSig receives.
type builtinGroupingImplSig struct {
	baseBuiltinFunc

	// TODO these are two temporary fields for tests
	mode          tipb.GroupingMode
	groupingMarks map[int64]struct{}
	isMetaInited  bool
}

func (b *builtinGroupingImplSig) SetMetadata(mode tipb.GroupingMode, groupingMarks map[int64]struct{}) error {
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

func (b *builtinGroupingImplSig) setGroupingMode(mode tipb.GroupingMode) {
	b.mode = mode
}

func (b *builtinGroupingImplSig) setMetaGroupingMarks(groupingMarks map[int64]struct{}) {
	b.groupingMarks = groupingMarks
}

func (b *builtinGroupingImplSig) getGroupingMode() tipb.GroupingMode {
	return b.mode
}

// metadata returns the metadata of grouping functions
func (b *builtinGroupingImplSig) metadata() proto.Message {
	err := b.checkMetadata()
	if err != nil {
		return &tipb.GroupingFunctionMetadata{}
	}
	args := &tipb.GroupingFunctionMetadata{}
	*(args.Mode) = b.mode
	for groupingMark := range b.groupingMarks {
		args.GroupingMarks = append(args.GroupingMarks, uint64(groupingMark))
	}
	return args
}

func (b *builtinGroupingImplSig) Clone() builtinFunc {
	newSig := &builtinGroupingImplSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.mode = b.mode
	newSig.groupingMarks = b.groupingMarks
	return newSig
}

func (b *builtinGroupingImplSig) getMetaGroupingMarks() map[int64]struct{} {
	return b.groupingMarks
}

func (b *builtinGroupingImplSig) getMetaGroupingID() int64 {
	var metaGroupingID int64
	grouping_ids := b.getMetaGroupingMarks()
	for key := range grouping_ids {
		metaGroupingID = key
	}
	return metaGroupingID
}

func (b *builtinGroupingImplSig) checkMetadata() error {
	if !b.isMetaInited {
		return errors.Errorf("Meta data hasn't been initialized")
	}
	mode := b.getGroupingMode()
	grouping_ids := b.getMetaGroupingMarks()
	if mode != tipb.GroupingMode_ModeBitAnd && mode != tipb.GroupingMode_ModeNumericCmp && mode != tipb.GroupingMode_ModeNumericSet {
		return errors.Errorf("Mode of meta data in grouping function is invalid. input mode: %d", mode)
	} else if (mode == tipb.GroupingMode_ModeBitAnd || mode == tipb.GroupingMode_ModeNumericCmp) && len(grouping_ids) != 1 {
		return errors.Errorf("Invalid number of groupingID. mode: %d, number of groupingID: %d", mode, len(b.groupingMarks))
	}
	return nil
}

func (b *builtinGroupingImplSig) groupingImplBitAnd(groupingID int64, metaGroupingID int64) int64 {
	if groupingID&metaGroupingID > 0 {
		return 1
	}
	return 0
}

func (b *builtinGroupingImplSig) groupingImplNumericCmp(groupingID int64, metaGroupingID int64) int64 {
	if groupingID > metaGroupingID {
		return 1
	}
	return 0
}

func (b *builtinGroupingImplSig) groupingImplNumericSet(groupingID int64) int64 {
	grouping_ids := b.getMetaGroupingMarks()
	_, ok := grouping_ids[groupingID]
	if ok {
		return 0
	}
	return 1
}

func (b *builtinGroupingImplSig) grouping(groupingID int64) int64 {
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
func (b *builtinGroupingImplSig) evalInt(row chunk.Row) (int64, bool, error) {
	if !b.isMetaInited {
		return 0, false, errors.Errorf("Meta data is not initialzied")
	}

	groupingID, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return b.grouping(groupingID), false, nil
}

func (b *builtinGroupingImplSig) groupingVec(groupingIds *chunk.Column, rowNum int, result *chunk.Column) {
	result.ResizeInt64(rowNum, false)
	resContainer := result.Int64s()
	switch b.mode {
	case tipb.GroupingMode_ModeBitAnd:
		metaGroupingID := b.getMetaGroupingID()
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplBitAnd(groupingIds.GetInt64(i), metaGroupingID)
		}
	case tipb.GroupingMode_ModeNumericCmp:
		metaGroupingID := b.getMetaGroupingID()
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericCmp(groupingIds.GetInt64(i), metaGroupingID)
		}
	case tipb.GroupingMode_ModeNumericSet:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericSet(groupingIds.GetInt64(i))
		}
	}
}

func (b *builtinGroupingImplSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
