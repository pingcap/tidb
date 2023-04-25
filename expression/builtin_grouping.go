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
	_ builtinFunc = &builtinGroupingSig{}
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
	sig := &builtinGroupingSig{bf, 0, map[int64]struct{}{}}
	sig.setPbCode(tipb.ScalarFuncSig_GroupingSig)
	return sig, nil
}

type builtinGroupingSig struct {
	baseBuiltinFunc

	// TODO these are two temporary fields for tests
	mode        tipb.GroupingMode
	groupingIDs map[int64]struct{}
}

func (b *builtinGroupingSig) SetMetaVersion(mode tipb.GroupingMode) {
	b.mode = mode
}

func (b *builtinGroupingSig) SetMetaGroupingIDs(groupingIDs map[int64]struct{}) {
	b.groupingIDs = groupingIDs
}

func (b *builtinGroupingSig) getMetaVersion() tipb.GroupingMode {
	return b.mode
}

// metadata returns the metadata of grouping functions
func (b *builtinGroupingSig) metadata() proto.Message {
	args := &tipb.GroupingFunctionMetadata{
		// TODO
	}
	return args
}

func (b *builtinGroupingSig) Clone() builtinFunc {
	newSig := &builtinGroupingSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.mode = b.mode
	newSig.groupingIDs = b.groupingIDs
	return newSig
}

func (b *builtinGroupingSig) getMetaGroupingIDs() map[int64]struct{} {
	return b.groupingIDs
}

func (b *builtinGroupingSig) getMetaGroupingID() int64 {
	var metaGroupingID int64
	grouping_ids := b.getMetaGroupingIDs()
	for key := range grouping_ids {
		metaGroupingID = key
	}
	return metaGroupingID
}

func (b *builtinGroupingSig) checkMetadata() error {
	version := b.getMetaVersion()
	grouping_ids := b.getMetaGroupingIDs()
	if version < 1 || version > 3 {
		return errors.Errorf("Version of meta data in grouping function is invalid. input version: %d", version)
	} else if (version == 1 || version == 2) && len(grouping_ids) != 1 {
		return errors.Errorf("Invalid number of groupingID. version: %d, number of groupingID: %d", version, len(b.groupingIDs))
	}
	return nil
}

func (b *builtinGroupingSig) groupingImplBitAnd(groupingID int64, metaGroupingID int64) int64 {
	if groupingID&metaGroupingID > 0 {
		return 1
	}
	return 0
}

func (b *builtinGroupingSig) groupingImplNumericCmp(groupingID int64, metaGroupingID int64) int64 {
	if groupingID > metaGroupingID {
		return 1
	}
	return 0
}

func (b *builtinGroupingSig) groupingImplNumericSet(groupingID int64) int64 {
	grouping_ids := b.getMetaGroupingIDs()
	_, ok := grouping_ids[groupingID]
	if ok {
		return 0
	}
	return 1
}

func (b *builtinGroupingSig) grouping(groupingID int64) int64 {
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
func (b *builtinGroupingSig) evalInt(row chunk.Row) (int64, bool, error) {
	err := b.checkMetadata()
	if err != nil {
		return 0, false, err
	}

	groupingID, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return b.grouping(groupingID), false, nil
}

func (b *builtinGroupingSig) groupingVec(groupingIds *chunk.Column, rowNum int, result *chunk.Column) {
	result.ResizeInt64(rowNum, false)
	resContainer := result.Int64s()
	switch b.mode {
	case tipb.GroupingMode_ModeBitAnd:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplBitAnd(groupingIds.GetInt64(i), b.getMetaGroupingID())
		}
	case tipb.GroupingMode_ModeNumericCmp:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericCmp(groupingIds.GetInt64(i), b.getMetaGroupingID())
		}
	case tipb.GroupingMode_ModeNumericSet:
		for i := 0; i < rowNum; i++ {
			resContainer[i] = b.groupingImplNumericSet(groupingIds.GetInt64(i))
		}
	}
}

func (b *builtinGroupingSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
