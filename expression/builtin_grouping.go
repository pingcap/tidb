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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &groupingFunctionClass{}
)

var (
	_ builtinFunc = &builtinGroupingSig{}
)

type groupingFunctionClass struct {
	baseFunctionClass
}

// TODO
func (c *groupingFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	// 	if err := c.verifyArgs(args); err != nil {
	// 		return nil, err
	// 	}
	// 	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	// 	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	bf.tp.SetFlen(1)
	// 	sig := &builtinIlikeSig{bf, nil, false, sync.Once{}}
	// 	sig.setPbCode(tipb.ScalarFuncSig_IlikeSig)
	// 	return sig, nil
	return nil, nil
}

type builtinGroupingSig struct {
	baseBuiltinFunc

	version      uint32
	grouping_ids map[uint64]uint64
}

// metadata returns the metadata of grouping functions
func (b *builtinGroupingSig) metadata() proto.Message {
	args := &tipb.GroupingFunctionMetadata{
		// TODO
	}
	return args
}

// TODO
func (b *builtinGroupingSig) Clone() builtinFunc {
	newSig := &builtinGroupingSig{}
	// newSig.cloneFrom(&b.baseBuiltinFunc)
	// newSig.pattern = b.pattern
	// newSig.isMemorizedPattern = b.isMemorizedPattern
	return newSig
}

func (b *builtinGroupingSig) getMetaGroupingID() uint64 {
	var meta_grouping_id uint64
	for key, _ := range b.grouping_ids {
		meta_grouping_id = key
	}
	return meta_grouping_id
}

func (b *builtinGroupingSig) checkMetadata() error {
	if b.version < 1 || b.version > 3 {
		return errors.Errorf("Version of meta data in grouping function is invalid. input version: %d", b.version)
	} else if (b.version == 1 || b.version == 2) && len(b.grouping_ids) != 0 {
		return errors.Errorf("Invalid number of grouping_id. version: %d, number of grouping_id: %d", b.version, len(b.grouping_ids))
	}
	return nil
}

func (b *builtinGroupingSig) groupingImplV1(grouping_id uint64, meta_grouping_id uint64) int64 {
	// TODO
	return 0
}

func (b *builtinGroupingSig) groupingImplV2(grouping_id uint64, meta_grouping_id uint64) int64 {
	// TODO
	return 0
}

func (b *builtinGroupingSig) groupingImplV3(grouping_id uint64) int64 {
	// TODO
	return 0
}

func (b *builtinGroupingSig) grouping(grouping_id uint64) int64 {
	switch b.version {
	case 1:
		return b.groupingImplV1(grouping_id, b.getMetaGroupingID())
	case 2:
		return b.groupingImplV2(grouping_id, b.getMetaGroupingID())
	case 3:
		return b.groupingImplV3(grouping_id)
	}
	return 0
}

// evalInt evals a builtinGroupingSig.
func (b *builtinGroupingSig) evalInt(row chunk.Row) (int64, bool, error) {
	err := b.checkMetadata()
	if err != nil {
		return 0, false, err
	}

	grouping_id, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return b.grouping(uint64(grouping_id)), false, nil
}

func (b *builtinGroupingSig) groupingVec(param *funcParam) {
	// TODO
}

func (b *builtinGroupingSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	rowNum := input.NumRows()
	params := make([]*funcParam, 0, 1)
	defer releaseBuffers(&b.baseBuiltinFunc, params)

	param, isConstNull, err := buildIntParam(&b.baseBuiltinFunc, 0, input, false, 0)
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullBytesIntoResult(result, rowNum)
		return nil
	}

	b.groupingVec(param)

	return nil // TODO
}
