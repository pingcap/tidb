// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *builtinArithmeticPlusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := s.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer s.bufAllocator.put(buf)

	if err := s.args[0].VecEvalInt(s.ctx, input, result); err != nil {
		return err
	}
	if err := s.args[1].VecEvalInt(s.ctx, input, buf); err != nil {
		return err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)
	as := result.Int64s()
	bs := buf.Int64s()
	switch {
	case isLHSUnsigned && isRHSUnsigned:
		for i := 0; i < n; i ++ {
			if result.IsNull(i) {
				continue
			}
			if buf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}
			if uint64(as[i]) > math.MaxUint64-uint64(bs[i]) {
				err := types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
				if err := s.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			as[i] += bs[i]
		}
	case isLHSUnsigned && !isRHSUnsigned:
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if buf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}
			if (bs[i] < 0 && uint64(-bs[i]) > uint64(as[i])) || (bs[i] > 0 && uint64(as[i]) > math.MaxUint64-uint64(bs[i])) {
				err := types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
				if err := s.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			as[i] += bs[i]
		}
	case !isLHSUnsigned && isRHSUnsigned:
		for i := 0; i < n; i ++ {
			if result.IsNull(i) {
				continue
			}
			if buf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}
			if (as[i] < 0 && uint64(-as[i]) > uint64(bs[i])) || (as[i] > 0 && uint64(bs[i]) > math.MaxUint64-uint64(as[i])) {
				err := types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
				if err := s.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			as[i] += bs[i]
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		for i := 0; i < n; i ++ {
			if result.IsNull(i) {
				continue
			}
			if buf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}
			if (as[i] > 0 && bs[i] > math.MaxInt64-as[i]) || (as[i] < 0 && bs[i] < math.MaxInt64-as[i]) {
				err := types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
				if err := s.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			as[i] += bs[i]
		}
	}
	return nil
}

func (s *builtinArithmeticPlusIntSig) vectorized() bool {
	return true
}
