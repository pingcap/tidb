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
	"encoding/binary"
	"math"
	"net"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinInetNtoaSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	ip := make(net.IP, net.IPv4len)
	for i := 0; i < n; i++ {
		val := i64s[i]
		if buf.IsNull(i) || val < 0 || uint64(val) > math.MaxUint32 {
			result.AppendNull()
			continue
		}
		binary.BigEndian.PutUint32(ip, uint32(val))
		ipv4 := ip.To4()
		if ipv4 == nil {
			//Not a vaild ipv4 address.
			result.AppendNull()
			continue
		}
		result.AppendString(ipv4.String())
	}
	return nil
}

func (b *builtinInetNtoaSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4Sig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		// Note that even when the i-th input string is null, the output is
		// 0 instead of null, therefore we do not set the null bit mask in
		// result's corresponding row.
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4
		if isIPv4(buf.GetString(i)) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinIsIPv4Sig) vectorized() bool {
	return true
}
func (b *builtinJSONAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinJSONAnyValueSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalJSON(b.ctx, input, result)
}

func (b *builtinRealAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinRealAnyValueSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalReal(b.ctx, input, result)
}

func (b *builtinStringAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinStringAnyValueSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalString(b.ctx, input, result)
}

func (b *builtinIsIPv6Sig) vectorized() bool {
	return false
}

func (b *builtinIsIPv6Sig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstStringSig) vectorized() bool {
	return true
}

func (b *builtinNameConstStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalString(b.ctx, input, result)
}

func (b *builtinDecimalAnyValueSig) vectorized() bool {
	return false
}

func (b *builtinDecimalAnyValueSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUUIDSig) vectorized() bool {
	return true
}

func (b *builtinUUIDSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	var id uuid.UUID
	var err error
	for i := 0; i < n; i++ {
		id, err = uuid.NewUUID()
		if err != nil {
			return err
		}
		result.AppendString(id.String())
	}
	return nil
}

func (b *builtinNameConstDurationSig) vectorized() bool {
	return true
}

func (b *builtinNameConstDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalDuration(b.ctx, input, result)
}

func (b *builtinLockSig) vectorized() bool {
	return false
}

func (b *builtinLockSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDurationAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinDurationAnyValueSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalDuration(b.ctx, input, result)
}

func (b *builtinIntAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinIntAnyValueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(b.ctx, input, result)
}

func (b *builtinIsIPv4CompatSig) vectorized() bool {
	return false
}

func (b *builtinIsIPv4CompatSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstIntSig) vectorized() bool {
	return false
}

func (b *builtinNameConstIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstTimeSig) vectorized() bool {
	return true
}

func (b *builtinNameConstTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalTime(b.ctx, input, result)
}

func (b *builtinSleepSig) vectorized() bool {
	return false
}

func (b *builtinSleepSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinIsIPv4MappedSig) vectorized() bool {
	return false
}

func (b *builtinIsIPv4MappedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstDecimalSig) vectorized() bool {
	return false
}

func (b *builtinNameConstDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstJSONSig) vectorized() bool {
	return false
}

func (b *builtinNameConstJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInet6AtonSig) vectorized() bool {
	return false
}

func (b *builtinInet6AtonSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTimeAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinTimeAnyValueSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalTime(b.ctx, input, result)
}

func (b *builtinInetAtonSig) vectorized() bool {
	return false
}

func (b *builtinInetAtonSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInet6NtoaSig) vectorized() bool {
	return false
}

func (b *builtinInet6NtoaSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNameConstRealSig) vectorized() bool {
	return true
}

func (b *builtinNameConstRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalReal(b.ctx, input, result)
}

func (b *builtinReleaseLockSig) vectorized() bool {
	return false
}

func (b *builtinReleaseLockSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
