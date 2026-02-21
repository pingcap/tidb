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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// UUIDStrLen is the length of a UUID in string format
// 16 bytes in hex is 32 characters, plus 4 hyphens = 36 characters (hex/ASCII, 1 byte chars)
// https://datatracker.ietf.org/doc/html/rfc9562#name-uuid-format
const UUIDStrLen = 36

func (b *builtinInetNtoaSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	ip := make(net.IP, net.IPv4len)
	for i := range n {
		val := i64s[i]
		if buf.IsNull(i) || val < 0 || uint64(val) > math.MaxUint32 {
			result.AppendNull()
			continue
		}
		binary.BigEndian.PutUint32(ip, uint32(val))
		ipv4 := ip.To4()
		if ipv4 == nil {
			// Not a valid ipv4 address.
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

func (b *builtinIsIPv4Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
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

func (b *builtinJSONAnyValueSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalJSON(ctx, input, result)
}

func (b *builtinRealAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinRealAnyValueSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalReal(ctx, input, result)
}

func (b *builtinStringAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinStringAnyValueSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinIsIPv6Sig) vectorized() bool {
	return true
}

func (b *builtinIsIPv6Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		ipStr := buf.GetString(i)
		if ip := net.ParseIP(ipStr); ip != nil && !isIPv4(ipStr) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinIsUUIDSig) vectorized() bool {
	return true
}

func (b *builtinIsUUIDSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetString(i)
		// MySQL's IS_UUID is strict and doesn't trim spaces, unlike Go's uuid.Parse
		// We need to check if the string has leading/trailing spaces before parsing
		if strings.TrimSpace(val) != val {
			i64s[i] = 0
		} else if _, err = uuid.Parse(val); err != nil {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstStringSig) vectorized() bool {
	return true
}

func (b *builtinNameConstStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalString(ctx, input, result)
}

func (b *builtinDecimalAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinDecimalAnyValueSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalDecimal(ctx, input, result)
}

func (b *builtinUUIDSig) vectorized() bool {
	return true
}

func (b *builtinUUIDSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveStringWithSizeHint(n, UUIDStrLen)
	var id uuid.UUID
	var err error
	for range n {
		id, err = uuid.NewUUID()
		if err != nil {
			return err
		}
		result.AppendString(id.String())
	}
	return nil
}

func (b *builtinUUIDv4Sig) vectorized() bool {
	return true
}

func (b *builtinUUIDv4Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveStringWithSizeHint(n, UUIDStrLen)
	var id uuid.UUID
	var err error
	for range n {
		id, err = uuid.NewRandom()
		if err != nil {
			return err
		}
		result.AppendString(id.String())
	}
	return nil
}

func (b *builtinUUIDv7Sig) vectorized() bool {
	return true
}

func (b *builtinUUIDv7Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveStringWithSizeHint(n, UUIDStrLen)
	var id uuid.UUID
	var err error
	for range n {
		id, err = uuid.NewV7()
		if err != nil {
			return err
		}
		result.AppendString(id.String())
	}
	return nil
}

func (b *builtinUUIDVersionSig) vectorized() bool {
	return true
}

func (b *builtinUUIDVersionSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetString(i)
		u, err := uuid.Parse(val)
		if err != nil {
			return errWrongValueForType.GenWithStackByArgs("string", val, "uuid_version")
		}
		i64s[i] = int64(u.Version())
	}
	return nil
}

func (b *builtinUUIDTimestampSig) vectorized() bool {
	return true
}

func (b *builtinUUIDTimestampSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	d := result.Decimals()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetString(i)
		u, err := uuid.Parse(val)
		if err != nil {
			return errWrongValueForType.GenWithStackByArgs("string", val, "uuid_timestamp")
		}
		switch u.Version() {
		case 1, 6, 7:
		default:
			result.SetNull(i, true)
			continue
		}

		s, ns := u.Time().UnixTime()
		d[i].FromInt((s * 1000000) + (ns / 1000))
		err = d[i].Shift(-6)
		if err != nil {
			return err
		}
		err = d[i].Round(&d[i], 6, types.ModeHalfUp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinNameConstDurationSig) vectorized() bool {
	return true
}

func (b *builtinNameConstDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalDuration(ctx, input, result)
}

func (b *builtinDurationAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinDurationAnyValueSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalDuration(ctx, input, result)
}

func (b *builtinIntAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinIntAnyValueSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinIsIPv4CompatSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4CompatSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	prefixCompat := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		// Note that the input should be IP address in byte format.
		// For IPv4, it should be byte slice with 4 bytes.
		// For IPv6, it should be byte slice with 16 bytes.
		// See example https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
		ipAddress := buf.GetBytes(i)
		if len(ipAddress) != net.IPv6len || !bytes.HasPrefix(ipAddress, prefixCompat) {
			// Not an IPv6 address, return false
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstIntSig) vectorized() bool {
	return true
}

func (b *builtinNameConstIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalInt(ctx, input, result)
}

func (b *builtinNameConstTimeSig) vectorized() bool {
	return true
}

func (b *builtinNameConstTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalTime(ctx, input, result)
}

func (b *builtinSleepSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinSleepSig in a vectorized manner.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	err = b.args[0].VecEvalReal(ctx, input, buf)
	if err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	ec := errCtx(ctx)
	for i := range n {
		isNull := buf.IsNull(i)
		val := buf.GetFloat64(i)

		if isNull || val < 0 {
			// for insert ignore stmt, the StrictSQLMode and ignoreErr should both be considered.
			err := ec.HandleErrorWithAlias(errBadNull,
				errIncorrectArgs.GenWithStackByArgs("sleep"),
				errIncorrectArgs.FastGenByArgs("sleep"),
			)
			if err != nil {
				return err
			}
			continue
		}

		if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
			return errIncorrectArgs.GenWithStackByArgs("sleep")
		}

		if isKilled := doSleep(val, vars); isKilled {
			for j := i; j < n; j++ {
				i64s[j] = 1
			}
			return nil
		}
	}

	return nil
}

func doSleep(secs float64, sessVars *variable.SessionVars) (isKilled bool) {
	if secs <= 0.0 {
		return false
	}
	dur := time.Duration(secs * float64(time.Second.Nanoseconds()))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(dur)
	for {
		select {
		case <-ticker.C:
			// MySQL 8.0 sleep: https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_sleep
			// Regular kill or Killed because of max execution time
			if err := sessVars.SQLKiller.HandleSignal(); err != nil {
				if len(sessVars.StmtCtx.TableIDs) == 0 {
					sessVars.SQLKiller.Reset()
				}
				timer.Stop()
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

func (b *builtinIsIPv4MappedSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4MappedSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	prefixMapped := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		// Note that the input should be IP address in byte format.
		// For IPv4, it should be byte slice with 4 bytes.
		// For IPv6, it should be byte slice with 16 bytes.
		// See example https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
		ipAddress := buf.GetBytes(i)
		if len(ipAddress) != net.IPv6len || !bytes.HasPrefix(ipAddress, prefixMapped) {
			// Not an IPv6 address, return false
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstDecimalSig) vectorized() bool {
	return true
}

func (b *builtinNameConstDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalDecimal(ctx, input, result)
}

func (b *builtinNameConstJSONSig) vectorized() bool {
	return true
}

func (b *builtinNameConstJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalJSON(ctx, input, result)
}

