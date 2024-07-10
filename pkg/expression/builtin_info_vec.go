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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/printer"
)

func (b *builtinDatabaseSig) vectorized() bool {
	return true
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	currentDB := ctx.CurrentDB()
	result.ReserveString(n)
	if currentDB == "" {
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
	} else {
		for i := 0; i < n; i++ {
			result.AppendString(currentDB)
		}
	}
	return nil
}

func (b *builtinConnectionIDSig) vectorized() bool {
	return true
}

func (b *builtinConnectionIDSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	if data == nil {
		return errors.Errorf("Missing session variable in `builtinConnectionIDSig.vecEvalInt`")
	}
	connectionID := int64(data.ConnectionID)
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = connectionID
	}
	return nil
}

func (b *builtinTiDBVersionSig) vectorized() bool {
	return true
}

func (b *builtinTiDBVersionSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	info := printer.GetTiDBInfo()
	for i := 0; i < n; i++ {
		result.AppendString(info)
	}
	return nil
}

func (b *builtinRowCountSig) vectorized() bool {
	return true
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinRowCountSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := vars.StmtCtx.PrevAffectedRows
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinCurrentUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return err
	}

	result.ReserveString(n)
	if user == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}
	for i := 0; i < n; i++ {
		result.AppendString(user.String())
	}
	return nil
}

func (b *builtinCurrentResourceGroupSig) vectorized() bool {
	return true
}

func (b *builtinCurrentResourceGroupSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	if data == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}
	n := input.NumRows()
	result.ReserveString(n)
	groupName := getHintResourceGroupName(data)
	for i := 0; i < n; i++ {
		result.AppendString(groupName)
	}
	return nil
}

func (b *builtinCurrentRoleSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_current-role
func (b *builtinCurrentRoleSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	roles, err := b.ActiveRoles(ctx)
	if err != nil {
		return err
	}

	if roles == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}

	result.ReserveString(n)
	if len(roles) == 0 {
		for i := 0; i < n; i++ {
			result.AppendString("NONE")
		}
		return nil
	}

	sortedRes := make([]string, 0, 10)
	for _, r := range roles {
		sortedRes = append(sortedRes, r.String())
	}
	slices.Sort(sortedRes)
	res := strings.Join(sortedRes, ",")
	for i := 0; i < n; i++ {
		result.AppendString(res)
	}
	return nil
}

func (b *builtinUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return err
	}
	if user == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(user.LoginString())
	}
	return nil
}

func (b *builtinTiDBIsDDLOwnerSig) vectorized() bool {
	return true
}

func (b *builtinTiDBIsDDLOwnerSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var res int64

	isOwner, err := b.IsDDLOwner(ctx)
	if err != nil {
		return err
	}

	if isOwner {
		res = 1
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinFoundRowsSig) vectorized() bool {
	return true
}

func (b *builtinFoundRowsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	if data == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}
	lastFoundRows := int64(data.LastFoundRows)
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := range i64s {
		i64s[i] = lastFoundRows
	}
	return nil
}

func (b *builtinBenchmarkSig) vectorized() bool {
	return b.constLoopCount > 0
}

func (b *builtinBenchmarkSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	loopCount := b.constLoopCount
	arg := b.args[1]
	evalType := arg.GetType(ctx).EvalType()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	var k int64
	switch evalType {
	case types.ETInt:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalInt(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETReal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalReal(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDecimal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDecimal(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETString:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalString(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalTime(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDuration:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDuration(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETJson:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalJSON(ctx, input, buf); err != nil {
				return err
			}
		}
	default: // Should never go into here.
		return errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	// even if args[1].IsNull(i)
	result.ResizeInt64(n, false)

	return nil
}

func (b *builtinLastInsertIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := int64(vars.StmtCtx.PrevLastInsertID)
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinLastInsertIDWithIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDWithIDSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := len(i64s) - 1; i >= 0; i-- {
		if !result.IsNull(i) {
			vars.SetLastInsertID(uint64(i64s[i]))
			break
		}
	}
	return nil
}

func (b *builtinVersionSig) vectorized() bool {
	return true
}

func (b *builtinVersionSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(mysql.ServerVersion)
	}
	return nil
}

func (b *builtinTiDBDecodeKeySig) vectorized() bool {
	return true
}

func (b *builtinTiDBDecodeKeySig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)

	is, err := b.GetDomainInfoSchema(ctx)
	if err != nil {
		return err
	}

	decode := DecodeKeyFromString
	if decode == nil {
		decode = func(_ types.Context, _ infoschema.MetaOnlyInfoSchema, s string) string {
			return s
		}
	}
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(decode(ctx.TypeCtx(), is, buf.GetString(i)))
	}
	return nil
}
