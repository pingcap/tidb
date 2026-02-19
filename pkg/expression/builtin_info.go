// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/printer"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &databaseFunctionClass{}
	_ functionClass = &foundRowsFunctionClass{}
	_ functionClass = &currentUserFunctionClass{}
	_ functionClass = &currentRoleFunctionClass{}
	_ functionClass = &currentResourceGroupFunctionClass{}
	_ functionClass = &userFunctionClass{}
	_ functionClass = &connectionIDFunctionClass{}
	_ functionClass = &lastInsertIDFunctionClass{}
	_ functionClass = &versionFunctionClass{}
	_ functionClass = &benchmarkFunctionClass{}
	_ functionClass = &charsetFunctionClass{}
	_ functionClass = &coercibilityFunctionClass{}
	_ functionClass = &collationFunctionClass{}
	_ functionClass = &rowCountFunctionClass{}
	_ functionClass = &tidbVersionFunctionClass{}
	_ functionClass = &tidbIsDDLOwnerFunctionClass{}
	_ functionClass = &tidbDecodePlanFunctionClass{}
	_ functionClass = &tidbMVCCInfoFunctionClass{}
	_ functionClass = &tidbEncodeRecordKeyClass{}
	_ functionClass = &tidbEncodeIndexKeyClass{}
	_ functionClass = &tidbDecodeKeyFunctionClass{}
	_ functionClass = &tidbDecodeSQLDigestsFunctionClass{}
	_ functionClass = &nextValFunctionClass{}
	_ functionClass = &lastValFunctionClass{}
	_ functionClass = &setValFunctionClass{}
	_ functionClass = &formatBytesFunctionClass{}
	_ functionClass = &formatNanoTimeFunctionClass{}
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundRowsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinCurrentResourceGroupSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinConnectionIDSig{}
	_ builtinFunc = &builtinLastInsertIDSig{}
	_ builtinFunc = &builtinLastInsertIDWithIDSig{}
	_ builtinFunc = &builtinVersionSig{}
	_ builtinFunc = &builtinTiDBVersionSig{}
	_ builtinFunc = &builtinRowCountSig{}
	_ builtinFunc = &builtinTiDBMVCCInfoSig{}
	_ builtinFunc = &builtinTiDBEncodeRecordKeySig{}
	_ builtinFunc = &builtinTiDBEncodeIndexKeySig{}
	_ builtinFunc = &builtinTiDBDecodeKeySig{}
	_ builtinFunc = &builtinTiDBDecodeSQLDigestsSig{}
	_ builtinFunc = &builtinNextValSig{}
	_ builtinFunc = &builtinLastValSig{}
	_ builtinFunc = &builtinSetValSig{}
	_ builtinFunc = &builtinFormatBytesSig{}
	_ builtinFunc = &builtinFormatNanoTimeSig{}
)


type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinVersionSig{bf}
	return sig, nil
}

type builtinVersionSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinVersionSig) Clone() builtinFunc {
	newSig := &builtinVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinVersionSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return mysql.ServerVersion, false, nil
}

type tidbVersionFunctionClass struct {
	baseFunctionClass
}

func (c *tidbVersionFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(len(printer.GetTiDBInfo()))
	sig := &builtinTiDBVersionSig{bf}
	return sig, nil
}

type builtinTiDBVersionSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTiDBVersionSig) Clone() builtinFunc {
	newSig := &builtinTiDBVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBVersionSig.
// This will show git hash and build time for tidb-server.
func (b *builtinTiDBVersionSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return printer.GetTiDBInfo(), false, nil
}

type tidbIsDDLOwnerFunctionClass struct {
	baseFunctionClass
}

func (c *tidbIsDDLOwnerFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBIsDDLOwnerSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBIsDDLOwnerSig struct {
	baseBuiltinFunc
	expropt.DDLOwnerPropReader
}

func (b *builtinTiDBIsDDLOwnerSig) Clone() builtinFunc {
	newSig := &builtinTiDBIsDDLOwnerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBIsDDLOwnerSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.DDLOwnerPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinTiDBIsDDLOwnerSig.
func (b *builtinTiDBIsDDLOwnerSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	isOwner, err := b.IsDDLOwner(ctx)
	if err != nil {
		return 0, true, err
	}

	if isOwner {
		res = 1
	}

	return res, false, nil
}

type benchmarkFunctionClass struct {
	baseFunctionClass
}

func (c *benchmarkFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	// Syntax: BENCHMARK(loop_count, expression)
	// Define with same eval type of input arg to avoid unnecessary cast function.
	sameEvalType := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	// constLoopCount is used by VecEvalInt
	// since non-constant loop count would be different between rows, and cannot be vectorized.
	var constLoopCount int64
	con, ok := args[0].(*Constant)
	if ok && con.Value.Kind() == types.KindInt64 {
		if lc, isNull, err := con.EvalInt(ctx.GetEvalCtx(), chunk.Row{}); err == nil && !isNull {
			constLoopCount = lc
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, sameEvalType)
	if err != nil {
		return nil, err
	}
	sig := &builtinBenchmarkSig{bf, constLoopCount}
	return sig, nil
}

type builtinBenchmarkSig struct {
	baseBuiltinFunc
	constLoopCount int64
}

func (b *builtinBenchmarkSig) Clone() builtinFunc {
	newSig := &builtinBenchmarkSig{constLoopCount: b.constLoopCount}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinBenchmarkSig. It will execute expression repeatedly count times.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
func (b *builtinBenchmarkSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	// Get loop count.
	var loopCount int64
	var isNull bool
	var err error
	if b.constLoopCount > 0 {
		loopCount = b.constLoopCount
	} else {
		loopCount, isNull, err = b.args[0].EvalInt(ctx, row)
		if isNull || err != nil {
			return 0, isNull, err
		}
	}

	// BENCHMARK() will return NULL if loop count < 0,
	// behavior observed on MySQL 5.7.24.
	if loopCount < 0 {
		return 0, true, nil
	}

	// Eval loop count times based on arg type.
	// BENCHMARK() will pass-through the eval error,
	// behavior observed on MySQL 5.7.24.
	var i int64
	arg := b.args[1]
	switch evalType := arg.GetType(ctx).EvalType(); evalType {
	case types.ETInt:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalInt(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETReal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalReal(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDecimal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDecimal(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETString:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalString(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalTime(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDuration:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDuration(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETJson:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalJSON(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	default: // Should never go into here.
		return 0, true, errors.Errorf("%s is not supported for BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	return 0, false, nil
}

type charsetFunctionClass struct {
	baseFunctionClass
}

func (c *charsetFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argsTps := make([]types.EvalType, 0, len(args))
	for _, arg := range args {
		argsTps = append(argsTps, arg.GetType(ctx.GetEvalCtx()).EvalType())
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argsTps...)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinCharsetSig{bf}
	return sig, nil
}

type builtinCharsetSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCharsetSig) Clone() builtinFunc {
	newSig := &builtinCharsetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCharsetSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return b.args[0].GetType(ctx).GetCharset(), false, nil
}

type coercibilityFunctionClass struct {
	baseFunctionClass
}

func (c *coercibilityFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, args[0].GetType(ctx.GetEvalCtx()).EvalType())
	if err != nil {
		return nil, err
	}
	sig := &builtinCoercibilitySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	return sig, nil
}

type builtinCoercibilitySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *builtinCoercibilitySig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return int64(c.args[0].Coercibility()), false, nil
}

func (c *builtinCoercibilitySig) Clone() builtinFunc {
	newSig := &builtinCoercibilitySig{}
	newSig.cloneFrom(&c.baseBuiltinFunc)
	return newSig
}

type collationFunctionClass struct {
	baseFunctionClass
}

func (c *collationFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argsTps := make([]types.EvalType, 0, len(args))
	for _, arg := range args {
		argsTps = append(argsTps, arg.GetType(ctx.GetEvalCtx()).EvalType())
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argsTps...)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinCollationSig{bf}
	return sig, nil
}

type builtinCollationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCollationSig) Clone() builtinFunc {
	newSig := &builtinCollationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCollationSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return b.args[0].GetType(ctx).GetCollate(), false, nil
}

type rowCountFunctionClass struct {
	baseFunctionClass
}

func (c *rowCountFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig = &builtinRowCountSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_RowCount)
	return sig, nil
}

type builtinRowCountSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinRowCountSig) Clone() builtinFunc {
	newSig := &builtinRowCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRowCountSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count.
func (b *builtinRowCountSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	res = vars.StmtCtx.PrevAffectedRows
	return res, false, nil
}

