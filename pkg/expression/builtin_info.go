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
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/plancodec"
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
	_ builtinFunc = &builtinTiDBDecodeKeySig{}
	_ builtinFunc = &builtinTiDBDecodeSQLDigestsSig{}
	_ builtinFunc = &builtinNextValSig{}
	_ builtinFunc = &builtinLastValSig{}
	_ builtinFunc = &builtinSetValSig{}
	_ builtinFunc = &builtinFormatBytesSig{}
	_ builtinFunc = &builtinFormatNanoTimeSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinDatabaseSig{bf}
	return sig, nil
}

type builtinDatabaseSig struct {
	baseBuiltinFunc
}

func (b *builtinDatabaseSig) Clone() builtinFunc {
	newSig := &builtinDatabaseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	currentDB := ctx.CurrentDB()
	return currentDB, currentDB == "", nil
}

type foundRowsFunctionClass struct {
	baseFunctionClass
}

func (c *foundRowsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinFoundRowsSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinFoundRowsSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinFoundRowsSig) Clone() builtinFunc {
	newSig := &builtinFoundRowsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFoundRowsSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinFoundRowsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_found-rows
// TODO: SQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundRowsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable when eval builtin")
	}
	return int64(data.LastFoundRows), false, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinCurrentUserSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinCurrentUserSig struct {
	baseBuiltinFunc
	contextopt.CurrentUserPropReader
}

func (b *builtinCurrentUserSig) Clone() builtinFunc {
	newSig := &builtinCurrentUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinCurrentUserSig) RequiredOptionalEvalProps() (set OptionalEvalPropKeySet) {
	return b.CurrentUserPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) evalString(ctx EvalContext, _ chunk.Row) (string, bool, error) {
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return "", true, err
	}
	if user == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	return user.String(), false, nil
}

type currentRoleFunctionClass struct {
	baseFunctionClass
}

func (c *currentRoleFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinCurrentRoleSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinCurrentRoleSig struct {
	baseBuiltinFunc
	contextopt.CurrentUserPropReader
}

func (b *builtinCurrentRoleSig) Clone() builtinFunc {
	newSig := &builtinCurrentRoleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinCurrentRoleSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.CurrentUserPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_current-role
func (b *builtinCurrentRoleSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	roles, err := b.ActiveRoles(ctx)
	if err != nil {
		return "", true, err
	}
	if roles == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	if len(roles) == 0 {
		return "NONE", false, nil
	}
	sortedRes := make([]string, 0, 10)
	for _, r := range roles {
		sortedRes = append(sortedRes, r.String())
	}
	slices.Sort(sortedRes)
	for i, r := range sortedRes {
		res += r
		if i != len(roles)-1 {
			res += ","
		}
	}
	return res, false, nil
}

type currentResourceGroupFunctionClass struct {
	baseFunctionClass
}

func (c *currentResourceGroupFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinCurrentResourceGroupSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinCurrentResourceGroupSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinCurrentResourceGroupSig) Clone() builtinFunc {
	newSig := &builtinCurrentResourceGroupSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentResourceGroupSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinCurrentResourceGroupSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	if data == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}

	return getHintResourceGroupName(data), false, nil
}

// get statement resource group name with hint in consideration
// NOTE: because function `CURRENT_RESOURCE_GROUP()` maybe evaluated in optimizer
// before we assign the hint value to StmtCtx.ResourceGroupName, so we have to
// explicitly check the hint here.
func getHintResourceGroupName(vars *variable.SessionVars) string {
	groupName := vars.StmtCtx.ResourceGroupName
	if vars.StmtCtx.HasResourceGroup {
		groupName = vars.StmtCtx.StmtHints.ResourceGroup
	}
	return groupName
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinUserSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinUserSig struct {
	baseBuiltinFunc
	contextopt.CurrentUserPropReader
}

func (b *builtinUserSig) Clone() builtinFunc {
	newSig := &builtinUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinUserSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.CurrentUserPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) evalString(ctx EvalContext, _ chunk.Row) (string, bool, error) {
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return "", true, err
	}
	if user == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	return user.LoginString(), false, nil
}

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinConnectionIDSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinConnectionIDSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinConnectionIDSig) Clone() builtinFunc {
	newSig := &builtinConnectionIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinConnectionIDSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinConnectionIDSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}

	if data == nil {
		return 0, true, errors.Errorf("Missing session variable `builtinConnectionIDSig.evalInt`")
	}
	return int64(data.ConnectionID), false, nil
}

type lastInsertIDFunctionClass struct {
	baseFunctionClass
}

func (c *lastInsertIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	var argsTp []types.EvalType
	if len(args) == 1 {
		argsTp = append(argsTp, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argsTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.UnsignedFlag)

	if len(args) == 1 {
		sig = &builtinLastInsertIDWithIDSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_LastInsertIDWithID)
	} else {
		sig = &builtinLastInsertIDSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_LastInsertID)
	}
	return sig, err
}

type builtinLastInsertIDSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinLastInsertIDSig) Clone() builtinFunc {
	newSig := &builtinLastInsertIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLastInsertIDSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals LAST_INSERT_ID().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	res = int64(vars.StmtCtx.PrevLastInsertID)
	return res, false, nil
}

type builtinLastInsertIDWithIDSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinLastInsertIDWithIDSig) Clone() builtinFunc {
	newSig := &builtinLastInsertIDWithIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLastInsertIDWithIDSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals LAST_INSERT_ID(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDWithIDSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}

	res, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	vars.SetLastInsertID(uint64(res))
	return res, false, nil
}

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
	contextopt.DDLOwnerPropReader
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
		return 0, true, errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
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
	contextopt.SessionVarsPropReader
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

type tidbDecodeKeyFunctionClass struct {
	baseFunctionClass
}

func (c *tidbDecodeKeyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBDecodeKeySig{baseBuiltinFunc: bf}
	return sig, nil
}

// DecodeKeyFromString is used to decode key by expressions
var DecodeKeyFromString func(types.Context, infoschema.MetaOnlyInfoSchema, string) string

type builtinTiDBDecodeKeySig struct {
	baseBuiltinFunc
	contextopt.InfoSchemaPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBDecodeKeySig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.InfoSchemaPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBDecodeKeySig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTiDBDecodeKeySig.
func (b *builtinTiDBDecodeKeySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	s, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	is, err := b.GetDomainInfoSchema(ctx)
	if err != nil {
		return "", true, err
	}

	if fn := DecodeKeyFromString; fn != nil {
		s = fn(ctx.TypeCtx(), is, s)
	}
	return s, false, nil
}

type tidbDecodeSQLDigestsFunctionClass struct {
	baseFunctionClass
}

func (c *tidbDecodeSQLDigestsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	if !ctx.GetEvalCtx().RequestVerification("", "", "", mysql.ProcessPriv) {
		return nil, errSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}

	var argTps []types.EvalType
	if len(args) > 1 {
		argTps = []types.EvalType{types.ETString, types.ETInt}
	} else {
		argTps = []types.EvalType{types.ETString}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBDecodeSQLDigestsSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBDecodeSQLDigestsSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
	contextopt.SQLExecutorPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBDecodeSQLDigestsSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.SQLExecutorPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBDecodeSQLDigestsSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeSQLDigestsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodeSQLDigestsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	args := b.getArgs()
	digestsStr, isNull, err := args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}

	stmtTruncateLength := int64(0)
	if len(args) > 1 {
		stmtTruncateLength, isNull, err = args[1].EvalInt(ctx, row)
		if err != nil {
			return "", true, err
		}
		if isNull {
			stmtTruncateLength = 0
		}
	}

	var digests []any
	err = json.Unmarshal([]byte(digestsStr), &digests)
	if err != nil {
		const errMsgMaxLength = 32
		if len(digestsStr) > errMsgMaxLength {
			digestsStr = digestsStr[:errMsgMaxLength] + "..."
		}
		tc := typeCtx(ctx)
		tc.AppendWarning(errIncorrectArgs.FastGen("The argument can't be unmarshalled as JSON array: '%s'", digestsStr))
		return "", true, nil
	}

	// Query the SQL Statements by digests.
	retriever := NewSQLDigestTextRetriever()
	for _, item := range digests {
		if item != nil {
			digest, ok := item.(string)
			if ok {
				retriever.SQLDigestsMap[digest] = ""
			}
		}
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}

	// Querying may take some time and it takes a context.Context as argument, which is not available here.
	// We simply create a context with a timeout here.
	timeout := time.Duration(vars.GetMaxExecutionTime()) * time.Millisecond
	if timeout == 0 || timeout > 20*time.Second {
		timeout = 20 * time.Second
	}
	goCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	exec, err := b.GetSQLExecutor(ctx)
	if err != nil {
		return "", true, err
	}

	err = retriever.RetrieveGlobal(goCtx, exec)
	if err != nil {
		if errors.Cause(err) == context.DeadlineExceeded || errors.Cause(err) == context.Canceled {
			return "", true, errUnknown.GenWithStack("Retrieving cancelled internally with error: %v", err)
		}

		tc := typeCtx(ctx)
		tc.AppendWarning(errUnknown.FastGen("Retrieving statements information failed with error: %v", err))
		return "", true, nil
	}

	// Collect the result.
	result := make([]any, len(digests))
	for i, item := range digests {
		if item == nil {
			continue
		}
		if digest, ok := item.(string); ok {
			if stmt, ok := retriever.SQLDigestsMap[digest]; ok && len(stmt) > 0 {
				// Truncate too-long statements if necessary.
				if stmtTruncateLength > 0 && int64(len(stmt)) > stmtTruncateLength {
					stmt = stmt[:stmtTruncateLength] + "..."
				}
				result[i] = stmt
			}
		}
	}

	resultStr, err := json.Marshal(result)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(errUnknown.FastGen("Marshalling result as JSON failed with error: %v", err))
		return "", true, nil
	}

	return string(resultStr), false, nil
}

type tidbEncodeSQLDigestFunctionClass struct {
	baseFunctionClass
}

func (c *tidbEncodeSQLDigestFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBEncodeSQLDigestSig{bf}
	return sig, nil
}

type builtinTiDBEncodeSQLDigestSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBEncodeSQLDigestSig) Clone() builtinFunc {
	newSig := &builtinTiDBEncodeSQLDigestSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBEncodeSQLDigestSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	orgSQLStr, isNull, err := b.getArgs()[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}
	return parser.DigestHash(orgSQLStr).String(), false, nil
}

type tidbDecodePlanFunctionClass struct {
	baseFunctionClass
}

func (c *tidbDecodePlanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if c.funcName == ast.TiDBDecodePlan {
		return &builtinTiDBDecodePlanSig{bf}, nil
	} else if c.funcName == ast.TiDBDecodeBinaryPlan {
		return &builtinTiDBDecodeBinaryPlanSig{bf}, nil
	}
	return nil, errors.New("unknown decode plan function")
}

type builtinTiDBDecodePlanSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBDecodePlanSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodePlanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodePlanSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	planString, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodePlan(planString)
	if err != nil {
		return planString, false, nil
	}
	return planTree, false, nil
}

type builtinTiDBDecodeBinaryPlanSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBDecodeBinaryPlanSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeBinaryPlanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodeBinaryPlanSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	planString, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodeBinaryPlan(planString)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		return "", false, nil
	}
	return planTree, false, nil
}

type nextValFunctionClass struct {
	baseFunctionClass
}

func (c *nextValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinNextValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(10)
	return sig, nil
}

type builtinNextValSig struct {
	baseBuiltinFunc
	contextopt.SequenceOperatorPropReader
	contextopt.SessionVarsPropReader
}

func (b *builtinNextValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinNextValSig) Clone() builtinFunc {
	newSig := &builtinNextValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNextValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	if !ctx.RequestVerification(db, seq, "", mysql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	nextVal, err := sequence.GetSequenceNextVal()
	if err != nil {
		return 0, false, err
	}
	// update the sequenceState.
	vars.SequenceState.UpdateState(sequence.GetSequenceID(), nextVal)
	return nextVal, false, nil
}

type lastValFunctionClass struct {
	baseFunctionClass
}

func (c *lastValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinLastValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(10)
	return sig, nil
}

type builtinLastValSig struct {
	baseBuiltinFunc
	contextopt.SequenceOperatorPropReader
	contextopt.SessionVarsPropReader
}

func (b *builtinLastValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinLastValSig) Clone() builtinFunc {
	newSig := &builtinLastValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLastValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	if !ctx.RequestVerification(db, seq, "", mysql.SelectPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, seq)
	}
	return vars.SequenceState.GetLastValue(sequence.GetSequenceID())
}

type setValFunctionClass struct {
	baseFunctionClass
}

func (c *setValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinSetValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(args[1].GetType(ctx.GetEvalCtx()).GetFlen())
	return sig, nil
}

type builtinSetValSig struct {
	baseBuiltinFunc
	contextopt.SequenceOperatorPropReader
	contextopt.SessionVarsPropReader
}

func (b *builtinSetValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetValSig) Clone() builtinFunc {
	newSig := &builtinSetValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	if !ctx.RequestVerification(db, seq, "", mysql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	setValue, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return sequence.SetSequenceVal(setValue)
}

func getSchemaAndSequence(sequenceName string) (string, string) {
	res := strings.Split(sequenceName, ".")
	if len(res) == 1 {
		return "", res[0]
	}
	return res[0], res[1]
}

type formatBytesFunctionClass struct {
	baseFunctionClass
}

func (c *formatBytesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	sig := &builtinFormatBytesSig{bf}
	return sig, nil
}

type builtinFormatBytesSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatBytesSig) Clone() builtinFunc {
	newSig := &builtinFormatBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatBytes evals a builtinFormatBytesSig.
// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html#function_format-bytes
func (b *builtinFormatBytesSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatBytes(val), false, nil
}

type formatNanoTimeFunctionClass struct {
	baseFunctionClass
}

func (c *formatNanoTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	sig := &builtinFormatNanoTimeSig{bf}
	return sig, nil
}

type builtinFormatNanoTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatNanoTimeSig) Clone() builtinFunc {
	newSig := &builtinFormatNanoTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatNanoTime evals a builtinFormatNanoTimeSig, as time unit in TiDB is always nanosecond, not picosecond.
// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html#function_format-pico-time
func (b *builtinFormatNanoTimeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatNanoTime(val), false, nil
}
