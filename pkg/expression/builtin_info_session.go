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

package expression

import (
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	expropt.SessionVarsPropReader
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
	expropt.CurrentUserPropReader
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
	expropt.CurrentUserPropReader
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
	expropt.SessionVarsPropReader
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
	expropt.CurrentUserPropReader
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
	expropt.SessionVarsPropReader
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
	expropt.SessionVarsPropReader
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
	expropt.SessionVarsPropReader
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
