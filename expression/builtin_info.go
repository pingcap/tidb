// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &databaseFunctionClass{}
	_ functionClass = &foundRowsFunctionClass{}
	_ functionClass = &currentUserFunctionClass{}
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
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundRowsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinConnectionIDSig{}
	_ builtinFunc = &builtinLastInsertIDSig{}
	_ builtinFunc = &builtinLastInsertIDWithIDSig{}
	_ builtinFunc = &builtinVersionSig{}
	_ builtinFunc = &builtinTiDBVersionSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.tp.Flen = 64
	bf.foldable = false
	sig := &builtinDatabaseSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDatabaseSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) evalString(row []types.Datum) (string, bool, error) {
	currentDB := b.ctx.GetSessionVars().CurrentDB
	return currentDB, currentDB == "", nil
}

type foundRowsFunctionClass struct {
	baseFunctionClass
}

func (c *foundRowsFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	bf.foldable = false
	sig := &builtinFoundRowsSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinFoundRowsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinFoundRowsSig.
// See https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_found-rows
// TODO: SQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundRowsSig) evalInt(row []types.Datum) (int64, bool, error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable when eval builtin")
	}
	return int64(data.LastFoundRows), false, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.tp.Flen = 64
	bf.foldable = false
	sig := &builtinCurrentUserSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCurrentUserSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func (b *builtinCurrentUserSig) evalString(row []types.Datum) (string, bool, error) {
	data := b.ctx.GetSessionVars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}

	return data.User.String(), false, nil
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.foldable = false
	bf.tp.Flen = 64
	sig := &builtinUserSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUserSig struct {
	baseStringBuiltinFunc
}

// eval evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) evalString(row []types.Datum) (string, bool, error) {
	data := b.ctx.GetSessionVars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}

	return data.User.String(), false, nil
}

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt)
	bf.foldable = false
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinConnectionIDSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinConnectionIDSig struct {
	baseIntBuiltinFunc
}

func (b *builtinConnectionIDSig) evalInt(_ []types.Datum) (int64, bool, error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable when evalue builtin")
	}
	return int64(data.ConnectionID), false, nil
}

type lastInsertIDFunctionClass struct {
	baseFunctionClass
}

func (c *lastInsertIDFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argsTp := []evalTp{}
	if len(args) == 1 {
		argsTp = append(argsTp, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argsTp...)
	bf.tp.Flag |= mysql.UnsignedFlag
	bf.foldable = false

	if len(args) == 1 {
		sig = &builtinLastInsertIDWithIDSig{baseIntBuiltinFunc{bf}}
	} else {
		sig = &builtinLastInsertIDSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), errors.Trace(err)
}

type builtinLastInsertIDSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LAST_INSERT_ID().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	res = int64(b.ctx.GetSessionVars().PrevLastInsertID)
	return res, false, nil
}

type builtinLastInsertIDWithIDSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LAST_INSERT_ID(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDWithIDSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	b.ctx.GetSessionVars().SetLastInsertID(uint64(res))
	return res, false, nil
}

type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.tp.Flen = 64
	bf.foldable = false
	sig := &builtinVersionSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinVersionSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinVersionSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) evalString(row []types.Datum) (string, bool, error) {
	return mysql.ServerVersion, false, nil
}

type tidbVersionFunctionClass struct {
	baseFunctionClass
}

func (c *tidbVersionFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.tp.Flen = len(printer.GetTiDBInfo())
	sig := &builtinTiDBVersionSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTiDBVersionSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTiDBVersionSig.
// This will show git hash and build time for tidb-server.
func (b *builtinTiDBVersionSig) evalString(_ []types.Datum) (string, bool, error) {
	return printer.GetTiDBInfo(), false, nil
}

type benchmarkFunctionClass struct {
	baseFunctionClass
}

func (c *benchmarkFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("BENCHMARK")
}

type charsetFunctionClass struct {
	baseFunctionClass
}

func (c *charsetFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("CHARSET")
}

type coercibilityFunctionClass struct {
	baseFunctionClass
}

func (c *coercibilityFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("COERCIBILITY")
}

type collationFunctionClass struct {
	baseFunctionClass
}

func (c *collationFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("COLLATION")
}

type rowCountFunctionClass struct {
	baseFunctionClass
}

func (c *rowCountFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("ROW_COUNT")
}
