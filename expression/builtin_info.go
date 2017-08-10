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
	_ builtinFunc = &builtinBenchmarkSig{}
	_ builtinFunc = &builtinCharsetSig{}
	_ builtinFunc = &builtinCoercibilitySig{}
	_ builtinFunc = &builtinCollationSig{}
	_ builtinFunc = &builtinRowCountSig{}
	_ builtinFunc = &builtinTiDBVersionSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinDatabaseSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinDatabaseSig struct {
	baseBuiltinFunc
}

// eval evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) eval(_ []types.Datum) (d types.Datum, err error) {
	currentDB := b.ctx.GetSessionVars().CurrentDB
	if currentDB == "" {
		return d, nil
	}
	d.SetString(currentDB)
	return d, nil
}

type foundRowsFunctionClass struct {
	baseFunctionClass
}

func (c *foundRowsFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinFoundRowsSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinFoundRowsSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFoundRowsSig.
// See https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_found-rows
// TODO: SQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundRowsSig) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.LastFoundRows)
	return d, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinCurrentUserSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinCurrentUserSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func (b *builtinCurrentUserSig) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinUserSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinUserSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinConnectionIDSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinConnectionIDSig struct {
	baseBuiltinFunc
}

func (b *builtinConnectionIDSig) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.ConnectionID)
	return d, nil
}

type lastInsertIDFunctionClass struct {
	baseFunctionClass
}

func (c *lastInsertIDFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argsTp := []evalTp{}
	if len(args) == 1 {
		argsTp = append(argsTp, tpInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argsTp...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flag |= mysql.UnsignedFlag
	bf.deterministic = false

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

func (c *versionFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinVersionSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinVersionSig struct {
	baseBuiltinFunc
}

// eval evals a builtinVersionSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) eval(_ []types.Datum) (d types.Datum, err error) {
	d.SetString(mysql.ServerVersion)
	return d, nil
}

type tidbVersionFunctionClass struct {
	baseFunctionClass
}

func (c *tidbVersionFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *benchmarkFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinBenchmarkSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinBenchmarkSig struct {
	baseBuiltinFunc
}

// eval evals a builtinBenchmarkSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
func (b *builtinBenchmarkSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("BENCHMARK")
}

type charsetFunctionClass struct {
	baseFunctionClass
}

func (c *charsetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCharsetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinCharsetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCharsetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_charset
func (b *builtinCharsetSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("CHARSET")
}

type coercibilityFunctionClass struct {
	baseFunctionClass
}

func (c *coercibilityFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCoercibilitySig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinCoercibilitySig struct {
	baseBuiltinFunc
}

// eval evals a builtinCoercibilitySig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_coercibility
func (b *builtinCoercibilitySig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("COERCIBILITY")
}

type collationFunctionClass struct {
	baseFunctionClass
}

func (c *collationFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCollationSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinCollationSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCollationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_collation
func (b *builtinCollationSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("COLLATION")
}

type rowCountFunctionClass struct {
	baseFunctionClass
}

func (c *rowCountFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinRowCountSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinRowCountSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRowCountSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinRowCountSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ROW_COUNT")
}
