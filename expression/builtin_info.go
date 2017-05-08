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
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundRowsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinConnectionIDSig{}
	_ builtinFunc = &builtinLastInsertIDSig{}
	_ builtinFunc = &builtinVersionSig{}
	_ builtinFunc = &builtinBenchmarkSig{}
	_ builtinFunc = &builtinCharsetSig{}
	_ builtinFunc = &builtinCoercibilitySig{}
	_ builtinFunc = &builtinCollationSig{}
	_ builtinFunc = &builtinRowCountSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinDatabaseSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
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
	return bt, errors.Trace(err)
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
	return bt, errors.Trace(err)
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
	return bt, errors.Trace(err)
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
	return bt, errors.Trace(err)
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

func (c *lastInsertIDFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinLastInsertIDSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
}

type builtinLastInsertIDSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLastInsertIDSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
func (b *builtinLastInsertIDSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if len(args) == 1 {
		id, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		b.ctx.GetSessionVars().SetLastInsertID(uint64(id))
		d.SetUint64(uint64(id))
	} else {
		d.SetUint64(b.ctx.GetSessionVars().PrevLastInsertID)
	}
	return
}

type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinVersionSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
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

type benchmarkFunctionClass struct {
	baseFunctionClass
}

func (c *benchmarkFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinBenchmarkSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinCharsetSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinCoercibilitySig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinCollationSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return bt, errors.Trace(err)
}

type builtinRowCountSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRowCountSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinRowCountSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ROW_COUNT")
}
