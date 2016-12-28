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

type databaseFuncClass struct {
	baseFuncClass
}

type builtinDatabase struct {
	baseBuiltinFunc
}

func (b *databaseFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinDatabase{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabase) eval(_ []types.Datum) (d types.Datum, err error) {
	currentDB := b.ctx.GetSessionVars().CurrentDB
	if currentDB == "" {
		return d, nil
	}
	d.SetString(currentDB)
	return d, nil
}

type foundRowsFuncClass struct {
	baseFuncClass
}

type builtinFoundRows struct {
	baseBuiltinFunc
}

func (b *foundRowsFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinFoundRows{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

func (b *builtinFoundRows) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.StmtCtx.FoundRows())
	return d, nil
}

type currentUserFuncClass struct {
	baseFuncClass
}

type builtinCurrentUser struct {
	baseBuiltinFunc
}

func (b *currentUserFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinCurrentUser{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func (b *builtinCurrentUser) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type userFuncClass struct {
	baseFuncClass
}

type builtinUser struct {
	baseBuiltinFunc
}

func (b *userFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinUser{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

func (b *builtinUser) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type connectionIDFuncClass struct {
	baseFuncClass
}

type builtinConnectionID struct {
	baseBuiltinFunc
}

func (b *connectionIDFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinConnectionID{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

func (b *builtinConnectionID) eval(_ []types.Datum) (d types.Datum, err error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.ConnectionID)
	return d, nil
}

type lastInsertIDFuncClass struct {
	baseFuncClass
}

type builtinLastInsertID struct {
	baseBuiltinFunc
}

func (b *lastInsertIDFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLastInsertID{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
func (b *builtinLastInsertID) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	if len(args) == 1 {
		id, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		b.ctx.GetSessionVars().SetLastInsertID(uint64(id))
	}

	d.SetUint64(b.ctx.GetSessionVars().LastInsertID)
	return
}

type versionFuncClass struct {
	baseFuncClass
}

type builtinVersion struct {
	baseBuiltinFunc
}

func (b *versionFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinVersion{baseBuiltinFunc: newBaseBuiltinFunc(args, false, ctx)}
	f.self = f
	return f, nil
}

func (b *builtinVersion) eval(_ []types.Datum) (d types.Datum, err error) {
	d.SetString(mysql.ServerVersion)
	return d, nil
}
