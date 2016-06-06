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

package evaluator

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// See: https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func builtinDatabase(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	s := db.GetCurrentSchema(ctx)
	if s == "" {
		return d, nil
	}
	d.SetString(s)
	return d, nil
}

func builtinFoundRows(arg []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := variable.GetSessionVars(ctx)
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.FoundRows)
	return d, nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func builtinCurrentUser(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := variable.GetSessionVars(ctx)
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

func builtinUser(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := variable.GetSessionVars(ctx)
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

func builtinConnectionID(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := variable.GetSessionVars(ctx)
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.ConnectionID)
	return d, nil
}

// See: http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
func builtinLastInsertID(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	if len(args) == 1 {
		id, err := args[0].ToInt64()
		if err != nil {
			return d, errors.Trace(err)
		}
		variable.GetSessionVars(ctx).SetLastInsertID(uint64(id))
	}

	d.SetUint64(variable.GetSessionVars(ctx).LastInsertID)
	return
}

func builtinVersion(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	d.SetString(mysql.ServerVersion)
	return d, nil
}
