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

package builtin

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// See: https://dev.mysql.com/doc/refman/5.7/en/information-functions.html

func builtinDatabase(args []interface{}, data map[interface{}]interface{}) (v interface{}, err error) {
	c, ok := data[ExprEvalArgCtx]
	if !ok {
		return nil, errors.Errorf("Missing ExprEvalArgCtx when evalue builtin")
	}
	ctx := c.(context.Context)
	d := db.GetCurrentSchema(ctx)
	if d == "" {
		return nil, nil
	}
	return d, nil
}

func builtinFoundRows(arg []interface{}, data map[interface{}]interface{}) (interface{}, error) {
	c, ok := data[ExprEvalArgCtx]
	if !ok {
		return nil, errors.Errorf("Missing ExprEvalArgCtx when evalue builtin")
	}
	ctx := c.(context.Context)
	return variable.GetSessionVars(ctx).FoundRows, nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func builtinCurrentUser(args []interface{}, data map[interface{}]interface{}) (v interface{}, err error) {
	c, ok := data[ExprEvalArgCtx]
	if !ok {
		return nil, errors.Errorf("Missing ExprEvalArgCtx when evalue builtin")
	}
	ctx := c.(context.Context)
	return variable.GetSessionVars(ctx).User, nil
}

func builtinUser(args []interface{}, data map[interface{}]interface{}) (v interface{}, err error) {
	c, ok := data[ExprEvalArgCtx]
	if !ok {
		return nil, errors.Errorf("Missing ExprEvalArgCtx when evalue builtin")
	}
	ctx := c.(context.Context)
	return variable.GetSessionVars(ctx).User, nil
}

func builtinConnectionID(args []interface{}, data map[interface{}]interface{}) (v interface{}, err error) {
	c, ok := data[ExprEvalArgCtx]
	if !ok {
		return nil, errors.Errorf("Missing ExprEvalArgCtx when evalue builtin")
	}
	ctx := c.(context.Context)
	return variable.GetSessionVars(ctx).ConnectionID, nil
}

func builtinVersion(args []interface{}, data map[interface{}]interface{}) (v interface{}, err error) {
	return mysql.ServerVersion, nil
}
