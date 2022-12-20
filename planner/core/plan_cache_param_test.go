// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	"github.com/stretchr/testify/require"
)

func TestParameterize(t *testing.T) {
	sctx := MockContext()
	cases := []struct {
		sql        string
		paramSQL   string
		params     []interface{}
		restoreSQL string
	}{
		{
			"select * from t where a<10",
			"SELECT * FROM `t` WHERE `a`<?",
			[]interface{}{int64(10)},
			"SELECT * FROM `t` WHERE `a`<10",
		},
		{
			"select * from t",
			"SELECT * FROM `t`",
			[]interface{}{},
			"SELECT * FROM `t`",
		},
		{
			"select * from t where a<10 and b<20 and c=30 and d>40",
			"SELECT * FROM `t` WHERE `a`<? AND `b`<? AND `c`=? AND `d`>?",
			[]interface{}{int64(10), int64(20), int64(30), int64(40)},
			"SELECT * FROM `t` WHERE `a`<10 AND `b`<20 AND `c`=30 AND `d`>40",
		},
		{
			"select * from t where a='a' and b='bbbbbbbbbbbbbbbbbbbbbbbb'",
			"SELECT * FROM `t` WHERE `a`=? AND `b`=?",
			[]interface{}{"a", "bbbbbbbbbbbbbbbbbbbbbbbb"},
			"SELECT * FROM `t` WHERE `a`=_UTF8MB4'a' AND `b`=_UTF8MB4'bbbbbbbbbbbbbbbbbbbbbbbb'",
		},
		// TODO: more test cases
	}

	for _, c := range cases {
		stmt, err := parser.New().ParseOneStmt(c.sql, "", "")
		require.Nil(t, err)
		paramSQL, params, err := ParameterizeAST(context.Background(), sctx, stmt)
		require.Nil(t, err)
		require.Equal(t, c.paramSQL, paramSQL)
		require.Equal(t, len(c.params), len(params))
		for i := range params {
			require.Equal(t, c.params[i], params[i].Datum.GetValue())
		}

		err = RestoreASTWithParams(context.Background(), sctx, stmt, params)
		require.Nil(t, err)
		var buf strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		require.Nil(t, stmt.Restore(rCtx))
		require.Equal(t, c.restoreSQL, buf.String())
	}
}
