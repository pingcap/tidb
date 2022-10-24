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

package expression

import (
	"encoding/hex"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestStGeomFromTextFunc(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"POINT(0 0)"}, false, false, "00000000010100000000000000000000000000000000000000"},
		{[]interface{}{"POINT(0 1)"}, false, false, "0000000001010000000000000000000000000000000000f03f"},
		{[]interface{}{"POINT(1 0)"}, false, false, "000000000101000000000000000000f03f0000000000000000"},
		{[]interface{}{"POINT(1 1)"}, false, false, "000000000101000000000000000000f03f000000000000f03f"},
		{[]interface{}{"POINT(1)"}, false, true, ""},
		{[]interface{}{"POINT(0 0)", 4326}, false, false, "e6100000010100000000000000000000000000000000000000"},
		{[]interface{}{errors.New("must error")}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.StGeomFromText, primitiveValsToConstants(ctx, c.arg)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, hex.EncodeToString([]byte(d.GetString())))
			}
		}
	}
}

func TestStAsTextFunc(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", false, false, "POINT (0 0)"},
		{"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f", false, false, "POINT (0 1)"},
		{"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\x00\x00", false, false, "POINT (1 0)"},
		{"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f", false, false, "POINT (1 1)"},
		{"\xe6\x10\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", false, false, "POINT (0 0)"},
		{errors.New("must error"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.StAsText, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}
}
