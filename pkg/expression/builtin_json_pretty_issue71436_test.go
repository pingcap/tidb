// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// TestJSONPrettyIssue71436 distinguishes SQL booleans from valid JSON text.
func TestJSONPrettyIssue71436(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expr     string
		want     string
		wantErr  bool
		wantNull bool
	}{
		{name: "json_boolean_text", expr: "JSON_PRETTY('true')", want: "true"},
		{name: "sql_boolean", expr: "JSON_PRETTY(TRUE)", wantErr: true},
		{name: "sql_false", expr: "JSON_PRETTY(FALSE)", wantErr: true},
		{name: "sql_integer", expr: "JSON_PRETTY(1)", wantErr: true},
		{name: "sql_decimal", expr: "JSON_PRETTY(1.5)", wantErr: true},
		{name: "sql_double", expr: "JSON_PRETTY(1e0)", wantErr: true},
		{name: "json_number_text", expr: "JSON_PRETTY('1')", want: "1"},
		{name: "json_boolean", expr: "JSON_PRETTY(CAST('true' AS JSON))", want: "true"},
		{name: "json_null", expr: "JSON_PRETTY('null')", want: "null"},
		{name: "sql_null", expr: "JSON_PRETTY(NULL)", wantNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := createContext(t)
			expr, err := ParseSimpleExpr(ctx, tc.expr)
			var result types.Datum
			if err == nil {
				result, err = expr.Eval(ctx, chunk.Row{})
			}
			if tc.wantErr {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(ErrInvalidTypeForJSON.GenWithStackByArgs(1, "json_pretty"), err), "unexpected error: %v", err)
				return
			}
			require.NoError(t, err)
			if tc.wantNull {
				require.True(t, result.IsNull())
				return
			}
			require.Equal(t, tc.want, result.GetString())
		})
	}
}
