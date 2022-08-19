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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

// test Regexp_like function when all parameters are constant
func TestRegexpLikeConst(t *testing.T) {
	ctx := createContext(t)

	// test Regexp_like without match type
	testsExcludeMatchType := []struct {
		pattern string
		input   string
		match   int64
		err     error
	}{
		{"^$", "a", 0, nil},
		{"a", "a", 1, nil},
		{"a", "b", 0, nil},
		{"aA", "aA", 1, nil},
		{".", "a", 1, nil},
		{"^.$", "ab", 0, nil},
		{"..", "b", 0, nil},
		{".ab", "aab", 1, nil},
		{".*", "abcd", 1, nil},
		{"(", "", 0, ErrRegexp},
		{"(*", "", 0, ErrRegexp},
		{"[a", "", 0, ErrRegexp},
		{"\\", "", 0, ErrRegexp},
	}

	for _, tt := range testsExcludeMatchType {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}

	// test Regexp_like with match type
	testsIncludeMatchType := []struct {
		pattern   string
		input     string
		matchType string
		match     int64
		err       error
	}{
		{"^$", "a", "", 0, nil},
		{"a", "a", "", 1, nil},
		{"a", "b", "", 0, nil},
		{"aA", "aA", "", 1, nil},
		{".", "a", "", 1, nil},
		{"^.$", "ab", "", 0, nil},
		{"..", "b", "", 0, nil},
		{".ab", "aab", "", 1, nil},
		{".*", "abcd", "", 1, nil},
		// Test case-insensitive
		{"AbC", "abc", "", 0, nil},
		{"AbC", "abc", "i", 1, nil}, // 10
		// Test multiple-line mode
		{"23$", "123\n321", "", 0, nil},
		{"23$", "123\n321", "m", 1, nil},
		// Test n flag
		{".", "\n", "", 0, nil},
		{".", "\n", "n", 1, nil}, // 14
		// Test rightmost rule
		{"aBc", "abc", "ic", 0, nil},
		{"aBc", "abc", "ci", 1, nil},
		// Test invalid match type
		{"abc", "abc", "p", 0, ErrRegexp},
		{"abc", "abc", "cpi", 0, ErrRegexp},
	}

	for _, tt := range testsIncludeMatchType {
		fc := funcs[ast.RegexpLike]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.matchType)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}
}

func TestRegexpLikeVec(t *testing.T) {

}
