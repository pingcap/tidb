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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	elems := []string{"a", "b", "c", "d"}

	t.Run("ParseSet", func(t *testing.T) {
		tests := []struct {
			Name          string
			ExpectedValue uint64
			ExpectedName  string
		}{
			{"a", 1, "a"},
			{"a,b,a", 3, "a,b"},
			{"b,a", 3, "a,b"},
			{"a,b,c,d", 15, "a,b,c,d"},
			{"d", 8, "d"},
			{"", 0, ""},
			{"0", 0, ""},
		}

		for _, collation := range []string{mysql.DefaultCollationName, "utf8_unicode_ci"} {
			for _, test := range tests {
				e, err := ParseSet(elems, test.Name, collation)
				require.NoError(t, err)
				require.Equal(t, float64(test.ExpectedValue), e.ToNumber())
				require.Equal(t, test.ExpectedName, e.String())
			}
		}
	})

	t.Run("ParseSet_ci", func(t *testing.T) {
		tests := []struct {
			Name          string
			ExpectedValue uint64
			ExpectedName  string
		}{
			{"A ", 1, "a"},
			{"a,B,a", 3, "a,b"},
		}

		for _, test := range tests {
			e, err := ParseSet(elems, test.Name, "utf8_general_ci")
			require.NoError(t, err)
			require.Equal(t, float64(test.ExpectedValue), e.ToNumber())
			require.Equal(t, test.ExpectedName, e.String())
		}
	})

	t.Run("ParseSetValue", func(t *testing.T) {
		tests := []struct {
			Number       uint64
			ExpectedName string
		}{
			{0, ""},
			{1, "a"},
			{3, "a,b"},
			{9, "a,d"},
		}

		for _, test := range tests {
			e, err := ParseSetValue(elems, test.Number)
			require.NoError(t, err)
			require.Equal(t, float64(test.Number), e.ToNumber())
			require.Equal(t, test.ExpectedName, e.String())
		}
	})

	t.Run("ParseSet_err", func(t *testing.T) {
		tests := []string{"a.e", "e.f"}
		for _, test := range tests {
			_, err := ParseSet(elems, test, mysql.DefaultCollationName)
			require.Error(t, err)
		}
	})

	t.Run("ParseSetValue_err", func(t *testing.T) {
		tests := []uint64{100, 16, 64}
		for _, test := range tests {
			_, err := ParseSetValue(elems, test)
			require.Error(t, err)
		}
	})
}
