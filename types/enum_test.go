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
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestEnum(t *testing.T) {
	t.Run("ParseEnum", func(t *testing.T) {
		tests := []struct {
			Elems    []string
			Name     string
			Expected int
		}{
			{[]string{"a", "b"}, "a", 1},
			{[]string{"a"}, "b", 0},
			{[]string{"a"}, "1", 1},
		}

		for _, collation := range []string{mysql.DefaultCollationName, "utf8_unicode_ci"} {
			for _, test := range tests {
				e, err := ParseEnum(test.Elems, test.Name, collation)
				if test.Expected == 0 {
					require.Error(t, err)
					require.Equal(t, float64(0), e.ToNumber())
					require.Equal(t, "", e.String())
					continue
				}

				require.NoError(t, err)
				require.Equal(t, test.Elems[test.Expected-1], e.String())
				require.Equal(t, float64(test.Expected), e.ToNumber())
			}
		}
	})

	t.Run("ParseEnum_ci", func(t *testing.T) {
		tests := []struct {
			Elems    []string
			Name     string
			Expected int
		}{
			{[]string{"a", "b"}, "A     ", 1},
			{[]string{"a"}, "A", 1},
			{[]string{"a"}, "b", 0},
			{[]string{"啊"}, "啊", 1},
			{[]string{"a"}, "1", 1},
		}

		for _, test := range tests {
			e, err := ParseEnum(test.Elems, test.Name, "utf8_general_ci")
			if test.Expected == 0 {
				require.Error(t, err)
				require.Equal(t, float64(0), e.ToNumber())
				require.Equal(t, "", e.String())
				continue
			}

			require.NoError(t, err)
			require.Equal(t, test.Elems[test.Expected-1], e.String())
			require.Equal(t, float64(test.Expected), e.ToNumber())
		}
	})

	t.Run("ParseEnumValue", func(t *testing.T) {
		tests := []struct {
			Elems    []string
			Number   uint64
			Expected int
		}{
			{[]string{"a"}, 1, 1},
			{[]string{"a"}, 0, 0},
		}

		for _, test := range tests {
			e, err := ParseEnumValue(test.Elems, test.Number)
			if test.Expected == 0 {
				require.Error(t, err)
				continue
			}

			require.NoError(t, err)
			require.Equal(t, float64(test.Expected), e.ToNumber())
		}
	})
}
