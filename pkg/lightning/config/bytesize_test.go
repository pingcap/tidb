// Copyright 2020 PingCAP, Inc.
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

package config_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestByteSizeTOMLDecode(t *testing.T) {
	testCases := []struct {
		input  string
		output config.ByteSize
		err    string
	}{
		{
			input:  "x = 10000",
			output: 10000,
		},
		{
			input:  "x = 107_374_182_400",
			output: 107_374_182_400,
		},
		{
			input:  "x = '10k'",
			output: 10 * 1024,
		},
		{
			input:  "x = '10PiB'",
			output: 10 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			input:  "x = '10 KB'",
			output: 10 * 1024,
		},
		{
			input:  "x = '32768'",
			output: 32768,
		},
		{
			input: "x = -1",
			err:   "invalid size: '-1'",
		},
		{
			input: "x = 'invalid value'",
			err:   "strconv.ParseFloat: parsing \"invalid\": invalid syntax",
		},
		{
			input: "x = true",
			err:   "invalid size: 'true'",
		},
		{
			input:  "x = 256.0",
			output: 256,
		},
		{
			input:  "x = 256.9",
			output: 256,
		},
		{
			input:  "x = 10e+9",
			output: 10_000_000_000,
		},
		{
			input:  "x = '2.5MB'",
			output: 5 * 512 * 1024,
		},
		{
			input: "x = 2020-01-01T00:00:00Z",
			err:   "strconv.ParseFloat: parsing \"2020-01-01T00:00:00\": invalid syntax",
		},
		{
			input: "x = ['100000']",
			err:   "toml: .* incompatible types:.*",
		},
		{
			input: "x = { size = '100000' }",
			err:   "toml: .* incompatible types:.*",
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("input: `%s`", tc.input)
		var output struct{ X config.ByteSize }
		err := toml.Unmarshal([]byte(tc.input), &output)
		if tc.err != "" {
			require.Error(t, err)
			require.Regexp(t, tc.err, err.Error(), comment)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.output, output.X, comment)
		}
	}
}

func TestByteSizeTOMLAndJSONEncode(t *testing.T) {
	var input struct {
		X config.ByteSize `toml:"x" json:"x"`
	}
	input.X = 1048576

	var output strings.Builder
	err := toml.NewEncoder(&output).Encode(input)
	require.NoError(t, err)
	require.Equal(t, "x = 1048576\n", output.String())

	js, err := json.Marshal(input)
	require.NoError(t, err)
	require.Equal(t, `{"x":1048576}`, string(js))
}
