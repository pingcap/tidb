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

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadPasswordRequiresTerminal(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "password")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	_, err = file.WriteString("PASSWORD_SECRET\n")
	require.NoError(t, err)
	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	password, err := readPassword(file)
	require.EqualError(t, err, "--ask-password requires a terminal")
	require.Empty(t, password)
	require.NotContains(t, err.Error(), "PASSWORD_SECRET")
	pos, err := file.Seek(0, 1)
	require.NoError(t, err)
	require.Zero(t, pos, "non-terminal input must not be consumed")
}

func TestReadPasswordLine(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    string
		password string
		wantErr  string
	}{
		{name: "empty", input: "\r"},
		{name: "spaces and tab", input: " secret\tvalue \r", password: " secret\tvalue "},
		{name: "unicode", input: "密码🥟\n", password: "密码🥟"},
		{name: "long", input: strings.Repeat("x", 5000) + "\n", password: strings.Repeat("x", 5000)},
		{name: "invalid UTF-8", input: "a\xffz\n", password: "a\xffz"},
		{name: "backspace", input: "a🥟\x7fz\n", password: "az"},
		{name: "backspace at start", input: "\b\x7fa\n", password: "a"},
		{name: "clear line", input: "secret\x15replacement\n", password: "replacement"},
		{name: "interrupt", input: "secret\x03", wantErr: "password prompt interrupted"},
		{name: "end of input", input: "secret\x04", wantErr: "EOF"},
		{name: "partial EOF", input: "secret", wantErr: "EOF"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			password, err := readPasswordLine(strings.NewReader(testCase.input))
			if testCase.wantErr != "" {
				require.EqualError(t, err, testCase.wantErr)
				require.Empty(t, password)
				require.NotContains(t, err.Error(), "secret")
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.password, password)
		})
	}
}
