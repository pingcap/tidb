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

import "testing"

func TestFormatCellPreservesAndEscapesBytes(t *testing.T) {
	testCases := []struct {
		name  string
		value string
		want  string
	}{
		{name: "text", value: "TiDB", want: "TiDB"},
		{name: "embedded NUL", value: "a\x00b", want: "a\x00b"},
		{name: "embedded newline", value: "a\nb", want: "BYTES_HEX:610A62"},
		{name: "embedded carriage return", value: "a\rb", want: "BYTES_HEX:610D62"},
		{name: "invalid UTF-8", value: string([]byte{0xff, 0, 'A'}), want: "BYTES_HEX:FF0041"},
		{name: "hex marker text", value: "BYTES_HEX:FF", want: "TEXT:BYTES_HEX:FF"},
		{name: "text marker text", value: "TEXT:value", want: "TEXT:TEXT:value"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := formatCell(testCase.value); got != testCase.want {
				t.Fatalf("formatCell(%q) = %q, want %q", testCase.value, got, testCase.want)
			}
		})
	}
}
