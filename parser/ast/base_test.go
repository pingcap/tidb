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
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ast is the abstract syntax tree parsed from a SQL statement by parser.
// It can be analysed and transformed by optimizer.
package ast

import (
	"testing"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/stretchr/testify/require"
)

func TestNodeSetText(t *testing.T) {
	n := &node{}
	tests := []struct {
		text           string
		enc            charset.Encoding
		expectUTF8Text string
		expectText     string
	}{
		{"你好", nil, "你好", "你好"},
		{"\xd2\xbb", charset.EncodingGBKImpl, "一", "\xd2\xbb"},
		{"\xc1\xd0", charset.EncodingGBKImpl, "列", "\xc1\xd0"},
	}
	for _, tt := range tests {
		n.SetText(tt.enc, tt.text)
		require.Equal(t, tt.expectUTF8Text, n.Text())
		require.Equal(t, tt.expectText, n.OriginalText())
	}
}
