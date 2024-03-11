// Copyright 2024 PingCAP, Inc.
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

package redact

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestRedact(t *testing.T) {
	for _, c := range []struct {
		mode   string
		input  string
		output string
	}{
		{"OFF", "fxcv", "fxcv"},
		{"OFF", "f‹xcv", "f‹xcv"},
		{"ON", "f‹xcv", ""},
		{"MARKER", "f‹xcv", "‹f‹‹xcv›"},
		{"MARKER", "f›xcv", "‹f››xcv›"},
	} {
		require.Equal(t, c.output, Redact(c.mode, c.input))
		require.Equal(t, c.output, RedactStringer(c.mode, &testStringer{c.input}))
	}
}
