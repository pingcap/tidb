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
	"bytes"
	"encoding/hex"
	"strings"
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
		require.Equal(t, c.output, String(c.mode, c.input))
		require.Equal(t, c.output, Stringer(c.mode, &testStringer{c.input}).String())
	}
}

func TestDeRedact(t *testing.T) {
	for _, c := range []struct {
		remove bool
		input  string
		output string
	}{
		{true, "‹fxcv›ggg", "?ggg"},
		{false, "‹fxcv›ggg", "fxcvggg"},
		{true, "fxcv", "fxcv"},
		{false, "fxcv", "fxcv"},
		{true, "‹fxcv›ggg‹fxcv›eee", "?ggg?eee"},
		{false, "‹fxcv›ggg‹fxcv›eee", "fxcvgggfxcveee"},
		{true, "‹›", "?"},
		{false, "‹›", ""},
		{true, "gg‹ee", "gg‹ee"},
		{false, "gg‹ee", "gg‹ee"},
		{true, "gg›ee", "gg›ee"},
		{false, "gg›ee", "gg›ee"},
		{true, "gg‹ee‹ee", "gg‹ee‹ee"},
		{false, "gg‹ee‹gg", "gg‹ee‹gg"},
		{true, "gg›ee›gg", "gg›ee›gg"},
		{false, "gg›ee›ee", "gg›ee›ee"},
	} {
		w := bytes.NewBuffer(nil)
		require.NoError(t, DeRedact(c.remove, strings.NewReader(c.input), w, ""))
		require.Equal(t, c.output, w.String())
	}
}

func TestRedactInitAndValueAndKey(t *testing.T) {
	redacted, secret := "?", "secret"

	InitRedact(false)
	require.Equal(t, Value(secret), secret)
	require.Equal(t, Key([]byte(secret)), hex.EncodeToString([]byte(secret)))

	InitRedact(true)
	require.Equal(t, Value(secret), redacted)
	require.Equal(t, Key([]byte(secret)), redacted)
}
