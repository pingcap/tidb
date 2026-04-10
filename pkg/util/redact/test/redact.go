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

package redact_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/stretchr/testify/require"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func RunRedact(t *testing.T) {
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
		require.Equal(t, c.output, redact.ExportedString(c.mode, c.input))
		require.Equal(t, c.output, redact.ExportedStringer(c.mode, &testStringer{c.input}).String())
	}
}

func RunDeRedact(t *testing.T) {
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
		require.NoError(t, redact.ExportedDeRedact(c.remove, strings.NewReader(c.input), w, ""))
		require.Equal(t, c.output, w.String())
	}
}

func RunRedactInitAndValueAndKey(t *testing.T) {
	redacted, secret := "?", "secret"

	redact.ExportedInitRedact(false)
	require.Equal(t, redact.ExportedValue(secret), secret)
	require.Equal(t, redact.ExportedKey([]byte(secret)), hex.EncodeToString([]byte(secret)))

	redact.ExportedInitRedact(true)
	require.Equal(t, redact.ExportedValue(secret), redacted)
	require.Equal(t, redact.ExportedKey([]byte(secret)), redacted)
}
