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

	"github.com/pingcap/errors"
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

func TestAny(t *testing.T) {
	redacted := "?"

	// Test with redaction OFF
	InitRedact(false)
	require.Equal(t, "123", Any(123))
	require.Equal(t, "true", Any(true))
	require.Equal(t, "sensitive-data", Any("sensitive-data"))
	require.Equal(t, "NULL", Any(nil))

	// Test with redaction ON
	InitRedact(true)
	require.Equal(t, redacted, Any(123))
	require.Equal(t, redacted, Any(true))
	require.Equal(t, redacted, Any("sensitive-data"))
	require.Equal(t, "NULL", Any(nil)) // nil is always "NULL"
}

func TestArgs(t *testing.T) {
	redacted := "?"

	// Test with redaction OFF
	InitRedact(false)
	require.Equal(t, "()", Args([]any{}))
	require.Equal(t, "(NULL)", Args([]any{nil}))
	require.Equal(t, "(123)", Args([]any{123}))
	require.Equal(t, "(password123)", Args([]any{"password123"}))
	require.Equal(t, "(1, user@example.com, password123, NULL)",
		Args([]any{1, "user@example.com", "password123", nil}))

	// Test with redaction ON
	InitRedact(true)
	require.Equal(t, "()", Args([]any{}))
	require.Equal(t, "(NULL)", Args([]any{nil}))
	require.Equal(t, "("+redacted+")", Args([]any{123}))
	require.Equal(t, "("+redacted+")", Args([]any{"password123"}))
	require.Equal(t, "("+redacted+", "+redacted+", "+redacted+", NULL)",
		Args([]any{1, "user@example.com", "password123", nil}))
}

func TestParseRedactMode(t *testing.T) {
	// Test ON variants
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("ON"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("on"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("TRUE"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("true"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("1"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("ENABLE"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("enable"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("ENABLED"))
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("enabled"))

	// Test MARKER variants
	require.Equal(t, errors.RedactLogMarker, ParseRedactMode("MARKER"))
	require.Equal(t, errors.RedactLogMarker, ParseRedactMode("marker"))
	require.Equal(t, errors.RedactLogMarker, ParseRedactMode("MARK"))
	require.Equal(t, errors.RedactLogMarker, ParseRedactMode("mark"))

	// Test OFF variants
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("OFF"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("off"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("FALSE"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("false"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("0"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("DISABLE"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("disable"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("DISABLED"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("disabled"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode(""))

	// Test with whitespace
	require.Equal(t, errors.RedactLogEnable, ParseRedactMode("  ON  "))
	require.Equal(t, errors.RedactLogMarker, ParseRedactMode("  MARKER  "))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("  OFF  "))

	// Test unknown values default to OFF
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("INVALID"))
	require.Equal(t, errors.RedactLogDisable, ParseRedactMode("unknown"))
}

func TestIsValidRedactMode(t *testing.T) {
	// Test valid ON modes
	require.True(t, IsValidRedactMode("ON", ParseRedactMode("ON")))
	require.True(t, IsValidRedactMode("on", ParseRedactMode("on")))
	require.True(t, IsValidRedactMode("TRUE", ParseRedactMode("TRUE")))
	require.True(t, IsValidRedactMode("true", ParseRedactMode("true")))
	require.True(t, IsValidRedactMode("1", ParseRedactMode("1")))
	require.True(t, IsValidRedactMode("ENABLE", ParseRedactMode("ENABLE")))
	require.True(t, IsValidRedactMode("ENABLED", ParseRedactMode("ENABLED")))

	// Test valid MARKER modes
	require.True(t, IsValidRedactMode("MARKER", ParseRedactMode("MARKER")))
	require.True(t, IsValidRedactMode("marker", ParseRedactMode("marker")))
	require.True(t, IsValidRedactMode("MARK", ParseRedactMode("MARK")))
	require.True(t, IsValidRedactMode("mark", ParseRedactMode("mark")))

	// Test valid OFF modes
	require.True(t, IsValidRedactMode("OFF", ParseRedactMode("OFF")))
	require.True(t, IsValidRedactMode("off", ParseRedactMode("off")))
	require.True(t, IsValidRedactMode("FALSE", ParseRedactMode("FALSE")))
	require.True(t, IsValidRedactMode("false", ParseRedactMode("false")))
	require.True(t, IsValidRedactMode("0", ParseRedactMode("0")))
	require.True(t, IsValidRedactMode("DISABLE", ParseRedactMode("DISABLE")))
	require.True(t, IsValidRedactMode("DISABLED", ParseRedactMode("DISABLED")))
	require.True(t, IsValidRedactMode("", ParseRedactMode("")))

	// Test with whitespace
	require.True(t, IsValidRedactMode("  ON  ", ParseRedactMode("  ON  ")))
	require.True(t, IsValidRedactMode("  MARKER  ", ParseRedactMode("  MARKER  ")))
	require.True(t, IsValidRedactMode("  OFF  ", ParseRedactMode("  OFF  ")))
}
