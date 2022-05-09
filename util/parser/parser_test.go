// Copyright 2019 PingCAP, Inc.
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

package parser_test

import (
	"testing"

	utilparser "github.com/pingcap/tidb/util/parser"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	okTable := []struct {
		Times    int
		Input    string
		Expected string
	}{
		{0, " 1", "1"},
		{0, "1", "1"},
		{1, "     1", "1"},
		{2, "  1", "1"},
	}
	for _, test := range okTable {
		rest, err := utilparser.Space(test.Input, test.Times)
		require.NoError(t, err)
		require.Equal(t, test.Expected, rest)
	}

	errTable := []struct {
		Times int
		Input string
	}{
		{1, "1"},
		{2, " 1"},
	}

	for _, test := range errTable {
		rest, err := utilparser.Space(test.Input, test.Times)

		require.NotNil(t, err)
		require.Equal(t, test.Input, rest)
	}
}

func TestDigit(t *testing.T) {
	okTable := []struct {
		Times          int
		Input          string
		ExpectedDigits string
		ExpectedRest   string
	}{
		{0, "123abc", "123", "abc"},
		{1, "123abc", "123", "abc"},
		{2, "123 @)@)", "123", " @)@)"},
		{3, "456 121", "456", " 121"},
	}

	for _, test := range okTable {
		digits, rest, err := utilparser.Digit(test.Input, test.Times)

		require.NoError(t, err)
		require.Equal(t, test.ExpectedDigits, digits)
		require.Equal(t, test.ExpectedRest, rest)
	}

	errTable := []struct {
		Times int
		Input string
	}{
		{1, "int"},
		{2, "1int"},
		{3, "12 int"},
	}

	for _, test := range errTable {
		digits, rest, err := utilparser.Digit(test.Input, test.Times)

		require.NotNil(t, err)
		require.Equal(t, "", digits)
		require.Equal(t, test.Input, rest)
	}
}

func TestNumber(t *testing.T) {
	okTable := []struct {
		Input        string
		ExpectedNum  int
		ExpectedRest string
	}{
		{"123abc", 123, "abc"},
		{"123abc", 123, "abc"},
		{"123 @)@)", 123, " @)@)"},
		{"456 121", 456, " 121"},
	}
	for _, test := range okTable {
		digits, rest, err := utilparser.Number(test.Input)

		require.NoError(t, err)
		require.Equal(t, test.ExpectedNum, digits)
		require.Equal(t, test.ExpectedRest, rest)
	}

	errTable := []struct {
		Input string
	}{
		{"int"},
		{"abcint"},
		{"@)@)int"},
	}

	for _, test := range errTable {
		digits, rest, err := utilparser.Number(test.Input)

		require.NotNil(t, err)
		require.Equal(t, 0, digits)
		require.Equal(t, test.Input, rest)
	}
}

func TestCharAndAnyChar(t *testing.T) {
	okTable := []struct {
		Char     byte
		Input    string
		Expected string
	}{
		{'i', "int", "nt"},
		{'1', "1int", "int"},
		{'1', "12 int", "2 int"},
	}

	for _, test := range okTable {
		rest, err := utilparser.Char(test.Input, test.Char)

		require.NoError(t, err)
		require.Equal(t, test.Expected, rest)

		rest, err = utilparser.AnyChar(test.Input)

		require.NoError(t, err)
		require.Equal(t, test.Expected, rest)
	}

	errTable := []struct {
		Char  byte
		Input string
	}{
		{'i', "xint"},
		{'1', "x1int"},
		{'1', "x12 int"},
	}

	for _, test := range errTable {
		rest, err := utilparser.Char(test.Input, test.Char)

		require.NotNil(t, err)
		require.Equal(t, test.Input, rest)
	}
}
