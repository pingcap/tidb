// Copyright 2021 PingCAP, Inc.
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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLMode(t *testing.T) {
	// ref https://dev.mysql.com/doc/internals/en/query-event.html#q-sql-mode-code,
	hardCode := []struct {
		code  SQLMode
		value int
	}{{
		ModeRealAsFloat, 0x00000001,
	}, {
		ModePipesAsConcat, 0x00000002,
	}, {
		ModeANSIQuotes, 0x00000004,
	}, {
		ModeIgnoreSpace, 0x00000008,
	}, {
		ModeNotUsed, 0x00000010,
	}, {
		ModeOnlyFullGroupBy, 0x00000020,
	}, {
		ModeNoUnsignedSubtraction, 0x00000040,
	}, {
		ModeNoDirInCreate, 0x00000080,
	}, {
		ModePostgreSQL, 0x00000100,
	}, {
		ModeOracle, 0x00000200,
	}, {
		ModeMsSQL, 0x00000400,
	}, {
		ModeDb2, 0x00000800,
	}, {
		ModeMaxdb, 0x00001000,
	}, {
		ModeNoKeyOptions, 0x00002000,
	}, {
		ModeNoTableOptions, 0x00004000,
	}, {
		ModeNoFieldOptions, 0x00008000,
	}, {
		ModeMySQL323, 0x00010000,
	}, {
		ModeMySQL40, 0x00020000,
	}, {
		ModeANSI, 0x00040000,
	}, {
		ModeNoAutoValueOnZero, 0x00080000,
	}, {
		ModeNoBackslashEscapes, 0x00100000,
	}, {
		ModeStrictTransTables, 0x00200000,
	}, {
		ModeStrictAllTables, 0x00400000,
	}, {
		ModeNoZeroInDate, 0x00800000,
	}, {
		ModeNoZeroDate, 0x01000000,
	}, {
		ModeInvalidDates, 0x02000000,
	}, {
		ModeErrorForDivisionByZero, 0x04000000,
	}, {
		ModeTraditional, 0x08000000,
	}, {
		ModeNoAutoCreateUser, 0x10000000,
	}, {
		ModeHighNotPrecedence, 0x20000000,
	}, {
		ModeNoEngineSubstitution, 0x40000000,
	}, {
		ModePadCharToFullLength, 0x80000000,
	}}

	for _, ca := range hardCode {
		require.Equal(t, ca.value, int(ca.code))
	}
}
