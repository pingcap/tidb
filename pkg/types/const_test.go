// Copyright 2017 PingCAP, Inc.
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

package types_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestGetSQLMode(t *testing.T) {
	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, test := range positiveCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(test.arg))
		require.NoError(t, err)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, test := range negativeCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(test.arg))
		require.Error(t, err)
	}
}

func TestSQLMode(t *testing.T) {
	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, test := range tests {
		sqlMode, _ := mysql.GetSQLMode(test.arg)
		require.Equal(t, test.hasNoZeroDateMode, sqlMode.HasNoZeroDateMode())
		require.Equal(t, test.hasNoZeroInDateMode, sqlMode.HasNoZeroInDateMode())
		require.Equal(t, test.hasErrorForDivisionByZeroMode, sqlMode.HasErrorForDivisionByZeroMode())
	}
}

func TestServerStatus(t *testing.T) {
	tests := []struct {
		arg            uint16
		IsCursorExists bool
	}{
		{0, false},
		{mysql.ServerStatusInTrans | mysql.ServerStatusNoBackslashEscaped, false},
		{mysql.ServerStatusCursorExists, true},
		{mysql.ServerStatusCursorExists | mysql.ServerStatusLastRowSend, true},
	}

	for _, test := range tests {
		require.Equal(t, test.IsCursorExists, mysql.HasCursorExistsFlag(test.arg))
	}
}
