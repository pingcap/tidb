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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutil

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	pmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestReplacePlaceholder(t *testing.T) {
	testCases := []struct {
		originStr string
		args      []string
		expectStr string
	}{
		{
			"a > ? AND a < ?",
			[]string{"1", "2"},
			"a > '1' AND a < '2'",
		}, {
			"a = ? AND b = ?",
			[]string{"1", "2"},
			"a = '1' AND b = '2'",
		},
	}

	for _, testCase := range testCases {
		str := ReplacePlaceholder(testCase.originStr, testCase.args)
		require.Equal(t, testCase.expectStr, str)
	}

}

func TestTableName(t *testing.T) {
	testCases := []struct {
		schema          string
		table           string
		expectTableName string
	}{
		{
			"test",
			"testa",
			"`test`.`testa`",
		},
		{
			"test-1",
			"test-a",
			"`test-1`.`test-a`",
		},
		{
			"test",
			"t`esta",
			"`test`.`t``esta`",
		},
	}

	for _, testCase := range testCases {
		tableName := TableName(testCase.schema, testCase.table)
		require.Equal(t, testCase.expectTableName, tableName)
	}
}

func TestColumnName(t *testing.T) {
	testCases := []struct {
		column        string
		expectColName string
	}{
		{
			"test",
			"`test`",
		},
		{
			"test-1",
			"`test-1`",
		},
		{
			"t`esta",
			"`t``esta`",
		},
	}

	for _, testCase := range testCases {
		colName := ColumnName(testCase.column)
		require.Equal(t, testCase.expectColName, colName)
	}
}

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func TestIsIgnoreError(t *testing.T) {
	cases := []struct {
		err       error
		canIgnore bool
	}{
		{newMysqlErr(uint16(infoschema.ErrDatabaseExists.Code()), "Can't create database, database exists"), true},
		{newMysqlErr(uint16(infoschema.ErrDatabaseDropExists.Code()), "Can't drop database, database doesn't exists"), true},
		{newMysqlErr(uint16(infoschema.ErrTableExists.Code()), "Can't create table, table exists"), true},
		{newMysqlErr(uint16(infoschema.ErrTableDropExists.Code()), "Can't drop table, table dosen't exists"), true},
		{newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "Duplicate column name"), true},
		{newMysqlErr(uint16(infoschema.ErrIndexExists.Code()), "Duplicate Index"), true},

		{newMysqlErr(uint16(999), "fake error"), false},
		{errors.New("unknown error"), false},
	}

	for _, tt := range cases {
		t.Logf("err %v, expected %v", tt.err, tt.canIgnore)
		require.Equal(t, tt.canIgnore, ignoreError(tt.err))
	}
}

func TestDeleteRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// delete twice
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, DefaultDeleteRowsNum))
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, DefaultDeleteRowsNum-1))

	err = DeleteRows(context.Background(), db, "test", "t", "", nil)
	require.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetParser(t *testing.T) {
	testCases := []struct {
		sqlModeStr string
		hasErr     bool
	}{
		{
			"",
			false,
		}, {
			"ANSI_QUOTES",
			false,
		}, {
			"ANSI_QUOTES,IGNORE_SPACE",
			false,
		}, {
			"ANSI_QUOTES123",
			true,
		}, {
			"ANSI_QUOTES,IGNORE_SPACE123",
			true,
		},
	}

	for _, testCase := range testCases {
		parser, err := getParser(testCase.sqlModeStr)
		if testCase.hasErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.NotNil(t, parser)
		}
	}
}

func TestAnalyzeValuesFromBuckets(t *testing.T) {
	cases := []struct {
		value  string
		col    *model.ColumnInfo
		expect string
	}{
		{
			"2021-03-05 21:31:03",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeDatetime).Build()},
			"2021-03-05 21:31:03",
		},
		{
			"2021-03-05 21:31:03",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeTimestamp).Build()},
			"2021-03-05 21:31:03",
		},
		{
			"2021-03-05",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeDate).Build()},
			"2021-03-05",
		},
		{
			"1847956477067657216",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeDatetime).Build()},
			"2020-01-01 10:00:00",
		},
		{
			"1847955927311843328",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeTimestamp).Build()},
			"2020-01-01 02:00:00",
		},
		{
			"1847955789872889856",
			&model.ColumnInfo{FieldType: types.NewFieldTypeBuilder().SetType(pmysql.TypeDate).Build()},
			"2020-01-01 00:00:00",
		},
	}
	for _, ca := range cases {
		val, err := AnalyzeValuesFromBuckets(ca.value, []*model.ColumnInfo{ca.col})
		require.NoError(t, err)
		require.Len(t, val, 1)
		require.Equal(t, ca.expect, val[0])
	}
}

func TestFormatTimeZoneOffset(t *testing.T) {
	cases := map[string]time.Duration{
		"+00:00": 0,
		"+01:00": time.Hour,
		"-08:03": -1 * (8*time.Hour + 3*time.Minute),
		"-12:59": -1 * (12*time.Hour + 59*time.Minute),
		"+12:59": 12*time.Hour + 59*time.Minute,
	}

	for k, v := range cases {
		offset := FormatTimeZoneOffset(v)
		require.Equal(t, offset, k)
	}
}

func TestGetTimeZoneOffset(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(mock.NewRows([]string{""}).AddRow("01:00:00"))
	d, err := GetTimeZoneOffset(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, "1h0m0s", d.String())
}
