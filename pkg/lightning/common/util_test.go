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

package common_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirNotExist(t *testing.T) {
	require.True(t, common.IsDirExists("."))
	require.False(t, common.IsDirExists("not-exists"))
}

func TestGetJSON(t *testing.T) {
	type TestPayload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	request := TestPayload{
		Username: "lightning",
		Password: "lightning-ctl",
	}

	ctx := context.Background()
	// Mock success response
	handle := func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusOK)
		err := json.NewEncoder(res).Encode(request)
		require.NoError(t, err)
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		handle(res, req)
	}))
	defer testServer.Close()

	client := &http.Client{Timeout: time.Second}

	response := TestPayload{}
	err := common.GetJSON(ctx, client, "http://localhost:1", &response)
	require.Error(t, err)
	err = common.GetJSON(ctx, client, testServer.URL, &response)
	require.NoError(t, err)
	require.Equal(t, request, response)

	// Mock `StatusNoContent` response
	handle = func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusNoContent)
	}
	err = common.GetJSON(ctx, client, testServer.URL, &response)
	require.Error(t, err)
	require.Regexp(t, ".*http status code != 200.*", err.Error())
}

func TestConnect(t *testing.T) {
	plainPsw := "dQAUoDiyb1ucWZk7"

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/lightning/common/MustMySQLPassword",
		fmt.Sprintf("return(\"%s\")", plainPsw)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/common/MustMySQLPassword"))
	}()

	param := common.MySQLConnectParam{
		Host:             "127.0.0.1",
		Port:             4000,
		User:             "root",
		Password:         plainPsw,
		SQLMode:          "strict",
		MaxAllowedPacket: 1234,
	}
	_, err := param.Connect()
	require.NoError(t, err)
	param.Password = base64.StdEncoding.EncodeToString([]byte(plainPsw))
	_, err = param.Connect()
	require.NoError(t, err)
}

func TestIsContextCanceledError(t *testing.T) {
	require.True(t, common.IsContextCanceledError(context.Canceled))
	require.False(t, common.IsContextCanceledError(io.EOF))
}

func TestUniqueTable(t *testing.T) {
	tableName := common.UniqueTable("test", "t1")
	require.Equal(t, "`test`.`t1`", tableName)

	tableName = common.UniqueTable("test", "t`1")
	require.Equal(t, "`test`.`t``1`", tableName)
}

func TestSQLWithRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlWithRetry := &common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}
	aValue := new(int)

	// retry defaultMaxRetry times and still failed
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("select a from test.t1").WillReturnError(errors.Annotate(mysql.ErrInvalidConn, "mock error"))
	}
	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	require.Regexp(t, ".*mock error", err.Error())

	// meet unretryable error and will return directly
	mock.ExpectQuery("select a from test.t1").WillReturnError(context.Canceled)
	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	require.Regexp(t, ".*context canceled", err.Error())

	// query success
	rows := sqlmock.NewRows([]string{"a"}).AddRow("1")
	mock.ExpectQuery("select a from test.t1").WillReturnRows(rows)

	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	require.NoError(t, err)
	require.Equal(t, 1, *aValue)

	// test Exec
	mock.ExpectExec("delete from").WillReturnError(context.Canceled)
	err = sqlWithRetry.Exec(context.Background(), "", "delete from test.t1 where id = ?", 2)
	require.Regexp(t, ".*context canceled", err.Error())

	mock.ExpectExec("delete from").WillReturnResult(sqlmock.NewResult(0, 1))
	err = sqlWithRetry.Exec(context.Background(), "", "delete from test.t1 where id = ?", 2)
	require.NoError(t, err)

	require.Nil(t, mock.ExpectationsWereMet())
}

func TestInterpolateMySQLString(t *testing.T) {
	assert.Equal(t, "'123'", common.InterpolateMySQLString("123"))
	assert.Equal(t, "'1''23'", common.InterpolateMySQLString("1'23"))
	assert.Equal(t, "'1''2''''3'", common.InterpolateMySQLString("1'2''3"))
}

func TestGetAutoRandomColumn(t *testing.T) {
	tests := []struct {
		ddl     string
		colName string
	}{
		{"create table t(c int)", ""},
		{"create table t(c int auto_increment)", ""},
		{"create table t(c bigint auto_random primary key)", "c"},
		{"create table t(a int, c bigint auto_random primary key)", "c"},
		{"create table t(c bigint auto_random, a int, primary key(c,a))", "c"},
		{"create table t(a int, c bigint auto_random, primary key(c,a))", "c"},
	}
	p := parser.New()
	for _, tt := range tests {
		tableInfo, err := dbutiltest.GetTableInfoBySQL(tt.ddl, p)
		require.NoError(t, err)
		col := common.GetAutoRandomColumn(tableInfo)
		if tt.colName == "" {
			require.Nil(t, col, tt.ddl)
		} else {
			require.Equal(t, tt.colName, col.Name.L, tt.ddl)
		}
	}
}

func TestBuildAddIndexSQL(t *testing.T) {
	tests := []struct {
		table     string
		current   string
		desired   string
		singleSQL string
		multiSQLs []string
	}{
		{
			table: "`test`.`non_pk_auto_inc`",
			current: `CREATE TABLE non_pk_auto_inc (
				pk varchar(255),
				id int(11) NOT NULL AUTO_INCREMENT,
				UNIQUE KEY uniq_id (id)
			)`,
			desired: `CREATE TABLE non_pk_auto_inc (
				pk varchar(255) PRIMARY KEY NONCLUSTERED,
				id int(11) NOT NULL AUTO_INCREMENT,
				UNIQUE KEY uniq_id (id)
			)`,
			singleSQL: "ALTER TABLE `test`.`non_pk_auto_inc` ADD PRIMARY KEY (`pk`)",
			multiSQLs: []string{"ALTER TABLE `test`.`non_pk_auto_inc` ADD PRIMARY KEY (`pk`)"},
		},
		{
			table: "`test`.`multi_indexes`",
			current: `
CREATE TABLE multi_indexes (
    c1 bigint PRIMARY KEY CLUSTERED,
    c2 varchar(255) NOT NULL,
    c3 varchar(255) NOT NULL,
    c4 varchar(255) NOT NULL,
    c5 varchar(255) NOT NULL,
    c6 varchar(255) NOT NULL,
    c7 varchar(255) NOT NULL,
    c8 varchar(255) NOT NULL,
    c9 varchar(255) NOT NULL,
    c10 varchar(255) NOT NULL,
    c11 varchar(255) NOT NULL
)
`,
			desired: `
CREATE TABLE multi_indexes (
    c1 bigint PRIMARY KEY CLUSTERED,
    c2 varchar(255) NOT NULL UNIQUE KEY,
    c3 varchar(255) NOT NULL,
    c4 varchar(255) NOT NULL,
    c5 varchar(255) NOT NULL,
    c6 varchar(255) NOT NULL,
    c7 varchar(255) NOT NULL,
    c8 varchar(255) NOT NULL,
    c9 varchar(255) NOT NULL,
    c10 varchar(255) NOT NULL,
    c11 varchar(255) NOT NULL,
    INDEX idx_c2 (c2) COMMENT 'single column index',
    INDEX idx_c2_c3(c2, c3) COMMENT 'multiple column index',
    UNIQUE KEY uniq_c4 (c4) COMMENT 'single column unique key',
    UNIQUE KEY uniq_c4_c5 (c4, c5) COMMENT 'multiple column unique key',
    INDEX idx_c6 (c6 ASC)  COMMENT 'single column index with asc order',
    INDEX idx_c7 (c7 DESC) COMMENT 'single column index with desc order',
    INDEX idx_c6_c7 (c6 ASC, c7 DESC) COMMENT 'multiple column index with asc and desc order',
    INDEX idx_c8 (c8) VISIBLE COMMENT 'single column index with visible',
    INDEX idx_c9 (c9) INVISIBLE COMMENT 'single column index with invisible',
    INDEX idx_lower_c10 ((lower(c10))) COMMENT 'single column index with function',
    INDEX idx_prefix_c11 (c11(3)) COMMENT 'single column index with prefix'
);`,
			singleSQL: "ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c2`(`c2`) COMMENT 'single column index'" +
				", ADD KEY `idx_c2_c3`(`c2`,`c3`) COMMENT 'multiple column index'" +
				", ADD UNIQUE KEY `uniq_c4`(`c4`) COMMENT 'single column unique key'" +
				", ADD UNIQUE KEY `uniq_c4_c5`(`c4`,`c5`) COMMENT 'multiple column unique key'" +
				", ADD KEY `idx_c6`(`c6`) COMMENT 'single column index with asc order'" +
				", ADD KEY `idx_c7`(`c7`) COMMENT 'single column index with desc order'" +
				", ADD KEY `idx_c6_c7`(`c6`,`c7`) COMMENT 'multiple column index with asc and desc order'" +
				", ADD KEY `idx_c8`(`c8`) COMMENT 'single column index with visible'" +
				", ADD KEY `idx_c9`(`c9`) INVISIBLE COMMENT 'single column index with invisible'" +
				", ADD KEY `idx_lower_c10`((lower(`c10`))) COMMENT 'single column index with function'" +
				", ADD KEY `idx_prefix_c11`(`c11`(3)) COMMENT 'single column index with prefix'" +
				", ADD UNIQUE KEY `c2`(`c2`)",
			multiSQLs: []string{
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c2`(`c2`) COMMENT 'single column index'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c2_c3`(`c2`,`c3`) COMMENT 'multiple column index'",
				"ALTER TABLE `test`.`multi_indexes` ADD UNIQUE KEY `uniq_c4`(`c4`) COMMENT 'single column unique key'",
				"ALTER TABLE `test`.`multi_indexes` ADD UNIQUE KEY `uniq_c4_c5`(`c4`,`c5`) COMMENT 'multiple column unique key'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c6`(`c6`) COMMENT 'single column index with asc order'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c7`(`c7`) COMMENT 'single column index with desc order'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c6_c7`(`c6`,`c7`)" +
					" COMMENT 'multiple column index with asc and desc order'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c8`(`c8`) COMMENT 'single column index with visible'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_c9`(`c9`) INVISIBLE COMMENT 'single column index with invisible'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_lower_c10`((lower(`c10`)))" +
					" COMMENT 'single column index with function'",
				"ALTER TABLE `test`.`multi_indexes` ADD KEY `idx_prefix_c11`(`c11`(3)) COMMENT 'single column index with prefix'",
				"ALTER TABLE `test`.`multi_indexes` ADD UNIQUE KEY `c2`(`c2`)",
			},
		}}

	p := parser.New()

	for _, tt := range tests {
		curTblInfo, err := dbutiltest.GetTableInfoBySQL(tt.current, p)
		require.NoError(t, err)
		desiredTblInfo, err := dbutiltest.GetTableInfoBySQL(tt.desired, p)
		require.NoError(t, err)

		singleSQL, multiSQLs := common.BuildAddIndexSQL(tt.table, curTblInfo, desiredTblInfo)
		require.Equal(t, tt.singleSQL, singleSQL)
		require.Equal(t, tt.multiSQLs, multiSQLs)
	}
}
