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
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tmysql "github.com/pingcap/tidb/errno"
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
	err := common.GetJSON(ctx, client, "http://not-exists", &response)
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

func TestToDSN(t *testing.T) {
	param := common.MySQLConnectParam{
		Host:             "127.0.0.1",
		Port:             4000,
		User:             "root",
		Password:         "123456",
		SQLMode:          "strict",
		MaxAllowedPacket: 1234,
		TLS:              "cluster",
		Vars: map[string]string{
			"tidb_distsql_scan_concurrency": "1",
		},
	}
	require.Equal(t, "root:123456@tcp(127.0.0.1:4000)/?charset=utf8mb4&sql_mode='strict'&maxAllowedPacket=1234&tls=cluster&tidb_distsql_scan_concurrency='1'", param.ToDSN())

	param.Host = "::1"
	require.Equal(t, "root:123456@tcp([::1]:4000)/?charset=utf8mb4&sql_mode='strict'&maxAllowedPacket=1234&tls=cluster&tidb_distsql_scan_concurrency='1'", param.ToDSN())
}

type mockDriver struct {
	driver.Driver
	plainPsw string
}

func (m *mockDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	accessDenied := cfg.Passwd != m.plainPsw
	return &mockConn{accessDenied: accessDenied}, nil
}

type mockConn struct {
	driver.Conn
	driver.Pinger
	accessDenied bool
}

func (c *mockConn) Ping(ctx context.Context) error {
	if c.accessDenied {
		return &mysql.MySQLError{Number: tmysql.ErrAccessDenied, Message: "access denied"}
	}
	return nil
}

func (c *mockConn) Close() error {
	return nil
}

func TestConnect(t *testing.T) {
	plainPsw := "dQAUoDiyb1ucWZk7"
	driverName := "mysql-mock-" + strconv.Itoa(rand.Int())
	sql.Register(driverName, &mockDriver{plainPsw: plainPsw})

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/br/pkg/lightning/common/MockMySQLDriver",
		fmt.Sprintf("return(\"%s\")", driverName)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/common/MockMySQLDriver"))
	}()

	param := common.MySQLConnectParam{
		Host:             "127.0.0.1",
		Port:             4000,
		User:             "root",
		Password:         plainPsw,
		SQLMode:          "strict",
		MaxAllowedPacket: 1234,
	}
	db, err := param.Connect()
	require.NoError(t, err)
	require.NoError(t, db.Close())
	param.Password = base64.StdEncoding.EncodeToString([]byte(plainPsw))
	db, err = param.Connect()
	require.NoError(t, err)
	require.NoError(t, db.Close())
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

func TestStringSliceEqual(t *testing.T) {
	assert.True(t, common.StringSliceEqual(nil, nil))
	assert.True(t, common.StringSliceEqual(nil, []string{}))
	assert.False(t, common.StringSliceEqual(nil, []string{"a"}))
	assert.False(t, common.StringSliceEqual([]string{"a"}, nil))
	assert.True(t, common.StringSliceEqual([]string{"a"}, []string{"a"}))
	assert.False(t, common.StringSliceEqual([]string{"a"}, []string{"b"}))
	assert.True(t, common.StringSliceEqual([]string{"a", "b", "c"}, []string{"a", "b", "c"}))
	assert.False(t, common.StringSliceEqual([]string{"a"}, []string{"a", "b", "c"}))
	assert.False(t, common.StringSliceEqual([]string{"a", "b", "c"}, []string{"a", "b"}))
	assert.False(t, common.StringSliceEqual([]string{"a", "x", "y"}, []string{"a", "y", "x"}))
}

func TestInterpolateMySQLString(t *testing.T) {
	assert.Equal(t, "'123'", common.InterpolateMySQLString("123"))
	assert.Equal(t, "'1''23'", common.InterpolateMySQLString("1'23"))
	assert.Equal(t, "'1''2''''3'", common.InterpolateMySQLString("1'2''3"))
}
