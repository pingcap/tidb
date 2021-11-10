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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
)

type utilSuite struct{}

var _ = Suite(&utilSuite{})

func (s *utilSuite) TestDirNotExist(c *C) {
	c.Assert(common.IsDirExists("."), IsTrue)
	c.Assert(common.IsDirExists("not-exists"), IsFalse)
}

func (s *utilSuite) TestGetJSON(c *C) {
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
		c.Assert(err, IsNil)
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		handle(res, req)
	}))
	defer testServer.Close()

	client := &http.Client{Timeout: time.Second}

	response := TestPayload{}
	err := common.GetJSON(ctx, client, "http://not-exists", &response)
	c.Assert(err, NotNil)
	err = common.GetJSON(ctx, client, testServer.URL, &response)
	c.Assert(err, IsNil)
	c.Assert(request, DeepEquals, response)

	// Mock `StatusNoContent` response
	handle = func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusNoContent)
	}
	err = common.GetJSON(ctx, client, testServer.URL, &response)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*http status code != 200.*")
}

func (s *utilSuite) TestToDSN(c *C) {
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
	c.Assert(param.ToDSN(), Equals, "root:123456@tcp(127.0.0.1:4000)/?charset=utf8mb4&sql_mode='strict'&maxAllowedPacket=1234&tls=cluster&tidb_distsql_scan_concurrency=1")
}

func (s *utilSuite) TestIsContextCanceledError(c *C) {
	c.Assert(common.IsContextCanceledError(context.Canceled), IsTrue)
	c.Assert(common.IsContextCanceledError(io.EOF), IsFalse)
}

func (s *utilSuite) TestUniqueTable(c *C) {
	tableName := common.UniqueTable("test", "t1")
	c.Assert(tableName, Equals, "`test`.`t1`")

	tableName = common.UniqueTable("test", "t`1")
	c.Assert(tableName, Equals, "`test`.`t``1`")
}

func (s *utilSuite) TestSQLWithRetry(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	sqlWithRetry := &common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}
	aValue := new(int)

	// retry defaultMaxRetry times and still failed
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("select a from test.t1").WillReturnError(errors.New("mock error"))
	}
	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	c.Assert(err, ErrorMatches, ".*mock error")

	// meet unretryable error and will return directly
	mock.ExpectQuery("select a from test.t1").WillReturnError(context.Canceled)
	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	c.Assert(err, ErrorMatches, ".*context canceled")

	// query success
	rows := sqlmock.NewRows([]string{"a"}).AddRow("1")
	mock.ExpectQuery("select a from test.t1").WillReturnRows(rows)

	err = sqlWithRetry.QueryRow(context.Background(), "", "select a from test.t1", aValue)
	c.Assert(err, IsNil)
	c.Assert(*aValue, Equals, 1)

	// test Exec
	mock.ExpectExec("delete from").WillReturnError(context.Canceled)
	err = sqlWithRetry.Exec(context.Background(), "", "delete from test.t1 where id = ?", 2)
	c.Assert(err, ErrorMatches, ".*context canceled")

	mock.ExpectExec("delete from").WillReturnResult(sqlmock.NewResult(0, 1))
	err = sqlWithRetry.Exec(context.Background(), "", "delete from test.t1 where id = ?", 2)
	c.Assert(err, IsNil)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *utilSuite) TestStringSliceEqual(c *C) {
	c.Assert(common.StringSliceEqual(nil, nil), IsTrue)
	c.Assert(common.StringSliceEqual(nil, []string{}), IsTrue)
	c.Assert(common.StringSliceEqual(nil, []string{"a"}), IsFalse)
	c.Assert(common.StringSliceEqual([]string{"a"}, nil), IsFalse)
	c.Assert(common.StringSliceEqual([]string{"a"}, []string{"a"}), IsTrue)
	c.Assert(common.StringSliceEqual([]string{"a"}, []string{"b"}), IsFalse)
	c.Assert(common.StringSliceEqual([]string{"a", "b", "c"}, []string{"a", "b", "c"}), IsTrue)
	c.Assert(common.StringSliceEqual([]string{"a"}, []string{"a", "b", "c"}), IsFalse)
	c.Assert(common.StringSliceEqual([]string{"a", "b", "c"}, []string{"a", "b"}), IsFalse)
	c.Assert(common.StringSliceEqual([]string{"a", "x", "y"}, []string{"a", "y", "x"}), IsFalse)
}

func (s *utilSuite) TestInterpolateMySQLString(c *C) {
	c.Assert(common.InterpolateMySQLString("123"), Equals, "'123'")
	c.Assert(common.InterpolateMySQLString("1'23"), Equals, "'1''23'")
	c.Assert(common.InterpolateMySQLString("1'2''3"), Equals, "'1''2''''3'")
}
