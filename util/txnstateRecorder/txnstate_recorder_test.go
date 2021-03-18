// Copyright 2020 PingCAP, Inc.
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

// Package txnstateRecorder is for recording the transaction running state on current tidb instance
// so we can display them in `information_schema.TIDB_TRX`
package txnstateRecorder_test

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
)

type testTxnStateRecorderSuite struct {
	store kv.Storage
}

var _ = Suite(&testTxnStateRecorderSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testTxnStateRecorderSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = mockstore.NewMockStore()
	if err != nil {
		panic(err)
	}
	_, err = session.BootstrapSession(s.store)
	if err != nil {
		panic(err)
	}
}

func (s *testTxnStateRecorderSuite) TearDownSuite(c *C) {
	_ = s.store.Close()
}

func (s *testTxnStateRecorderSuite) TestBasicRecordingTxnState(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("create table t(id int, a varchar(128));")
	tk.MustExec("insert into t(id, a) values (1, 'abcd');")
	rows := tk.MustQuery("select * from information_schema.TIDB_TRX;").Rows()
	c.Assert(len(rows), Equals, 0)
	tk.MustExec("BEGIN;")
	tk.MustExec("select * from t for update;")
	rows = tk.MustQuery("select * from information_schema.TIDB_TRX;").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("COMMIT;")
	rows = tk.MustQuery("select * from information_schema.TIDB_TRX;").Rows()
	c.Assert(len(rows), Equals, 0)
}
