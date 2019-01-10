// Copyright 2018 PingCAP, Inc.
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

package tiniub_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/infoschema/tiniub"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) TestTiNiuBTables(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	tiniub.Init()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use PERFORMANCE_SCHEMA")
	tk.MustQuery("select * from slow_query").Check(testkit.Rows())
}
