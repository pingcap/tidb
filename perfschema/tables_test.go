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

package perfschema_test

import (
	"testing"

	. "github.com/pingcap/check"
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

func (s *testSuite) TestPerfSchemaTables(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use performance_schema")
	tk.MustQuery("select * from global_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from session_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from setup_actors").Check(testkit.Rows())
	tk.MustQuery("select * from events_stages_history_long").Check(testkit.Rows())
}
