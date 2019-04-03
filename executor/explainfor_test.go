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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager1 struct {
	PS []util.ProcessInfo
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager1) ShowProcessList() map[uint64]util.ProcessInfo {
	ret := make(map[uint64]util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

// GetProcessInfo implements the SessionManager.GetProcessInfo interface.
func (msm *mockSessionManager1) GetProcessInfo(id uint64) (pi util.ProcessInfo, ok bool) {
	for _, item := range msm.PS {
		if id == item.ID {
			return item, true
		}
	}
	return
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager1) Kill(cid uint64, query bool) {

}

func (s *testSuite) TestExplainFor(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.store)
	tkUser := testkit.NewTestKitWithInit(c, s.store)
	tkRoot.MustExec("create table t1(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tkUser.Se.Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustQuery("select * from t1;")
	tkRootProcess := tkRoot.Se.ShowProcess()
	ps := []util.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkUser.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Check(testkit.Rows(
		"TableReader_5 10000.00 root data:TableScan_4",
		"└─TableScan_4 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	c.Check(err, NotNil)
}
