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

package infoschema_test

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

type inspectionSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

var _ = SerialSuites(&inspectionSuite{})

func (s *inspectionSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *inspectionSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *inspectionSuite) TestInspectionTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	instances := []string{
		"pd,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"tidb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
	}
	fpName := "github.com/pingcap/tidb/infoschema/mockClusterInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))

	// enable inspection mode
	inspectionTableCache := map[string]variable.TableSnapshot{}
	tk.Se.GetSessionVars().InspectionTableCache = inspectionTableCache
	tk.MustQuery("select type, instance, status_address, version, git_hash from inspection_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))
	c.Assert(inspectionTableCache["cluster_info"].Err, IsNil)
	c.Assert(len(inspectionTableCache["cluster_info"].Rows), DeepEquals, 3)

	// should invisible to other sessions
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	err := tk2.QueryToErr("select * from inspection_schema.cluster_info")
	c.Assert(err, ErrorMatches, "not currently in inspection mode")

	// check whether is obtain data from cache at the next time
	inspectionTableCache["cluster_info"].Rows[0][0].SetString("modified-pd", mysql.DefaultCollationName)
	tk.MustQuery("select type, instance, status_address, version, git_hash from inspection_schema.cluster_info").Check(testkit.Rows(
		"modified-pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))
	tk.Se.GetSessionVars().InspectionTableCache = nil

	// disable inspection mode
	err = tk.QueryToErr("select * from inspection_schema.cluster_info")
	c.Assert(err, ErrorMatches, "not currently in inspection mode")
}
