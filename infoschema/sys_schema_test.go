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

package infoschema

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

const testTableName = "test"
var testTableCols = []columnInfo{
	{"ID", mysql.TypeLong, 20, 0, nil, nil},
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSysTablesSuite{})

type testSysTablesSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testSysTablesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	// Create a table for test before creating domain.
	createTestTable()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSysTablesSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	_ = s.store.Close()
}

func (s *testSysTablesSuite) TestSysSchemaTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// Test existence of sys schema.
	tk.MustExec("use sys")

	// Test querying tables.
	sql := fmt.Sprintf("select * from %s", testTableName)
	tk.MustQuery(sql).Check(testkit.Rows())
}

func createTestTable() {
	dbID := autoid.SysSchemaDBID
	tableInfo := buildTableMeta(testTableName, testTableCols)
	tableInfo.ID = dbID + 1
	for i, c := range tableInfo.Columns {
		c.ID = int64(i) + 1
	}
	dbInfo := &model.DBInfo{
		ID:      dbID,
		Name:    util.SysSchemaName,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  []*model.TableInfo{tableInfo},
	}
	RegisterVirtualTable(dbInfo, sysTableFromMeta)
}
