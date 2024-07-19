// Copyright 2024 PingCAP, Inc.
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

package fastcreatetable

import (
	"testing"

	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestSwitchFastCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table OFF"))

	tk.MustExec("create database db1;")
	tk.MustExec("create database db2;")
	tk.MustExec("create table db1.tb1(id int);")
	tk.MustExec("create table db1.tb2(id int);")
	tk.MustExec("create table db2.tb1(id int);")

	tk.MustExec("set global tidb_enable_fast_create_table=ON")
	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table ON"))

	tk.MustExec("set global tidb_enable_fast_create_table=0")
	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table OFF"))

	tk.MustGetErrMsg("set global tidb_enable_fast_create_table='wrong'", "[variable:1231]Variable 'tidb_enable_fast_create_table' can't be set to the value of 'wrong'")
}

func TestDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustExec("set global tidb_enable_fast_create_table=ON")

	tk.MustExec("create database db")
	// Create Table
	tk.MustExec("create table db.tb1(id int)")
	tk.MustExec("create table db.tb2(id int)")
	// create table twice
	tk.MustGetErrMsg("create table db.tb1(id int)", "[schema:1050]Table 'db.tb1' already exists")

	// Truncate Table
	tk.MustExec("truncate table db.tb1")

	// Drop Table
	tk.MustExec("drop table db.tb1")

	// Rename Table
	tk.MustExec("rename table db.tb2 to db.tb3")

	// Drop Database
	tk.MustExec("drop database db")

	// create again
	tk.MustExec("create database db")
	// Create Table
	tk.MustExec("create table db.tb1(id int)")
	tk.MustExec("create table db.tb2(id int)")
}
