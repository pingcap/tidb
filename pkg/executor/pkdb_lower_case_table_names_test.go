// Copyright 2025 PingCAP, Inc.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestLowerCaseTableNames(t *testing.T) {
	model.SetLowerCaseTableNamesOnBootstrap(0)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.LowerCaseTableNamesOnFirstBootstrap = 0
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table tt (a int);")
	tk.MustExec("create table TT (b int);")
	tk.MustQuery("show tables;").Check(testkit.Rows("TT", "tt"))
	tk.MustExec("insert into tt values (1111);")
	tk.MustExec("insert into TT values (2222);")
	tk.MustQuery("select * from tt").Check(testkit.Rows("1111"))
	tk.MustQuery("select * from TT").Check(testkit.Rows("2222"))
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("alter table tt modify column a tinyint;")
	tk.MustQuery("select * from tt").Check(testkit.Rows("127"))

	tk.MustExec("create database dD;")
	tk.MustExec("create database Dd;")
	tk.MustExec("create table dD.t (id varchar(255), primary key(id));")
	tk.MustExec("create table Dd.t (id varchar(255), primary key(id));")
	tk.MustExec("insert into dD.t values ('dD');")
	tk.MustExec("insert into Dd.t values ('Dd');")
	tk.MustQuery("select * from dD.t").Check(testkit.Rows("dD"))
	tk.MustQuery("select * from Dd.t").Check(testkit.Rows("Dd"))

	tk.MustExec("use dD;")
	tk.MustQuery("select database();").Check(testkit.Rows("dD"))
	tk.MustExec("use Dd;")
	tk.MustQuery("select database();").Check(testkit.Rows("Dd"))
}
