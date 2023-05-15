// Copyright 2023 PingCAP, Inc.
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

package util_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestHasSysDB(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql")
	tk.MustExec("create table t_rename1(a int);")
	tk.MustExec("create table t_renames1(a int);")
	tk.MustExec(`CREATE TABLE t_p (
		id INT NOT NULL
	)
    PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (50),
        PARTITION p1 VALUES LESS THAN (100),
        PARTITION p2 VALUES LESS THAN (150),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
	);`)
	tk.MustExec("INSERT INTO t_p VALUES (1669),(337),(16),(2005)")
	tk.MustExec("CREATE TABLE t (id INT NOT NULL);")
	tk.MustExec("use test")
	tk.MustExec("create table t_rename2(a int);")
	tk.MustExec("create table t_renames11(a int);")
	tk.MustExec("create table t_renames2(a int);")
	tk.MustExec("create table t_renames22(a int);")
	tk.MustExec("create table t_p_1 like mysql.t_p")
	tk.MustExec("INSERT INTO t_p_1 VALUES (1669),(337),(16),(2005)")
	tk.MustExec("CREATE TABLE t1 (id INT NOT NULL);")

	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")

	ddlCases := []struct {
		ddl     string
		isSysDB bool
	}{
		{"create table mysql.sys_t(c1 bigint auto_increment primary key, c2 bigint);", true},
		{"create table tbl(c1 bigint auto_increment primary key, c2 bigint);", false},
		{"rename table mysql.t_rename1 to t_rename1", true},
		{"rename table t_rename2 to mysql.t_rename22", true},
		{"rename table mysql.t_renames1 to t_renames1, t_renames11 to t_renames110", true},
		{"rename table t_renames2 to t_renames20, t_renames22 to mysql.t_renames22", true},
		{"alter table mysql.t_p exchange partition p0 with table t1", true},
		{"alter table t_p_1 exchange partition p0 with table mysql.t", true},
	}

	hook := &callback.TestDDLCallback{Do: dom}
	cnt := 0
	var jobID int64
	var isJobFirstState bool
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		isJobFirstState = job.ID != jobID
		if isJobFirstState {
			hasSysDB, err := util.HasSysDB(job)
			require.NoError(t, err)
			require.Equal(t, ddlCases[cnt].isSysDB, hasSysDB, ddlCases[cnt].ddl)
			cnt++
		}
		jobID = job.ID
	}
	dom.DDL().SetHook(hook)

	for _, d := range ddlCases {
		tk.MustExec(d.ddl)
	}
	ret := tk.MustQuery(fmt.Sprintf("select * from mysql.tidb_ddl_history order by job_id desc limit %d", len(ddlCases)))
	for _, row := range ret.Rows() {
		jobBinary := []byte(row[1].(string))
		job := model.Job{}
		err := job.Decode(jobBinary)
		require.NoError(t, err)
	}
}
