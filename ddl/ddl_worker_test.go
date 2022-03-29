//// Copyright 2015 PingCAP, Inc.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
package ddl_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

const testLease = 5 * time.Millisecond

func TestCheckOwner(t *testing.T) {
	_, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

	time.Sleep(testLease)
	require.Equal(t, dom.DDL().OwnerManager().IsOwner(), true)
	require.Equal(t, dom.DDL().GetLease(), testLease)
}

func TestInvalidDDLJob(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	ctx := testNewContext(store)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().DoDDLJob(ctx, job)
	require.Equal(t, err.Error(), "[ddl:8204]invalid ddl job type: none")
}

func TestAddBatchJobError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	ctx := testNewContext(store)

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockAddBatchDDLJobsErr", `return(true)`))
	// Test the job runner should not hang forever.
	job := &model.Job{SchemaID: 1, TableID: 1}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().DoDDLJob(ctx, job)
	require.Error(t, err)
	require.Equal(t, err.Error(), "mockAddBatchDDLJobsErr")
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockAddBatchDDLJobsErr"))
}

func TestParallelDDL(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	/*
		build structure:
			DBs -> {
			 db1: test_parallel_ddl_1
			 db2: test_parallel_ddl_2
			}
			Tables -> {
			 db1.t1 (c1 int, c2 int)
			 db1.t2 (c1 int primary key, c2 int, c3 int)
			 db2.t3 (c1 int, c2 int, c3 int, c4 int)
			}
	*/
	tk.MustExec("create database test_parallel_ddl_1")
	tk.MustExec("create database test_parallel_ddl_2")
	tk.MustExec("create table test_parallel_ddl_1.t1(c1 int, c2 int, key db1_idx2(c2))")
	tk.MustExec("create table test_parallel_ddl_1.t2(c1 int primary key, c2 int, c3 int)")
	tk.MustExec("create table test_parallel_ddl_2.t3(c1 int, c2 int, c3 int, c4 int)")

	// set hook to execute jobs after all jobs are in queue.
	jobCnt := int64(11)
	tc := &ddl.TestDDLCallback{Do: dom}
	once := sync.Once{}
	var checkErr error
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		// TODO: extract a unified function for other tests.
		once.Do(func() {
			qLen1 := int64(0)
			qLen2 := int64(0)
			var err error
			for {
				checkErr = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
					m := meta.NewMeta(txn)
					qLen1, err = m.DDLJobQueueLen()
					if err != nil {
						return err
					}
					qLen2, err = m.DDLJobQueueLen(meta.AddIndexJobListKey)
					if err != nil {
						return err
					}
					return nil
				})
				if checkErr != nil {
					break
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						checkErr = errors.Errorf("add index jobs cnt %v != 6", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	dom.DDL().SetHook(tc)

	/*
		prepare jobs:
		/	job no.	/	database no.	/	table no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add column	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop column	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop column	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schema  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	var wg util.WaitGroupWrapper

	seqIDs := make([]int, 11)

	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t1 add index db1_idx1(c1)")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[0], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t1 add column c3 int")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[1], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t1 add index db1_idxx(c1)")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[2], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t2 drop column c3")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[3], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t1 drop index db1_idx2")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[4], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t2 add index db1_idx2(c2)")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[5], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_2.t3 drop column c4")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[6], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_2.t3 auto_id_cache 1024")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[7], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("alter table test_parallel_ddl_1.t1 add index db1_idx3(c2)")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[8], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("drop database test_parallel_ddl_2")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[9], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})
	time.Sleep(5 * time.Millisecond)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		_, err := tk.Exec("alter table test_parallel_ddl_2.t3 add index db3_idx1(c2)")
		require.Error(t, err)
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[10], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})

	wg.Wait()

	// Table 1 order.
	require.Less(t, seqIDs[0], seqIDs[1])
	require.Less(t, seqIDs[1], seqIDs[2])
	require.Less(t, seqIDs[2], seqIDs[4])
	require.Less(t, seqIDs[4], seqIDs[8])

	// Table 2 order.
	require.Less(t, seqIDs[3], seqIDs[10])

	// Table 3 order.
	require.Less(t, seqIDs[6], seqIDs[7])
	require.Less(t, seqIDs[7], seqIDs[9])
	require.Less(t, seqIDs[9], seqIDs[10])

	// General job order.
	require.Less(t, seqIDs[1], seqIDs[3])
	require.Less(t, seqIDs[3], seqIDs[4])
	require.Less(t, seqIDs[4], seqIDs[6])
	require.Less(t, seqIDs[6], seqIDs[7])
	require.Less(t, seqIDs[7], seqIDs[9])

	// Reorg job order.
	require.Less(t, seqIDs[2], seqIDs[5])
	require.Less(t, seqIDs[5], seqIDs[8])
	require.Less(t, seqIDs[8], seqIDs[10])
}
