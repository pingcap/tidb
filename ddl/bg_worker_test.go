// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testDDLSuite) TestDropSchema(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_drop_schema")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.Stop()

	job := &model.Job{
		SchemaID: 1,
		Type:     model.ActionDropSchema,
	}
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return d.prepareBgJob(t, job)
	})
	c.Check(err, IsNil)
	d.startBgJob(job.Type)

	verifyBgJobState(c, d, job, model.JobDone, testLease*2)
}

func verifyBgJobState(c *C, d *ddl, job *model.Job, state model.JobState, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	var isDone bool
	for i := 0; i < 50; i++ {
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			historyBgJob, err := t.GetHistoryBgJob(job.ID)
			c.Assert(err, IsNil)
			if historyBgJob != nil {
				c.Assert(historyBgJob, NotNil)
				c.Assert(historyBgJob.State, Equals, state)
				isDone = true
			}
			return nil
		})
		if isDone {
			break
		}
		c.Logf("historyBgJob is nil, no.%v", i)
		time.Sleep(testLease)
	}
	c.Assert(isDone, IsTrue)
}

func (s *testDDLSuite) TestDropTableError(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_drop_table")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.Stop()

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  1,
		Type:     model.ActionDropTable,
		Args: []interface{}{0, nil,
			&model.TableInfo{
				ID:   1,
				Name: model.CIStr{O: "t"},
			}},
	}
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return d.prepareBgJob(t, job)
	})
	c.Check(err, IsNil)
	d.startBgJob(job.Type)

	verifyBgJobState(c, d, job, model.JobCancelled, testLease*2)
}

func (s *testDDLSuite) TestInvalidBgJobType(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_invalid_bg_job_type")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.Stop()

	job := &model.Job{
		SchemaID:   1,
		TableID:    1,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return d.prepareBgJob(t, job)
	})
	c.Check(err, IsNil)
	d.startBgJob(model.ActionDropTable)

	verifyBgJobState(c, d, job, model.JobCancelled, testLease*2)
}
