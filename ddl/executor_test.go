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
)

func (s *testDDLSuite) TestDropSchemaError(c *C) {
	store := testCreateStore(c, "test_drop_schema")
	defer store.Close()

	lease := 10 * time.Millisecond
	d := newDDL(store, nil, nil, lease)
	defer d.close()

	task := &model.Job{
		SchemaID: 1,
		Type:     model.ActionDropSchema,
		Args: []interface{}{&model.DBInfo{
			Name: model.CIStr{O: "test"},
		}},
	}
	d.prepareTask(task)
	d.startTask(task.Type)

	time.Sleep(lease)
	testCheckTaskCancelled(c, d, task)
}

func testCheckTaskCancelled(c *C, d *ddl, task *model.Job) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyTask, err := t.GetHistoryDDLTask(task.ID)
		c.Assert(err, IsNil)
		c.Assert(historyTask.State, Equals, model.JobCancelled)

		return nil
	})
}

func (s *testDDLSuite) TestInvalidTaskType(c *C) {
	store := testCreateStore(c, "test_invalid_task_type")
	defer store.Close()

	lease := 10 * time.Millisecond
	d := newDDL(store, nil, nil, lease)
	defer d.close()

	task := &model.Job{
		SchemaID: 1,
		TableID:  1,
		Type:     model.ActionCreateTable,
	}
	d.prepareTask(task)
	d.startTask(model.ActionDropTable)

	time.Sleep(lease)
	testCheckTaskCancelled(c, d, task)
}
