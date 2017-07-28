// Copyright 2017 PingCAP, Inc.
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

package delrangesql

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	insertDeleteRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`
)

// LoadPendingBgJobsIntoDeleteTable loads all pending DDL backgroud jobs
// into table `gc_delete_range` so that gc worker can deal with them.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func LoadPendingBgJobsIntoDeleteTable(ctx context.Context) (err error) {
	var met = meta.NewMeta(ctx.Txn())
	for {
		var job *model.Job
		job, err = met.DeQueueBgJob()
		if err != nil || job == nil {
			break
		}
		err = InsertBgJobIntoDeleteRangeTable(ctx.(sqlexec.SQLExecutor), job)
		if err != nil {
			break
		}
	}
	return errors.Trace(err)
}

// InsertBgJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func InsertBgJobIntoDeleteRangeTable(s sqlexec.SQLExecutor, job *model.Job) error {
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, tableID := range tableIDs {
			startKey := tablecodec.EncodeTablePrefix(tableID)
			endKey := tablecodec.EncodeTablePrefix(tableID + 1)
			if err := doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix()); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix())
	}
	return nil
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts int64) error {
	log.Infof("[ddl] insert into delete-range table with key: (%d,%d)", jobID, elementID)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(sql)
	return errors.Trace(err)
}
