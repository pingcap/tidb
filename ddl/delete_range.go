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

package ddl

import (
	"encoding/base64"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	insertDeleteRangeSQL = `INSERT INTO gc_delete_range VALUES ("%d", "%s", "%s", "%d")`
)

func LoadPendingBgJobs(store kv.Storage) (jobs []*model.Job, err error) {
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		for i := 0; ; i++ {
			job, err := m.GetBgJob(i)
			if err != nil {
				return errors.Trace(err)
			}
			if job == nil {
				break
			}
			jobs = append(jobs, job)
		}
		return nil
	})
	return
}

func InsertBgJobIntoDeleteRangeTable(s sqlexec.SQLExecutor, job *model.Job) (err error) {
	if _, err = s.Execute("COMMIT"); err != nil {
		return errors.Trace(err)
	}
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, tableID := range tableIDs {
			startKey := tablecodec.EncodeTablePrefix(tableID)
			endKey := tablecodec.EncodeTablePrefix(tableID + 1)
			sql := getInsertDeleteRangeSQL(tableID, startKey, endKey, 1) // TODO real timestamp.
			s.Execute(sql)
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		var startKey, endKey kv.Key
		if err := job.DecodeArgs(&startKey); err != nil {
			return errors.Trace(err)
		}
		tableID, _, _, err := tablecodec.DecodeKeyHead(startKey)
		if err != nil {
			return errors.Trace(err)
		}
		endKey = tablecodec.EncodeTablePrefix(tableID + 1)
		sql := getInsertDeleteRangeSQL(tableID, startKey, endKey, 1) // TODO real timestamp.
		s.Execute(sql)
	}
	if _, err = s.Execute("COMMIT"); err != nil {
		return errors.Trace(err)
	}
	return
}

func getInsertDeleteRangeSQL(id int64, startKey, endKey kv.Key, ts uint64) string {
	startKeyEncoded := base64.StdEncoding.EncodeToString(startKey)
	endKeyEncoded := base64.StdEncoding.EncodeToString(endKey)
	return fmt.Sprintf(insertDeleteRangeSQL, id, startKeyEncoded, endKeyEncoded, ts)
}
