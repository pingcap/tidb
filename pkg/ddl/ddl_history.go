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

package ddl

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/pingcap/errors"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"go.uber.org/zap"
)

// DefNumHistoryJobs is default value of the default number of history job
const (
	DefNumHistoryJobs   = 10
	batchNumHistoryJobs = 128
)

// AddHistoryDDLJob record the history job.
func AddHistoryDDLJob(sess *sess.Session, t *meta.Meta, job *model.Job, updateRawArgs bool) error {
	err := addHistoryDDLJob2Table(sess, job, updateRawArgs)
	if err != nil {
		logutil.DDLLogger().Info("failed to add DDL job to history table", zap.Error(err))
	}
	// we always add history DDL job to job list at this moment.
	return t.AddHistoryDDLJob(job, updateRawArgs)
}

// addHistoryDDLJob2Table adds DDL job to history table.
func addHistoryDDLJob2Table(sess *sess.Session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	_, err = sess.Execute(context.Background(),
		fmt.Sprintf("insert ignore into mysql.tidb_ddl_history(job_id, job_meta, db_name, table_name, schema_ids, table_ids, create_time) values (%d, %s, %s, %s, %s, %s, %v)",
			job.ID, util.WrapKey2String(b), strconv.Quote(job.SchemaName), strconv.Quote(job.TableName),
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)),
			strconv.Quote(strconv.FormatInt(job.TableID, 10)),
			strconv.Quote(model.TSConvert2Time(job.StartTS).String()),
		),
		"insert_history")
	return errors.Trace(err)
}

// GetHistoryJobByID return history DDL job by ID.
func GetHistoryJobByID(sess sessionctx.Context, id int64) (*model.Job, error) {
	err := sessiontxn.NewTxn(context.Background(), sess)
	if err != nil {
		return nil, err
	}
	defer func() {
		// we can ignore the commit error because this txn is readonly.
		_ = sess.CommitTxn(context.Background())
	}()
	txn, err := sess.Txn(true)
	if err != nil {
		return nil, err
	}
	t := meta.NewMeta(txn)
	job, err := t.GetHistoryDDLJob(id)
	return job, errors.Trace(err)
}

// GetLastNHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetLastNHistoryDDLJobs(t *meta.Meta, maxNumJobs int) ([]*model.Job, error) {
	iterator, err := GetLastHistoryDDLJobsIterator(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iterator.GetLastJobs(maxNumJobs, nil)
}

// IterHistoryDDLJobs iterates history DDL jobs until the `finishFn` return true or error.
func IterHistoryDDLJobs(txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	txnMeta := meta.NewMeta(txn)
	iter, err := GetLastHistoryDDLJobsIterator(txnMeta)
	if err != nil {
		return err
	}
	cacheJobs := make([]*model.Job, 0, DefNumHistoryJobs)
	for {
		cacheJobs, err = iter.GetLastJobs(DefNumHistoryJobs, cacheJobs)
		if err != nil || len(cacheJobs) == 0 {
			return err
		}
		finish, err := finishFn(cacheJobs)
		if err != nil || finish {
			return err
		}
	}
}

// GetLastHistoryDDLJobsIterator gets latest N history DDL jobs iterator.
func GetLastHistoryDDLJobsIterator(m *meta.Meta) (meta.LastJobIterator, error) {
	return m.GetLastHistoryDDLJobsIterator()
}

// GetAllHistoryDDLJobs get all the done DDL jobs.
func GetAllHistoryDDLJobs(m *meta.Meta) ([]*model.Job, error) {
	iterator, err := GetLastHistoryDDLJobsIterator(m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allJobs := make([]*model.Job, 0, batchNumHistoryJobs)
	for {
		jobs, err := iterator.GetLastJobs(batchNumHistoryJobs, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allJobs = append(allJobs, jobs...)
		if len(jobs) < batchNumHistoryJobs {
			break
		}
	}
	// sort job.
	slices.SortFunc(allJobs, func(i, j *model.Job) int {
		return cmp.Compare(i.ID, j.ID)
	})
	return allJobs, nil
}

// ScanHistoryDDLJobs get some of the done DDL jobs.
// When the DDL history is quite large, GetAllHistoryDDLJobs() API can't work well, because it makes the server OOM.
// The result is in descending order by job ID.
func ScanHistoryDDLJobs(m *meta.Meta, startJobID int64, limit int) ([]*model.Job, error) {
	var iter meta.LastJobIterator
	var err error
	if startJobID == 0 {
		iter, err = m.GetLastHistoryDDLJobsIterator()
	} else {
		if limit == 0 {
			return nil, errors.New("when 'start_job_id' is specified, it must work with a 'limit'")
		}
		iter, err = m.GetHistoryDDLJobsIterator(startJobID)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter.GetLastJobs(limit, nil)
}
