// Copyright 2022 PingCAP, Inc.
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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func (d *ddl) getSomeGeneralJob(sess sessionctx.Context) ([]model.Job, error) {
	//sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	//defer func() {
	//	sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	//}()
	runningOrBlockedIDs := make([]string, 0, 10)
	d.runningReorgJobMapMu.RLock()
	for id := range d.runningReorgJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	d.runningReorgJobMapMu.RUnlock()
	var sql string
	ts := time.Now()
	if len(runningOrBlockedIDs) == 0 {
		sql = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id)) and not reorg"
	} else {
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id)) and not reorg and job_id not in (%s)", strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
	rs.Close()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobs := make([]model.Job, 0, 5)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, err
		}
		log.Warn("get ddl job from table", zap.String("time", time.Since(ts).String()), zap.String("job", job.String()))
		if job.Type == model.ActionDropSchema {
			for {
				canRun, err := d.checkDropSchemaJobIsRunnable(sess, &job)
				if err != nil {
					return nil, err
				}
				if canRun {
					log.Warn("get ddl job from table", zap.String("time", time.Since(ts).String()), zap.String("job", job.String()))
					jobs = append(jobs, job)
					break
				}
				runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(int(job.ID)))
				sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id) limit 20) and not reorg and job_id not in (%s)", strings.Join(runningOrBlockedIDs, ", "))
				rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
				if err != nil {
					return nil, err
				}
				rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
				rs.Close()
				if err != nil {
					return nil, err
				}
				if len(rows) == 0 {
					continue
				}
				jobBinary = rows[0].GetBytes(0)
				job = model.Job{}
				err = job.Decode(jobBinary)
				if err != nil {
					return nil, err
				}
			}
		} else {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (d *ddl) checkDropSchemaJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and reorg and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	rs.Close()
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) checkReorgJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and is_drop_schema and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	rs.Close()
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) getOneReorgJob(sess sessionctx.Context) (*model.Job, error) {
	//sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	//defer func() {
	//	sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	//}()
	runningOrBlockedIDs := make([]string, 0, 10)
	d.runningReorgJobMapMu.RLock()
	for id := range d.runningReorgJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	d.runningReorgJobMapMu.RUnlock()
	var sql string
	if len(runningOrBlockedIDs) == 0 {
		sql = "select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id order by min(job_id) limit 1)"
	} else {
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
	rs.Close()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobBinary := rows[0].GetBytes(0)
	job := &model.Job{}
	err = job.Decode(jobBinary)
	if err != nil {
		return nil, err
	}
	for {
		canRun, err := d.checkReorgJobIsRunnable(sess, job)
		if err != nil {
			return nil, err
		}
		if canRun {
			return job, nil
		}
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(int(job.ID)))
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) and not processing from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
		rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
		if err != nil {
			return nil, err
		}
		rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
		rs.Close()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
		jobBinary = rows[0].GetBytes(0)
		job = &model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, err
		}
	}
}

func (d *ddl) startDispatchLoop() {
	time.Sleep(2 * time.Second)
	for !d.isOwner() {
		time.Sleep(time.Second)
	}
	sess, _ := d.sessPool.get()
	defer d.sessPool.put(sess)
	var notifyDDLJobByEtcdChGeneral clientv3.WatchChan
	var notifyDDLJobByEtcdChReorg clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
		notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var ok bool
	for {
		select {
		case <-d.ddlJobCh:
			sleep := false
			for !sleep {
				log.Warn("wwz before getSomeGeneralJob", zap.String("time", time.Now().String()))
				jobs, err := d.getSomeGeneralJob(sess)
				if err != nil {
					log.Warn("err", zap.Error(err))
				}
				if jobs != nil {
					for _, jobb := range jobs {
						job := jobb
						d.runningReorgJobMapMu.Lock()
						d.runningReorgJobMap[int(job.ID)] = struct{}{}
						d.runningReorgJobMapMu.Unlock()
						go func() {
							defer func() {
								d.runningReorgJobMapMu.Lock()
								delete(d.runningReorgJobMap, int(job.ID))
								d.runningReorgJobMapMu.Unlock()
							}()
							wk, _ := d.gwp.get()
							wk.handleDDLJob(d.ddlCtx, &job, d.ddlJobCh)
							d.gwp.put(wk)
						}()
					}
				}
				if jobs == nil {
					sleep = true
				}
			}
		case <-ticker.C:
			//log.Info("wwz tickerddl")
			sleep := false
			for !sleep {
				jobs, err := d.getSomeGeneralJob(sess)
				if err != nil {
					log.Warn("err", zap.Error(err))
				}
				if jobs != nil {
					for _, jobb := range jobs {
						job := jobb
						d.runningReorgJobMapMu.Lock()
						d.runningReorgJobMap[int(job.ID)] = struct{}{}
						d.runningReorgJobMapMu.Unlock()
						go func() {
							defer func() {
								d.runningReorgJobMapMu.Lock()
								delete(d.runningReorgJobMap, int(job.ID))
								d.runningReorgJobMapMu.Unlock()
							}()
							wk, _ := d.gwp.get()
							wk.handleDDLJob(d.ddlCtx, &job, d.ddlJobCh)
							d.gwp.put(wk)
						}()
					}
				}
				job2, _ := d.getOneReorgJob(sess)
				if job2 != nil {
					d.runningReorgJobMapMu.Lock()
					d.runningReorgJobMap[int(job2.ID)] = struct{}{}
					d.runningReorgJobMapMu.Unlock()
					go func() {
						defer func() {
							d.runningReorgJobMapMu.Lock()
							delete(d.runningReorgJobMap, int(job2.ID))
							d.runningReorgJobMapMu.Unlock()
						}()
						wk, _ := d.wp.get()
						wk.handleDDLJob(d.ddlCtx, job2, d.ddlJobCh)
						d.wp.put(wk)
					}()
				}
				if jobs == nil && job2 == nil {
					sleep = true
				}
			}
		case _, ok = <-notifyDDLJobByEtcdChGeneral:
			log.Info("wwz notifyDDLJobByEtcdChReorg")
			if !ok {
				panic("notifyDDLJobByEtcdChGeneral in trouble")
			}
			jobs, _ := d.getSomeGeneralJob(sess)
			if jobs != nil {
				for _, jobb := range jobs {
					job := jobb
					d.runningReorgJobMapMu.Lock()
					d.runningReorgJobMap[int(job.ID)] = struct{}{}
					d.runningReorgJobMapMu.Unlock()
					go func() {
						defer func() {
							d.runningReorgJobMapMu.Lock()
							delete(d.runningReorgJobMap, int(job.ID))
							d.runningReorgJobMapMu.Unlock()
						}()
						wk, _ := d.gwp.get()
						// TODO(hawkingrei): refactor it
						// 1、 panic: context canceled
						// 2. sync: WaitGroup is reused before previous Wait has returned
						wk.handleDDLJob(d.ddlCtx, &job, d.ddlJobCh)
						d.gwp.put(wk)
					}()
				}
			}
		case _, ok = <-notifyDDLJobByEtcdChReorg:
			log.Info("wwz notifyDDLJobByEtcdChReorg")
			if !ok {
				panic("notifyDDLJobByEtcdChReorg in trouble")
			}
			job, _ := d.getOneReorgJob(sess)
			if job != nil {
				d.runningReorgJobMapMu.Lock()
				d.runningReorgJobMap[int(job.ID)] = struct{}{}
				d.runningReorgJobMapMu.Unlock()
				go func() {
					defer func() {
						d.runningReorgJobMapMu.Lock()
						delete(d.runningReorgJobMap, int(job.ID))
						d.runningReorgJobMapMu.Unlock()
					}()
					wk, _ := d.wp.get()
					wk.handleDDLJob(d.ddlCtx, job, d.ddlJobCh)
					d.wp.put(wk)
				}()
			}
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *ddl) addDDLJobs(job []*model.Job) error {
	sql := "insert into mysql.tidb_ddl_job values"
	for i, job := range job {
		b, err := job.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("(%d, %t, %d, %d, 0x%x, %d, %t)", job.ID, mayNeedReorg(job), job.SchemaID, job.TableID, b, 0, job.Type == model.ActionDropSchema)
	}
	log.Warn("add ddl job to table", zap.String("sql", sql))
	ts := time.Now()
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	log.Warn("add ddl job to table", zap.String("sql", sql), zap.String("time", time.Since(ts).String()))
	return nil
}

func (d *ddl) addDDLJob(job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return err
	}
	ts := time.Now()
	sql := fmt.Sprintf("insert into mysql.tidb_ddl_job values(%d, %t, %d, %d, 0x%x, %d, %t)", job.ID, mayNeedReorg(job), job.SchemaID, job.TableID, b, 0, job.Type == model.ActionDropSchema)
	log.Warn("add ddl job to table", zap.String("sql", sql))

	sess, err := d.sessPool.get()
	if err != nil {
		log.Error("add sessPool", zap.Error(err))
		return err
	}
	defer d.sessPool.put(sess)

	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		log.Error("add ddl job to table", zap.Error(err))
		return err
	}
	log.Warn("add ddl job to table", zap.String("sql", sql), zap.String("time", time.Since(ts).String()))
	return nil
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) updateDDLJobNew(job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	log.Warn("update ddl job", zap.String("job", job.String()))
	sql := fmt.Sprintf("update mysql.tidb_ddl_job set job_meta = 0x%x where job_id = %d", b, job.ID)
	_, err = w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgStartHandleNew(job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key) values (%d, %d, 0x%s)", job.ID, element.ID, wrapKey2String(startKey))
	_, err = w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgHandle(job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *meta.Element, newSess bool) error {
	sess := w.sessForJob
	if newSess {
		sess, _ = w.sessPool.get()
		defer w.sessPool.put(sess)
		sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
		defer sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	}
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key, end_key, physical_id) values (%d, %d, %s, %s, %d)", job.ID, element.ID, wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) RemoveDDLReorgHandle(job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}
