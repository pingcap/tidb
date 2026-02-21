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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"go.uber.org/zap"
)

func getTableByTxn(ctx context.Context, store kv.Storage, schemaID, tableID int64) (*model.DBInfo, table.Table, error) {
	var tbl table.Table
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tblInfo, err1 := getTableInfo(t, tableID, schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		// This tableInfo should never interact with the autoid allocator,
		// so we can use the autoid.Allocators{} here.
		// TODO(tangenta): Use model.TableInfo instead of tables.Table.
		tbl, err1 = table.TableFromMeta(autoid.Allocators{}, tblInfo)
		return errors.Trace(err1)
	})
	return dbInfo, tbl, err
}

func updateDDLJob2Table(
	ctx context.Context,
	se *sess.Session,
	job *model.Job,
	updateRawArgs bool,
) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.tidb_ddl_job set job_meta = %s where job_id = %d", util.WrapKey2String(b), job.ID)
	_, err = se.Execute(ctx, sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle gets DDL reorg handle.
func getDDLReorgHandle(se *sess.Session, job *model.Job) (element *meta.Element,
	startKey, endKey kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select ele_id, ele_type, start_key, end_key, physical_id, reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	id := rows[0].GetInt64(0)
	tp := rows[0].GetBytes(1)
	element = &meta.Element{
		ID:      id,
		TypeKey: tp,
	}
	startKey = rows[0].GetBytes(2)
	endKey = rows[0].GetBytes(3)
	physicalTableID = rows[0].GetInt64(4)
	return
}

func getImportedKeyFromCheckpoint(se *sess.Session, job *model.Job) (imported kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, 0, err
	}
	if len(rows) == 0 {
		return nil, 0, meta.ErrDDLReorgElementNotExist
	}
	if !rows[0].IsNull(0) {
		rawReorgMeta := rows[0].GetBytes(0)
		var reorgMeta ingest.JobReorgMeta
		err = json.Unmarshal(rawReorgMeta, &reorgMeta)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		if cp := reorgMeta.Checkpoint; cp != nil {
			logutil.DDLIngestLogger().Info("resume physical table ID from checkpoint",
				zap.Int64("jobID", job.ID),
				zap.String("global sync key", hex.EncodeToString(cp.GlobalSyncKey)),
				zap.Int64("checkpoint physical ID", cp.PhysicalID))
			return cp.GlobalSyncKey, cp.PhysicalID, nil
		}
	}
	return
}

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
// Caller should wrap this in a separate transaction, to avoid conflicts.
func updateDDLReorgHandle(se *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, jobID)
	_, err := se.Execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(s *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	rawReorgMeta, err := json.Marshal(ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			PhysicalID: physicalTableID,
			Version:    ingest.JobCheckpointVersionCurrent,
		}})
	if err != nil {
		return errors.Trace(err)
	}
	del := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", jobID)
	ins := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id, reorg_meta) values (%d, %d, %s, %s, %s, %d, %s)",
		jobID, element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, util.WrapKey2String(rawReorgMeta))
	return s.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), del, "init_handle")
		if err != nil {
			logutil.DDLLogger().Info("initDDLReorgHandle failed to delete", zap.Int64("jobID", jobID), zap.Error(err))
		}
		_, err = se.Execute(context.Background(), ins, "init_handle")
		return err
	})
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(se *sess.Session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(se *sess.Session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// cleanDDLReorgHandles removes handles that are no longer needed.
func cleanDDLReorgHandles(se *sess.Session, job *model.Job) error {
	sql := "delete from mysql.tidb_ddl_reorg where job_id = " + strconv.FormatInt(job.ID, 10)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "clean_handle")
		return err
	})
}

func getJobsBySQL(
	ctx context.Context,
	se *sess.Session,
	condition string,
) ([]*model.Job, error) {
	rows, err := se.Execute(ctx, fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where %s", condition), "get_job")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, 0, 16)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err := job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}
