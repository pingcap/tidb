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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	deleteRangesTable            = `gc_delete_range`
	doneDeleteRangesTable        = `gc_delete_range_done`
	loadDeleteRangeSQL           = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%n WHERE ts < %?`
	recordDoneDeletedRangeSQL    = `INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	completeDeleteRangeSQL       = `DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	completeDeleteMultiRangesSQL = `DELETE FROM mysql.gc_delete_range WHERE job_id = %?`
	updateDeleteRangeSQL         = `UPDATE mysql.gc_delete_range SET start_key = %? WHERE job_id = %? AND element_id = %? AND start_key = %?`
	deleteDoneRecordSQL          = `DELETE FROM mysql.gc_delete_range_done WHERE job_id = %? AND element_id = %?`
	loadGlobalVars               = `SELECT HIGH_PRIORITY variable_name, variable_value from mysql.global_variables where variable_name in (` // + nameList + ")"
	// KeyOpDefaultTimeout is the default timeout for each key operation.
	KeyOpDefaultTimeout = 2 * time.Second
	// KeyOpRetryInterval is the interval between two key operations.
	KeyOpRetryInterval = 30 * time.Millisecond
	// DDLAllSchemaVersions is the path on etcd that is used to store all servers current schema versions.
	DDLAllSchemaVersions = "/tidb/ddl/all_schema_versions"
	// DDLAllSchemaVersionsByJob is the path on etcd that is used to store all servers current schema versions.
	// /tidb/ddl/all_schema_by_job_versions/<job-id>/<tidb-id> ---> <schema-version>
	DDLAllSchemaVersionsByJob = "/tidb/ddl/all_schema_by_job_versions"
	// DDLGlobalSchemaVersion is the path on etcd that is used to store the latest schema versions.
	DDLGlobalSchemaVersion = "/tidb/ddl/global_schema_version"
	// ServerGlobalState is the path on etcd that is used to store the server global state.
	ServerGlobalState = "/tidb/server/global_state"
	// SessionTTL is the etcd session's TTL in seconds.
	SessionTTL = 90
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	StartKey  kv.Key
	EndKey    kv.Key
	JobID     int64
	ElementID int64
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() (kv.Key, kv.Key) {
	return t.StartKey, t.EndKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx context.Context, sctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromTable(ctx, sctx, deleteRangesTable, safePoint)
}

// LoadDoneDeleteRanges loads deleted ranges from gc_delete_range_done table.
func LoadDoneDeleteRanges(ctx context.Context, sctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromTable(ctx, sctx, doneDeleteRangesTable, safePoint)
}

func loadDeleteRangesFromTable(ctx context.Context, sctx sessionctx.Context, table string, safePoint uint64) (ranges []DelRangeTask, _ error) {
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, loadDeleteRangeSQL, table, safePoint)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			startKey, err := hex.DecodeString(row.GetString(2))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(row.GetString(3))
			if err != nil {
				return nil, errors.Trace(err)
			}
			ranges = append(ranges, DelRangeTask{
				JobID:     row.GetInt64(0),
				ElementID: row.GetInt64(1),
				StartKey:  startKey,
				EndKey:    endKey,
			})
		}
	}
	return ranges, nil
}

// CompleteDeleteRange moves a record from gc_delete_range table to gc_delete_range_done table.
func CompleteDeleteRange(sctx sessionctx.Context, dr DelRangeTask, needToRecordDone bool) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return errors.Trace(err)
	}

	if needToRecordDone {
		_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, recordDoneDeletedRangeSQL, dr.JobID, dr.ElementID)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = RemoveFromGCDeleteRange(sctx, dr.JobID, dr.ElementID)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, "COMMIT")
	return errors.Trace(err)
}

// RemoveFromGCDeleteRange is exported for ddl pkg to use.
func RemoveFromGCDeleteRange(sctx sessionctx.Context, jobID, elementID int64) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, completeDeleteRangeSQL, jobID, elementID)
	return errors.Trace(err)
}

// RemoveMultiFromGCDeleteRange is exported for ddl pkg to use.
func RemoveMultiFromGCDeleteRange(ctx context.Context, sctx sessionctx.Context, jobID int64) error {
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, completeDeleteMultiRangesSQL, jobID)
	return errors.Trace(err)
}

// DeleteDoneRecord removes a record from gc_delete_range_done table.
func DeleteDoneRecord(sctx sessionctx.Context, dr DelRangeTask) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, deleteDoneRecordSQL, dr.JobID, dr.ElementID)
	return errors.Trace(err)
}

// UpdateDeleteRange is only for emulator.
func UpdateDeleteRange(sctx sessionctx.Context, dr DelRangeTask, newStartKey, oldStartKey kv.Key) error {
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, updateDeleteRangeSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	return errors.Trace(err)
}

// LoadDDLReorgVars loads ddl reorg variable from mysql.global_variables.
func LoadDDLReorgVars(ctx context.Context, sctx sessionctx.Context) error {
	// close issue #21391
	// variable.TiDBRowFormatVersion is used to encode the new row for column type change.
	return LoadGlobalVars(ctx, sctx, []string{variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize, variable.TiDBRowFormatVersion})
}

// LoadDDLVars loads ddl variable from mysql.global_variables.
func LoadDDLVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(context.Background(), ctx, []string{variable.TiDBDDLErrorCountLimit})
}

// LoadGlobalVars loads global variable from mysql.global_variables.
func LoadGlobalVars(ctx context.Context, sctx sessionctx.Context, varNames []string) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	// *mock.Context does not support SQL execution. Only do it when sctx is not `mock.Context`
	if _, ok := sctx.(*mock.Context); !ok {
		e := sctx.GetRestrictedSQLExecutor()
		var buf strings.Builder
		buf.WriteString(loadGlobalVars)
		paramNames := make([]any, 0, len(varNames))
		for i, name := range varNames {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("%?")
			paramNames = append(paramNames, name)
		}
		buf.WriteString(")")
		rows, _, err := e.ExecRestrictedSQL(ctx, nil, buf.String(), paramNames...)
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range rows {
			varName := row.GetString(0)
			varValue := row.GetString(1)
			if err = sctx.GetSessionVars().SetSystemVarWithoutValidation(varName, varValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetTimeZone gets the session location's zone name and offset.
func GetTimeZone(sctx sessionctx.Context) (string, int) {
	loc := sctx.GetSessionVars().Location()
	name := loc.String()
	if name != "" {
		_, err := time.LoadLocation(name)
		if err == nil {
			return name, 0
		}
	}
	_, offset := time.Now().In(loc).Zone()
	return "", offset
}

// enableEmulatorGC means whether to enable emulator GC. The default is enable.
// In some unit tests, we want to stop emulator GC, then wen can set enableEmulatorGC to 0.
var emulatorGCEnable = atomicutil.NewInt32(1)

// EmulatorGCEnable enables emulator gc. It exports for testing.
func EmulatorGCEnable() {
	emulatorGCEnable.Store(1)
}

// EmulatorGCDisable disables emulator gc. It exports for testing.
func EmulatorGCDisable() {
	emulatorGCEnable.Store(0)
}

// IsEmulatorGCEnable indicates whether emulator GC enabled. It exports for testing.
func IsEmulatorGCEnable() bool {
	return emulatorGCEnable.Load() == 1
}

var internalResourceGroupTag = []byte{0}

// GetInternalResourceGroupTaggerForTopSQL only use for testing.
func GetInternalResourceGroupTaggerForTopSQL() tikvrpc.ResourceGroupTagger {
	tagger := func(req *tikvrpc.Request) {
		req.ResourceGroupTag = internalResourceGroupTag
	}
	return tagger
}

// IsInternalResourceGroupTaggerForTopSQL use for testing.
func IsInternalResourceGroupTaggerForTopSQL(tag []byte) bool {
	return bytes.Equal(tag, internalResourceGroupTag)
}

// DeleteKeyFromEtcd deletes key value from etcd.
func DeleteKeyFromEtcd(key string, etcdCli *clientv3.Client, retryCnt int, timeout time.Duration) error {
	var err error
	ctx := context.Background()
	for i := 0; i < retryCnt; i++ {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = etcdCli.Delete(childCtx, key)
		cancel()
		if err == nil {
			return nil
		}
		logutil.DDLLogger().Warn("etcd-cli delete key failed", zap.String("key", key), zap.Error(err), zap.Int("retryCnt", i))
	}
	return errors.Trace(err)
}

// PutKVToEtcdMono puts key value to etcd monotonously.
// etcdCli is client of etcd.
// retryCnt is retry time when an error occurs.
// opts are configures of etcd Operations.
func PutKVToEtcdMono(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if err = ctx.Err(); err != nil {
			return errors.Trace(err)
		}

		childCtx, cancel := context.WithTimeout(ctx, KeyOpDefaultTimeout)
		var resp *clientv3.GetResponse
		resp, err = etcdCli.Get(childCtx, key)
		if err != nil {
			cancel()
			logutil.DDLLogger().Warn("etcd-cli put kv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
			time.Sleep(KeyOpRetryInterval)
			continue
		}
		prevRevision := int64(0)
		if len(resp.Kvs) > 0 {
			prevRevision = resp.Kvs[0].ModRevision
		}

		var txnResp *clientv3.TxnResponse
		txnResp, err = etcdCli.Txn(childCtx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", prevRevision)).
			Then(clientv3.OpPut(key, val, opts...)).
			Commit()

		cancel()

		if err == nil && txnResp.Succeeded {
			return nil
		}

		if err == nil {
			err = errors.New("performing compare-and-swap during PutKVToEtcd failed")
		}

		logutil.DDLLogger().Warn("etcd-cli put kv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
		time.Sleep(KeyOpRetryInterval)
	}
	return errors.Trace(err)
}

// PutKVToEtcd puts key value to etcd.
// etcdCli is client of etcd.
// retryCnt is retry time when an error occurs.
// opts are configures of etcd Operations.
func PutKVToEtcd(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if err = ctx.Err(); err != nil {
			return errors.Trace(err)
		}

		childCtx, cancel := context.WithTimeout(ctx, KeyOpDefaultTimeout)
		_, err = etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			return nil
		}
		logutil.DDLLogger().Warn("etcd-cli put kv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
		time.Sleep(KeyOpRetryInterval)
	}
	return errors.Trace(err)
}

// WrapKey2String wraps the key to a string.
func WrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}

const (
	getRaftKvVersionSQL = "select `value` from information_schema.cluster_config where type = 'tikv' and `key` = 'storage.engine'"
	raftKv2             = "raft-kv2"
)

// IsRaftKv2 checks whether the raft-kv2 is enabled
func IsRaftKv2(ctx context.Context, sctx sessionctx.Context) (bool, error) {
	// Mock store does not support `show config` now, so we  use failpoint here
	// to control whether we are in raft-kv2
	failpoint.Inject("IsRaftKv2", func(v failpoint.Value) (bool, error) {
		v2, _ := v.(bool)
		return v2, nil
	})

	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, getRaftKvVersionSQL)
	if err != nil {
		return false, err
	}
	if rs == nil {
		return false, nil
	}

	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, sctx.GetSessionVars().MaxChunkSize)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return false, nil
	}

	// All nodes should have the same type of engine
	raftVersion := rows[0].GetString(0)
	return raftVersion == raftKv2, nil
}

// FolderNotEmpty returns true only when the folder is not empty.
func FolderNotEmpty(path string) bool {
	entries, _ := os.ReadDir(path)
	return len(entries) > 0
}
