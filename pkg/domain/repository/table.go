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

package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	// WorkloadSchema is the name of database for repository worker.
	WorkloadSchema = "workload_schema"
)

var (
	workloadSchemaCIStr = model.NewCIStr(WorkloadSchema)
	zeroTime            = time.Time{}
)

type repoTbl struct {
	name     string
	createFn func(context.Context, *strings.Builder)
}

var (
	tbls = []repoTbl{
		{
			"hist_snapshots",
			func(ctx context.Context, sb *strings.Builder) {
				fmt.Fprintf(sb,
					`CREATE TABLE IF NOT EXISTS hist_snapshots (
    SNAP_ID int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Global unique identifier of the snapshot',
    BEGIN_TIME DATETIME NOT NULL COMMENT 'Datetime that TiDB begins taking this snapshot.',
    END_TIME DATETIME COMMENT 'Datetime that TiDB finish taking this snapshot.',
    DB_VER JSON NOT NULL COMMENT 'Versions of TiDB, TiKV, PD at the moment',
    WR_VER int unsigned NOT NULL COMMENT 'Version to identifiy the compatibility of workload schema between releases.',
    INSTANCE varchar(64) DEFAULT NULL COMMENT 'The instance that initializes the snapshots',
    SOURCE VARCHAR(20) NOT NULL COMMENT 'The program that initializes the snaphost. ',
    ERROR TEXT DEFAULT NULL COMMENT 'extra messages are written if anything happens to block that snapshots.'
)`)
				generatePartitionDef(sb, "begin_time")
			},
		},
		{
			"hist_sp_statements_summary",
			func(ctx context.Context, sb *strings.Builder) {
				fmt.Fprintf(sb,
					`CREATE TABLE IF NOT EXISTS hist_sp_statements_summary (
  SNAP_ID int unsigned NOT NULL COMMENT 'Identifier of the snaoshot',
  BEGIN_TIME DATETIME NOT NULL,
  INSTANCE varchar(64) DEFAULT NULL,
  STMT_TYPE varchar(64) NOT NULL COMMENT 'Statement type',
  SCHEMA_NAME varchar(64) DEFAULT NULL COMMENT 'Current schema',
  DIGEST varchar(64) DEFAULT NULL,
  DIGEST_TEXT text NOT NULL COMMENT 'Normalized statement',
  TABLE_NAMES text DEFAULT NULL COMMENT 'Involved tables',
  INDEX_NAMES text DEFAULT NULL COMMENT 'Used indices',
  SAMPLE_USER varchar(64) DEFAULT NULL COMMENT 'Sampled user who executed these statements',
  EXEC_COUNT bigint(20) unsigned NOT NULL COMMENT 'Count of executions',
  SUM_ERRORS int(11) unsigned NOT NULL COMMENT 'Sum of errors',
  SUM_WARNINGS int(11) unsigned NOT NULL COMMENT 'Sum of warnings',
  SUM_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Sum latency of these statements',
  MAX_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Max latency of these statements',
  MIN_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Min latency of these statements',
  AVG_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Average latency of these statements',
  AVG_PARSE_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Average latency of parsing',
  MAX_PARSE_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Max latency of parsing',
  AVG_COMPILE_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Average latency of compiling',
  MAX_COMPILE_LATENCY bigint(20) unsigned NOT NULL COMMENT 'Max latency of compiling',
  SUM_COP_TASK_NUM bigint(20) unsigned NOT NULL COMMENT 'Total number of CopTasks',
  MAX_COP_PROCESS_TIME bigint(20) unsigned NOT NULL COMMENT 'Max processing time of CopTasks',
  MAX_COP_PROCESS_ADDRESS varchar(256) DEFAULT NULL COMMENT 'Address of the CopTask with max processing time',
  MAX_COP_WAIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Max waiting time of CopTasks',
  MAX_COP_WAIT_ADDRESS varchar(256) DEFAULT NULL COMMENT 'Address of the CopTask with max waiting time',
  AVG_PROCESS_TIME bigint(20) unsigned NOT NULL COMMENT 'Average processing time in TiKV',
  MAX_PROCESS_TIME bigint(20) unsigned NOT NULL COMMENT 'Max processing time in TiKV',
  AVG_WAIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Average waiting time in TiKV',
  MAX_WAIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Max waiting time in TiKV',
  AVG_BACKOFF_TIME bigint(20) unsigned NOT NULL COMMENT 'Average waiting time before retry',
  MAX_BACKOFF_TIME bigint(20) unsigned NOT NULL COMMENT 'Max waiting time before retry',
  AVG_TOTAL_KEYS bigint(20) unsigned NOT NULL COMMENT 'Average number of scanned keys',
  MAX_TOTAL_KEYS bigint(20) unsigned NOT NULL COMMENT 'Max number of scanned keys',
  AVG_PROCESSED_KEYS bigint(20) unsigned NOT NULL COMMENT 'Average number of processed keys',
  MAX_PROCESSED_KEYS bigint(20) unsigned NOT NULL COMMENT 'Max number of processed keys',
  AVG_ROCKSDB_DELETE_SKIPPED_COUNT double unsigned NOT NULL COMMENT 'Average number of rocksdb delete skipped count',
  MAX_ROCKSDB_DELETE_SKIPPED_COUNT int(11) unsigned NOT NULL COMMENT 'Max number of rocksdb delete skipped count',
  AVG_ROCKSDB_KEY_SKIPPED_COUNT double unsigned NOT NULL COMMENT 'Average number of rocksdb key skipped count',
  MAX_ROCKSDB_KEY_SKIPPED_COUNT int(11) unsigned NOT NULL COMMENT 'Max number of rocksdb key skipped count',
  AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT double unsigned NOT NULL COMMENT 'Average number of rocksdb block cache hit count',
  MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT int(11) unsigned NOT NULL COMMENT 'Max number of rocksdb block cache hit count',
  AVG_ROCKSDB_BLOCK_READ_COUNT double unsigned NOT NULL COMMENT 'Average number of rocksdb block read count',
  MAX_ROCKSDB_BLOCK_READ_COUNT int(11) unsigned NOT NULL COMMENT 'Max number of rocksdb block read count',
  AVG_ROCKSDB_BLOCK_READ_BYTE double unsigned NOT NULL COMMENT 'Average number of rocksdb block read byte',
  MAX_ROCKSDB_BLOCK_READ_BYTE int(11) unsigned NOT NULL COMMENT 'Max number of rocksdb block read byte',
  AVG_PREWRITE_TIME bigint(20) unsigned NOT NULL COMMENT 'Average time of prewrite phase',
  MAX_PREWRITE_TIME bigint(20) unsigned NOT NULL COMMENT 'Max time of prewrite phase',
  AVG_COMMIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Average time of commit phase',
  MAX_COMMIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Max time of commit phase',
  AVG_GET_COMMIT_TS_TIME bigint(20) unsigned NOT NULL COMMENT 'Average time of getting commit_ts',
  MAX_GET_COMMIT_TS_TIME bigint(20) unsigned NOT NULL COMMENT 'Max time of getting commit_ts',
  AVG_COMMIT_BACKOFF_TIME bigint(20) unsigned NOT NULL COMMENT 'Average time before retry during commit phase',
  MAX_COMMIT_BACKOFF_TIME bigint(20) unsigned NOT NULL COMMENT 'Max time before retry during commit phase',
  AVG_RESOLVE_LOCK_TIME bigint(20) unsigned NOT NULL COMMENT 'Average time for resolving locks',
  MAX_RESOLVE_LOCK_TIME bigint(20) unsigned NOT NULL COMMENT 'Max time for resolving locks',
  AVG_LOCAL_LATCH_WAIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Average waiting time of local transaction',
  MAX_LOCAL_LATCH_WAIT_TIME bigint(20) unsigned NOT NULL COMMENT 'Max waiting time of local transaction',
  AVG_WRITE_KEYS double unsigned NOT NULL COMMENT 'Average count of written keys',
  MAX_WRITE_KEYS bigint(20) unsigned NOT NULL COMMENT 'Max count of written keys',
  AVG_WRITE_SIZE double unsigned NOT NULL COMMENT 'Average amount of written bytes',
  MAX_WRITE_SIZE bigint(20) unsigned NOT NULL COMMENT 'Max amount of written bytes',
  AVG_PREWRITE_REGIONS double unsigned NOT NULL COMMENT 'Average number of involved regions in prewrite phase',
  MAX_PREWRITE_REGIONS int(11) unsigned NOT NULL COMMENT 'Max number of involved regions in prewrite phase',
  AVG_TXN_RETRY double unsigned NOT NULL COMMENT 'Average number of transaction retries',
  MAX_TXN_RETRY int(11) unsigned NOT NULL COMMENT 'Max number of transaction retries',
  SUM_EXEC_RETRY bigint(20) unsigned NOT NULL COMMENT 'Sum number of execution retries in pessimistic transactions',
  SUM_EXEC_RETRY_TIME bigint(20) unsigned NOT NULL COMMENT 'Sum time of execution retries in pessimistic transactions',
  SUM_BACKOFF_TIMES bigint(20) unsigned NOT NULL COMMENT 'Sum of retries',
  BACKOFF_TYPES varchar(1024) DEFAULT NULL COMMENT 'Types of errors and the number of retries for each type',
  AVG_MEM bigint(20) unsigned NOT NULL COMMENT 'Average memory(byte) used',
  MAX_MEM bigint(20) unsigned NOT NULL COMMENT 'Max memory(byte) used',
  AVG_DISK bigint(20) unsigned NOT NULL COMMENT 'Average disk space(byte) used',
  MAX_DISK bigint(20) unsigned NOT NULL COMMENT 'Max disk space(byte) used',
  AVG_KV_TIME bigint(22) unsigned NOT NULL COMMENT 'Average time of TiKV used',
  AVG_PD_TIME bigint(22) unsigned NOT NULL COMMENT 'Average time of PD used',
  AVG_BACKOFF_TOTAL_TIME bigint(22) unsigned NOT NULL COMMENT 'Average time of Backoff used',
  AVG_WRITE_SQL_RESP_TIME bigint(22) unsigned NOT NULL COMMENT 'Average time of write sql resp used',
  MAX_RESULT_ROWS bigint(22) NOT NULL COMMENT 'Max count of sql result rows',
  MIN_RESULT_ROWS bigint(22) NOT NULL COMMENT 'Min count of sql result rows',
  AVG_RESULT_ROWS bigint(22) NOT NULL COMMENT 'Average count of sql result rows',
  PREPARED tinyint(1) NOT NULL COMMENT 'Whether prepared',
  AVG_AFFECTED_ROWS double unsigned NOT NULL COMMENT 'Average number of rows affected',
  FIRST_SEEN timestamp NOT NULL COMMENT 'The time these statements are seen for the first time',
  LAST_SEEN timestamp NOT NULL COMMENT 'The time these statements are seen for the last time',
  PLAN_IN_CACHE tinyint(1) NOT NULL COMMENT 'Whether the last statement hit plan cache',
  PLAN_CACHE_HITS bigint(20) NOT NULL COMMENT 'The number of times these statements hit plan cache',
  PLAN_IN_BINDING tinyint(1) NOT NULL COMMENT 'Whether the last statement is matched with the hints in the binding',
  QUERY_SAMPLE_TEXT text DEFAULT NULL COMMENT 'Sampled original statement',
  PREV_SAMPLE_TEXT text DEFAULT NULL COMMENT 'The previous statement before commit',
  PLAN_DIGEST varchar(64) DEFAULT NULL COMMENT 'Digest of its execution plan',
  PLAN text DEFAULT NULL COMMENT 'Sampled execution plan',
  BINARY_PLAN text DEFAULT NULL COMMENT 'Sampled binary plan',
  CHARSET varchar(64) DEFAULT NULL COMMENT 'Sampled charset',
  COLLATION varchar(64) DEFAULT NULL COMMENT 'Sampled collation',
  PLAN_HINT varchar(64) DEFAULT NULL COMMENT 'Sampled plan hint',
  MAX_REQUEST_UNIT_READ double unsigned NOT NULL COMMENT 'Max read request-unit cost of these statements',
  AVG_REQUEST_UNIT_READ double unsigned NOT NULL COMMENT 'Average read request-unit cost of these statements',
  MAX_REQUEST_UNIT_WRITE double unsigned NOT NULL COMMENT 'Max write request-unit cost of these statements',
  AVG_REQUEST_UNIT_WRITE double unsigned NOT NULL COMMENT 'Average write request-unit cost of these statements',
  MAX_QUEUED_RC_TIME bigint(22) unsigned NOT NULL COMMENT 'Max time of waiting for available request-units',
  AVG_QUEUED_RC_TIME bigint(22) unsigned NOT NULL COMMENT 'Max time of waiting for available request-units',
  RESOURCE_GROUP varchar(64) DEFAULT NULL COMMENT 'Bind resource group name'
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`)
				generatePartitionDef(sb, "begin_time")
			},
		},
		{
			"hist_sp_tidb_index_usage",
			func(ctx context.Context, sb *strings.Builder) {
				fmt.Fprintf(sb,
					`CREATE TABLE IF NOT EXISTS hist_sp_tidb_index_usage (
  SNAP_ID int unsigned NOT NULL COMMENT 'Identifier of the snaoshot',
  BEGIN_TIME DATETIME NOT NULL,
  INSTANCE varchar(64) DEFAULT NULL,
  TABLE_SCHEMA varchar(64) DEFAULT NULL,
  TABLE_NAME varchar(64) DEFAULT NULL,
  INDEX_NAME varchar(64) DEFAULT NULL,
  QUERY_TOTAL bigint(21) DEFAULT NULL,
  KV_REQ_TOTAL bigint(21) DEFAULT NULL,
  ROWS_ACCESS_TOTAL bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_0 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_0_1 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_1_10 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_10_20 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_20_50 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_50_100 bigint(21) DEFAULT NULL,
  PERCENTAGE_ACCESS_100 bigint(21) DEFAULT NULL,
  LAST_ACCESS_TIME datetime DEFAULT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`)
				generatePartitionDef(sb, "begin_time")
			},
		},
		{
			"hist_processlist",
			func(ctx context.Context, sb *strings.Builder) {
				fmt.Fprintf(sb,
					`CREATE TABLE IF NOT EXISTS hist_processlist (
  TS datetime NOT NULL,
  INSTANCE varchar(64) DEFAULT NULL,
  ID bigint(21) unsigned NOT NULL DEFAULT '0',
  USER varchar(16) NOT NULL DEFAULT '',
  HOST varchar(64) NOT NULL DEFAULT '',
  DB varchar(64) DEFAULT NULL,
  COMMAND varchar(16) NOT NULL DEFAULT '',
  TIME int(7) NOT NULL DEFAULT '0',
  STATE varchar(7) DEFAULT NULL,
  INFO longtext DEFAULT NULL,
  DIGEST varchar(64) DEFAULT '',
  MEM bigint(21) unsigned DEFAULT NULL,
  DISK bigint(21) unsigned DEFAULT NULL,
  TxnStart varchar(64) NOT NULL DEFAULT '',
  RESOURCE_GROUP varchar(32) NOT NULL DEFAULT '',
  SESSION_ALIAS varchar(64) NOT NULL DEFAULT ''
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`)
				generatePartitionDef(sb, "ts")
			},
		},
		{
			"hist_tidb_trx",
			func(ctx context.Context, sb *strings.Builder) {
				fmt.Fprintf(sb,
					`CREATE TABLE IF NOT EXISTS hist_tidb_trx (
  TS datetime NOT NULL,
  INSTANCE varchar(64) DEFAULT NULL,
  ID bigint(21) unsigned NOT NULL,
  START_TIME timestamp(6) NULL DEFAULT NULL COMMENT 'Start time of the transaction',
  CURRENT_SQL_DIGEST varchar(64) DEFAULT NULL COMMENT 'Digest of the sql the transaction are currently running',
  CURRENT_SQL_DIGEST_TEXT text DEFAULT NULL COMMENT 'The normalized sql the transaction are currently running',
  STATE enum('Idle','Running','LockWaiting','Committing','RollingBack') DEFAULT NULL COMMENT 'Current running state of the transaction',
  WAITING_START_TIME timestamp(6) NULL DEFAULT NULL COMMENT 'Current lock waiting''s start time',
  MEM_BUFFER_KEYS bigint(64) DEFAULT NULL COMMENT 'How many entries are in MemDB',
  MEM_BUFFER_BYTES bigint(64) DEFAULT NULL COMMENT 'MemDB used memory',
  SESSION_ID bigint(21) unsigned DEFAULT NULL COMMENT 'Which session this transaction belongs to',
  USER varchar(16) DEFAULT NULL COMMENT 'The user who open this session',
  DB varchar(64) DEFAULT NULL COMMENT 'The schema this transaction works on',
  ALL_SQL_DIGESTS text DEFAULT NULL COMMENT 'A list of the digests of SQL statements that the transaction has executed',
  RELATED_TABLE_IDS text DEFAULT NULL COMMENT 'A list of the table IDs that the transaction has accessed',
  WAITING_TIME double DEFAULT NULL COMMENT 'Current lock waiting time'
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`)
				generatePartitionDef(sb, "ts")
			},
		},
	}
)

func (w *Worker) createAllTables(ctx context.Context) error {
	_sessctx := w.getSessionWithRetry()
	exec := _sessctx.(sqlexec.SQLExecutor)
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	if !is.SchemaExists(workloadSchemaCIStr) {
		_, err := exec.ExecuteInternal(ctx, "create database if not exists "+WorkloadSchema)
		if err != nil {
			return err
		}
	}

	if _, err := exec.ExecuteInternal(ctx, "use "+WorkloadSchema); err != nil {
		return err
	}
	sb := &strings.Builder{}
	for _, tbl := range tbls {
		if w.checkTableExistsByIS(ctx, is, tbl.name, zeroTime) {
			continue
		}
		sb.Reset()
		tbl.createFn(ctx, sb)
		if err := execRetry(ctx, exec, sb.String()); err != nil {
			return err
		}
	}
	return w.createAllPartitions(ctx, exec, is)
}

func (w *Worker) checkTablesExists(ctx context.Context) bool {
	_sessctx := w.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	now := time.Now()
	return slice.AllOf(tbls, func(i int) bool {
		return w.checkTableExistsByIS(ctx, is, tbls[i].name, now)
	})
}

func (w *Worker) checkTableExistsByIS(ctx context.Context, is infoschema.InfoSchema, tblName string, now time.Time) bool {
	if now == zeroTime {
		return is.TableExists(workloadSchemaCIStr, model.NewCIStr(tblName))
	}

	// check for partitions, too
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tblName))
	if err != nil {
		return false
	}

	tbInfo := tbSchema.Meta()
	for i := 0; i < 2; i++ {
		newPtTime := now.AddDate(0, 0, i+1)
		newPtName := "p" + newPtTime.Format("20060102")
		ptInfos := tbInfo.GetPartitionInfo().Definitions
		if slice.NoneOf(ptInfos, func(i int) bool {
			return ptInfos[i].Name.L == newPtName
		}) {
			return false
		}
	}
	return true
}
