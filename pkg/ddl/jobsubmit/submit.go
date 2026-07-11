// Copyright 2026 PingCAP, Inc.
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

package jobsubmit

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/bdr"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// SubmitBatch validates and inserts DDL jobs into mysql.tidb_ddl_job without
// starting any background submit loop, notification path, or history waiter.
// It mutates the input specs in place: jobs receive their IDs, StartTS, BDR
// role, and state. Upgrade-time submission may also set AdminOperator while
// pausing user jobs. Callers must not treat the input specs as read-only.
func SubmitBatch(ctx context.Context, opts SubmitOptions, specs []*JobSpec) error {
	if len(specs) == 0 {
		return nil
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	se, err := opts.SessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer opts.SessPool.Put(se)

	minJobID := opts.MinJobIDRefresher.GetCurrMinJobID()
	found, err := opts.SysTblMgr.HasFlashbackClusterJob(ctx, minJobID)
	if err != nil {
		return errors.Trace(err)
	}
	if found {
		return errors.Errorf("Can't add ddl job, have flashback cluster job")
	}

	var (
		startTS = uint64(0)
		bdrRole = string(ast.BDRRoleNone)
	)
	err = kv.RunInNewTxn(ctx, opts.Store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		bdrRole, err = t.GetBDRRole()
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, spec := range specs {
		job := spec.Job
		job.NormalizeInvolvingSchemaInfo()
		if err = job.CheckInvolvingSchemaInfo(); err != nil {
			return err
		}
		intest.Assert(job.Version != 0, "Job version should not be zero")
		if job.TraceInfo == nil {
			// job scheduler expects TraceInfo to be non-nil.
			job.TraceInfo = &tracing.TraceInfo{}
		}
		job.StartTS = startTS
		job.BDRRole = bdrRole

		// BDR mode only affects the DDL not from CDC.
		if job.CDCWriteSource == 0 && bdrRole != string(ast.BDRRoleNone) {
			if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
				for _, subJob := range job.MultiSchemaInfo.SubJobs {
					if bdr.IsDenied(ast.BDRRole(bdrRole), subJob.Type, subJob.JobArgs) && !filter.IsSystemSchema(job.SchemaName) {
						return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
					}
				}
			} else if bdr.IsDenied(ast.BDRRole(bdrRole), job.Type, spec.Args) && !filter.IsSystemSchema(job.SchemaName) {
				return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
			}
		}

		setJobStateToQueueing(job)

		if opts.ServerStateSyncer != nil && opts.ServerStateSyncer.IsUpgradingState() && !ddlutil.HasSysDB(job) {
			if err = ddlutil.PauseRunningJob(job, model.AdminCommandBySystem); err != nil {
				logutil.DDLUpgradingLogger().Warn("pause user DDL by system failed", zap.Stringer("job", job), zap.Error(err))
				return err
			}
			logutil.DDLUpgradingLogger().Info("pause user DDL by system successful", zap.Stringer("job", job))
		}
	}

	ddlSe := sess.NewSession(se)
	if err = GenGIDAndInsertJobsWithRetry(ctx, ddlSe, specs, opts.BeforeInsertWithAssignedIDs); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// GenGIDAndInsertJobsWithRetry generates job-related global IDs and inserts DDL
// jobs to the DDL job table with retry. Job ID allocation and job insertion are
// in the same transaction, as we want to make sure DDL jobs are inserted in ID
// order, then we can query from a min job ID when scheduling DDL jobs to
// mitigate https://github.com/pingcap/tidb/issues/52905. So this function has
// side effects, it will set table/db/job ID of the jobs in specs.
func GenGIDAndInsertJobsWithRetry(
	ctx context.Context,
	ddlSe *sess.Session,
	specs []*JobSpec,
	beforeInsertWithAssignedIDs func(specs []*JobSpec) (cleanup func()),
) error {
	count := getRequiredGIDCount(specs)
	var resErr error
	for i := range kv.MaxRetryCnt {
		resErr = func() (err error) {
			if err := ddlSe.Begin(ctx); err != nil {
				return errors.Trace(err)
			}
			var cleanup func()
			defer func() {
				if err != nil {
					if cleanup != nil {
						cleanup()
					}
					ddlSe.Rollback()
				}
			}()
			txn, err := ddlSe.Txn()
			if err != nil {
				return errors.Trace(err)
			}
			txn.SetOption(kv.Pessimistic, true)
			forUpdateTS, err := lockGlobalIDKey(ctx, ddlSe, txn)
			if err != nil {
				return errors.Trace(err)
			}
			txn.GetSnapshot().SetOption(kv.SnapshotTS, forUpdateTS)

			m := meta.NewMutator(txn)
			ids, err := m.GenGlobalIDs(count)
			if err != nil {
				return errors.Trace(err)
			}
			failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
				if val.(bool) {
					failpoint.Return(errors.New("gofail genGlobalIDs error"))
				}
			})
			assignGIDsForJobs(specs, ids)

			if beforeInsertWithAssignedIDs != nil {
				cleanup = beforeInsertWithAssignedIDs(specs)
			}
			failpoint.Inject("mockGenGIDRetryableError", func() {
				failpoint.Return(kv.ErrTxnRetryable)
			})
			if err = insertDDLJobs2Table(ctx, ddlSe, specs...); err != nil {
				return errors.Trace(err)
			}
			if err = ddlSe.Commit(ctx); err != nil {
				return errors.Trace(err)
			}
			cleanup = nil
			return nil
		}()

		if resErr != nil && kv.IsTxnRetryableError(resErr) {
			logutil.DDLLogger().Warn("insert job meet retryable error", zap.Error(resErr))
			kv.BackOff(i)
			failpoint.InjectCall("onGenGIDRetry")
			continue
		}
		break
	}
	return resErr
}

type gidAllocator struct {
	idx int
	ids []int64
}

func (a *gidAllocator) next() int64 {
	id := a.ids[a.idx]
	a.idx++
	return id
}

func (a *gidAllocator) assignIDsForTable(info *model.TableInfo) {
	info.ID = a.next()
	if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
		a.assignIDsForPartitionInfo(partitionInfo)
	}
}

func (a *gidAllocator) assignIDsForPartitionInfo(partitionInfo *model.PartitionInfo) {
	for i := range partitionInfo.Definitions {
		partitionInfo.Definitions[i].ID = a.next()
	}
}

func idCountForTable(info *model.TableInfo) int {
	c := 1
	if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
		c += len(partitionInfo.Definitions)
	}
	return c
}

// getRequiredGIDCount returns the count of required global IDs for the jobs. It's calculated
// as: the count of jobs + the count of IDs for the jobs which do NOT have pre-allocated ID.
func getRequiredGIDCount(specs []*JobSpec) int {
	count := len(specs)
	for _, spec := range specs {
		if spec.IDAllocated {
			continue
		}
		switch spec.Job.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			args := spec.Args.(*model.CreateTableArgs)
			count += idCountForTable(args.TableInfo)
		case model.ActionCreateTables:
			args := spec.Args.(*model.BatchCreateTableArgs)
			for _, tblArgs := range args.Tables {
				count += idCountForTable(tblArgs.TableInfo)
			}
		case model.ActionCreateSchema, model.ActionCreateResourceGroup:
			count++
		case model.ActionAlterTablePartitioning:
			args := spec.Args.(*model.TablePartitionArgs)
			// A new table ID would be needed for
			// the global table, which cannot be the same as the current table id,
			// since this table id will be removed in the final state when removing
			// all the data with this table id.
			count += 1 + len(args.PartInfo.Definitions)
		case model.ActionTruncateTablePartition:
			count += len(spec.Args.(*model.TruncateTableArgs).OldPartitionIDs)
		case model.ActionAddTablePartition, model.ActionReorganizePartition, model.ActionRemovePartitioning:
			args := spec.Args.(*model.TablePartitionArgs)
			count += len(args.PartInfo.Definitions)
		case model.ActionTruncateTable:
			count += 1 + len(spec.Args.(*model.TruncateTableArgs).OldPartitionIDs)
		}
	}
	return count
}

// assignGIDsForJobs should be used with getRequiredGIDCount, and len(ids) must equal
// what getRequiredGIDCount returns.
func assignGIDsForJobs(specs []*JobSpec, ids []int64) {
	alloc := &gidAllocator{ids: ids}
	for _, spec := range specs {
		switch spec.Job.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			args := spec.Args.(*model.CreateTableArgs)
			if !spec.IDAllocated {
				alloc.assignIDsForTable(args.TableInfo)
			}
			spec.Job.TableID = args.TableInfo.ID
		case model.ActionCreateTables:
			if !spec.IDAllocated {
				args := spec.Args.(*model.BatchCreateTableArgs)
				for _, tblArgs := range args.Tables {
					alloc.assignIDsForTable(tblArgs.TableInfo)
				}
			}
		case model.ActionCreateSchema:
			dbInfo := spec.Args.(*model.CreateSchemaArgs).DBInfo
			if !spec.IDAllocated {
				dbInfo.ID = alloc.next()
			}
			spec.Job.SchemaID = dbInfo.ID
		case model.ActionCreateResourceGroup:
			if !spec.IDAllocated {
				args := spec.Args.(*model.ResourceGroupArgs)
				args.RGInfo.ID = alloc.next()
			}
		case model.ActionAlterTablePartitioning:
			if !spec.IDAllocated {
				args := spec.Args.(*model.TablePartitionArgs)
				alloc.assignIDsForPartitionInfo(args.PartInfo)
				args.PartInfo.NewTableID = alloc.next()
			}
		case model.ActionAddTablePartition, model.ActionReorganizePartition:
			if !spec.IDAllocated {
				pInfo := spec.Args.(*model.TablePartitionArgs).PartInfo
				alloc.assignIDsForPartitionInfo(pInfo)
			}
		case model.ActionRemovePartitioning:
			// A special partition is used in this case, and we will use the ID
			// of the partition as the new table ID.
			pInfo := spec.Args.(*model.TablePartitionArgs).PartInfo
			if !spec.IDAllocated {
				alloc.assignIDsForPartitionInfo(pInfo)
			}
			pInfo.NewTableID = pInfo.Definitions[0].ID
		case model.ActionTruncateTable, model.ActionTruncateTablePartition:
			if !spec.IDAllocated {
				args := spec.Args.(*model.TruncateTableArgs)
				if spec.Job.Type == model.ActionTruncateTable {
					args.NewTableID = alloc.next()
				}
				partIDs := make([]int64, len(args.OldPartitionIDs))
				for i := range partIDs {
					partIDs[i] = alloc.next()
				}
				args.NewPartitionIDs = partIDs
			}
		}
		spec.Job.ID = alloc.next()
	}
}

// lockGlobalIDKey locks the global ID key in the meta store. It keeps retrying
// on write conflicts because we cannot have a fixed retry count for this error;
// see https://github.com/pingcap/tidb/issues/27197#issuecomment-2216315057.
// This is the same as how we implement pessimistic + repeatable read isolation
// level in SQL executor, see doLockKeys.
// NextGlobalID is a meta key, so we cannot use "select xx for update". If we store
// it into a table row or use an advisory lock, we will depend on a system table
// that is created by us, which is cyclic. Although we can create a system table
// without using DDL logic, keep it this way until we have a data dictionary.
// TODO maybe we can unify the lock mechanism with SQL executor in the future, or
// implement it inside TiKV client-go.
func lockGlobalIDKey(ctx context.Context, ddlSe *sess.Session, txn kv.Transaction) (uint64, error) {
	var (
		iteration   uint
		forUpdateTs = txn.StartTS()
		ver         kv.Version
		err         error
	)
	waitTime := ddlSe.GetSessionVars().LockWaitTimeout
	m := meta.NewMutator(txn)
	idKey := m.GlobalIDKey()
	for {
		lockCtx := tikv.NewLockCtx(forUpdateTs, waitTime, time.Now())
		err = txn.LockKeys(ctx, lockCtx, idKey)
		if err == nil || !terror.ErrorEqual(kv.ErrWriteConflict, err) {
			break
		}
		// ErrWriteConflict contains a conflict-commit-ts in most case, but it cannot
		// be used as forUpdateTs, see comments inside handleAfterPessimisticLockError.
		ver, err = ddlSe.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			break
		}
		forUpdateTs = ver.Ver

		kv.BackOff(iteration)
		// Avoid it keep growing and overflow.
		iteration = min(iteration+1, math.MaxInt)
	}
	return forUpdateTs, err
}

func insertDDLJobs2Table(ctx context.Context, se *sess.Session, specs ...*JobSpec) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(specs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString("insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values")
	for i, spec := range specs {
		fillArgsWithSubJobs(spec)
		b, err := spec.Job.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		fmt.Fprintf(&sql, "(%d, %t, %s, %s, %s, %d, %t)", spec.Job.ID, spec.Job.MayNeedReorg(),
			strconv.Quote(job2SchemaIDs(spec)), strconv.Quote(job2TableIDs(spec)),
			ddlutil.WrapKey2String(b), spec.Job.Type, spec.Job.Started())
	}
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.DDLLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func fillArgsWithSubJobs(spec *JobSpec) {
	if spec.Job.Type != model.ActionMultiSchemaChange {
		spec.Job.FillArgs(spec.Args)
		return
	}
	for _, sub := range spec.Job.MultiSchemaInfo.SubJobs {
		sub.FillArgs(spec.Job.Version)
	}
}

func makeStringForIDs(ids []int64) string {
	set := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}

	s := make([]string, 0, len(set))
	for id := range set {
		s = append(s, strconv.FormatInt(id, 10))
	}
	slices.Sort(s)
	return strings.Join(s, ",")
}

func job2SchemaIDs(spec *JobSpec) string {
	switch spec.Job.Type {
	case model.ActionRenameTables:
		arg := spec.Args.(*model.RenameTablesArgs)
		ids := make([]int64, 0, len(arg.RenameTableInfos)*2)
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.OldSchemaID, info.NewSchemaID)
		}
		return makeStringForIDs(ids)
	case model.ActionRenameTable:
		oldSchemaID := spec.Args.(*model.RenameTableArgs).OldSchemaID
		ids := []int64{oldSchemaID, spec.Job.SchemaID}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		args := spec.Args.(*model.ExchangeTablePartitionArgs)
		return makeStringForIDs([]int64{spec.Job.SchemaID, args.PTSchemaID})
	default:
		return strconv.FormatInt(spec.Job.SchemaID, 10)
	}
}

func job2TableIDs(spec *JobSpec) string {
	switch spec.Job.Type {
	case model.ActionRenameTables:
		arg := spec.Args.(*model.RenameTablesArgs)
		ids := make([]int64, 0, len(arg.RenameTableInfos))
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.TableID)
		}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		args := spec.Args.(*model.ExchangeTablePartitionArgs)
		return makeStringForIDs([]int64{spec.Job.TableID, args.PTTableID})
	case model.ActionTruncateTable:
		newTableID := spec.Args.(*model.TruncateTableArgs).NewTableID
		return strconv.FormatInt(spec.Job.TableID, 10) + "," + strconv.FormatInt(newTableID, 10)
	default:
		return strconv.FormatInt(spec.Job.TableID, 10)
	}
}

// setJobStateToQueueing marks a job and its sub-jobs as queueing.
func setJobStateToQueueing(job *model.Job) {
	if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			sub.State = model.JobStateQueueing
		}
	}
	job.State = model.JobStateQueueing
}

// NotifyDDLOwnerByEtcd notifies the DDL owner to pick up new DDL jobs by etcd.
func NotifyDDLOwnerByEtcd(ctx context.Context, etcdCli *clientv3.Client) {
	if etcdCli == nil {
		return
	}

	err := ddlutil.PutKVToEtcd(ctx, etcdCli, 1, ddlutil.AddingDDLJobNotifyKey, "0")
	if err != nil {
		logutil.DDLLogger().Info("notify new DDL job failed", zap.Error(err))
	}
}
