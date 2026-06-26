// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/jobsubmit"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const ddlHistoryPollInterval = 100 * time.Millisecond

type ddlClient struct {
	store     kv.Storage
	etcdCli   *clientv3.Client
	opts      jobsubmit.SubmitOptions
	sampleLog *zap.Logger
}

func newDDLClient(
	store kv.Storage,
	etcdCli *clientv3.Client,
	opts jobsubmit.SubmitOptions,
) *ddlClient {
	sampleLog := logutil.SampleErrVerboseLoggerFactory(time.Minute, 3, zap.String("target-keyspace", store.GetKeyspace()))()
	return &ddlClient{
		store:     store,
		etcdCli:   etcdCli,
		opts:      opts,
		sampleLog: sampleLog,
	}
}

func (c *ddlClient) alterTableMode(
	ctx context.Context,
	req model.AlterTableModeTarget,
) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	target, err := c.resolveAlterTableModeTarget(ctx, req)
	if err != nil {
		return errors.Trace(err)
	}

	job, args, noop, err := c.buildAlterTableModeJob(target)
	if err != nil {
		return errors.Trace(err)
	}
	if noop {
		return nil
	}
	if err = c.refreshServerState(ctx); err != nil {
		return errors.Trace(err)
	}

	if err = jobsubmit.SubmitBatch(ctx, c.opts, []*jobsubmit.JobSpec{{Job: job, Args: args}}); err != nil {
		return errors.Trace(err)
	}

	jobsubmit.NotifyDDLOwnerByEtcd(ctx, c.etcdCli)
	return errors.Trace(c.waitDDLFinished(ctx, job.ID))
}

func (*ddlClient) Close() {}

func (c *ddlClient) buildAlterTableModeJob(
	target model.AlterTableModeTarget,
) (*model.Job, model.JobArgs, bool, error) {
	sctx, err := c.opts.SessPool.Get()
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}
	defer c.opts.SessPool.Put(sctx)

	return jobsubmit.BuildAlterTableModeJob(sctx, target)
}

func (c *ddlClient) refreshServerState(ctx context.Context) error {
	if c.opts.ServerStateSyncer == nil {
		return nil
	}
	_, err := c.opts.ServerStateSyncer.GetGlobalState(ctx)
	return errors.Trace(err)
}

func (c *ddlClient) resolveAlterTableModeTarget(
	ctx context.Context,
	req model.AlterTableModeTarget,
) (model.AlterTableModeTarget, error) {
	var (
		dbInfo  *model.DBInfo
		tblInfo *model.TableInfo
	)
	err := kv.RunInNewTxn(ctx, c.store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewReader(txn)
		var err error
		dbInfo, err = m.GetDatabase(req.SchemaID)
		if err != nil {
			return errors.Trace(err)
		}
		if dbInfo == nil {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("(Schema ID %d)", req.SchemaID))
		}
		tblInfo, err = m.GetTable(req.SchemaID, req.TableID)
		if err != nil {
			return errors.Trace(err)
		}
		if tblInfo == nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
				fmt.Sprintf("(Table ID %d)", req.TableID),
			)
		}
		return nil
	})
	if err != nil {
		return model.AlterTableModeTarget{}, errors.Trace(err)
	}

	// below checks shouldn't happen in normal execution path, but if we add a
	// fallback restore table mode mechanism to end user, it might.
	if req.SchemaName.L != dbInfo.Name.L {
		return model.AlterTableModeTarget{}, errors.Errorf(
			"expected schema name %s does not match target schema name %s",
			req.SchemaName.O, dbInfo.Name.O)
	}
	if req.TableName.L != tblInfo.Name.L {
		return model.AlterTableModeTarget{}, errors.Errorf(
			"expected table name %s does not match target table name %s",
			req.TableName.O, tblInfo.Name.O)
	}

	return model.AlterTableModeTarget{
		SchemaID:    req.SchemaID,
		SchemaName:  req.SchemaName,
		TableID:     req.TableID,
		TableName:   req.TableName,
		CurrentMode: tblInfo.Mode,
		TargetMode:  req.TargetMode,
	}, nil
}

// this method is a simplified version of wait logic inside DoDDLJobWrapper.
func (c *ddlClient) waitDDLFinished(ctx context.Context, jobID int64) error {
	ticker := time.NewTicker(ddlHistoryPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		historyJob, err := c.getHistoryJob(ctx, jobID)
		if err != nil {
			c.sampleLog.Warn("get target DDL history failed, retrying",
				zap.Int64("jobID", jobID), zap.Error(err))
		} else if historyJob != nil {
			if historyJob.IsSynced() {
				return nil
			}
			if historyJob.Error != nil {
				return errors.Trace(historyJob.Error)
			}
			// should not happen.
			return errors.Errorf("target DDL job %d finished in unexpected state %s", jobID, historyJob.State)
		}
	}
}

func (c *ddlClient) getHistoryJob(ctx context.Context, jobID int64) (*model.Job, error) {
	var job *model.Job
	err := kv.RunInNewTxn(ctx, c.store, false, func(_ context.Context, txn kv.Transaction) error {
		var err error
		job, err = meta.NewReader(txn).GetHistoryDDLJob(jobID)
		return errors.Trace(err)
	})
	return job, errors.Trace(err)
}
