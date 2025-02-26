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

package ingest

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// ResignOwnerForTest is only used for test.
var ResignOwnerForTest = atomic.NewBool(false)

// NewBackendCtxBuilder creates a BackendCtxBuilder.
func NewBackendCtxBuilder(ctx context.Context, store kv.Storage, job *model.Job) *BackendCtxBuilder {
	return &BackendCtxBuilder{
		ctx:   ctx,
		store: store,
		job:   job,
	}
}

// BackendCtxBuilder is the builder of BackendCtx.
type BackendCtxBuilder struct {
	ctx   context.Context
	store kv.Storage
	job   *model.Job

	etcdClient *clientv3.Client
	importTS   uint64

	sessPool   *sess.Pool
	physicalID int64
	checkDup   bool
}

// WithImportDistributedLock needs a etcd client to maintain a distributed lock during partial import.
func (b *BackendCtxBuilder) WithImportDistributedLock(etcdCli *clientv3.Client, importTS uint64) *BackendCtxBuilder {
	b.etcdClient = etcdCli
	b.importTS = importTS
	return b
}

// WithCheckpointManagerParam only is used by non-DXF local ingest mode.
func (b *BackendCtxBuilder) WithCheckpointManagerParam(
	sessPool *sess.Pool,
	physicalID int64,
) *BackendCtxBuilder {
	b.sessPool = sessPool
	b.physicalID = physicalID
	return b
}

// ForDuplicateCheck marks this backend context is only used for duplicate check.
// TODO(tangenta): remove this after we don't rely on the backend to do duplicate check.
func (b *BackendCtxBuilder) ForDuplicateCheck() *BackendCtxBuilder {
	b.checkDup = true
	return b
}

// BackendCounterForTest is only used in test.
var BackendCounterForTest = atomic.Int64{}

// Build builds a BackendCtx.
func (b *BackendCtxBuilder) Build(cfg *local.BackendConfig, bd *local.Backend) (BackendCtx, error) {
	ctx, store, job := b.ctx, b.store, b.job
	jobSortPath, err := genJobSortPath(job.ID, b.checkDup)
	if err != nil {
		return nil, err
	}
	intest.Assert(job.Type == model.ActionAddPrimaryKey ||
		job.Type == model.ActionAddIndex)
	intest.Assert(job.ReorgMeta != nil)

	failpoint.Inject("beforeCreateLocalBackend", func() {
		ResignOwnerForTest.Store(true)
	})

	//nolint: forcetypeassert
	pdCli := store.(tikv.Storage).GetRegionCache().PDClient()
	var cpMgr *CheckpointManager
	if b.sessPool != nil {
		cpMgr, err = NewCheckpointManager(ctx, b.sessPool, b.physicalID, job.ID, jobSortPath, pdCli)
		if err != nil {
			logutil.Logger(ctx).Warn("create checkpoint manager failed",
				zap.Int64("jobID", job.ID),
				zap.Error(err))
			return nil, err
		}
	}

	var mockBackend BackendCtx
	failpoint.InjectCall("mockNewBackendContext", b.job, cpMgr, &mockBackend)
	if mockBackend != nil {
		BackendCounterForTest.Inc()
		return mockBackend, nil
	}

	bCtx := newBackendContext(ctx, job.ID, bd, cfg,
		defaultImportantVariables, LitMemRoot, b.etcdClient, job.RealStartTS, b.importTS, cpMgr)

	LitDiskRoot.Add(job.ID, bCtx)
	BackendCounterForTest.Add(1)
	return bCtx, nil
}

func genJobSortPath(jobID int64, checkDup bool) (string, error) {
	sortPath, err := GenIngestTempDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(sortPath, encodeBackendTag(jobID, checkDup)), nil
}

// CreateLocalBackend creates a local backend for adding index.
func CreateLocalBackend(ctx context.Context, store kv.Storage, job *model.Job, checkDup bool) (*local.BackendConfig, *local.Backend, error) {
	jobSortPath, err := genJobSortPath(job.ID, checkDup)
	if err != nil {
		return nil, nil, err
	}
	intest.Assert(job.Type == model.ActionAddPrimaryKey ||
		job.Type == model.ActionAddIndex)
	intest.Assert(job.ReorgMeta != nil)

	resGroupName := job.ReorgMeta.ResourceGroupName
	concurrency := job.ReorgMeta.GetConcurrency()
	maxWriteSpeed := job.ReorgMeta.GetMaxWriteSpeed()
	hasUnique, err := hasUniqueIndex(job)
	if err != nil {
		return nil, nil, err
	}
	cfg := genConfig(ctx, jobSortPath, LitMemRoot, hasUnique, resGroupName, concurrency, maxWriteSpeed)

	tidbCfg := config.GetGlobalConfig()
	tls, err := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort))),
		nil, nil, nil,
	)
	if err != nil {
		logutil.Logger(ctx).Error(LitErrCreateBackendFail, zap.Error(err))
		return nil, nil, err
	}

	ddllogutil.DDLIngestLogger().Info("create local backend for adding index",
		zap.String("sortDir", cfg.LocalStoreDir),
		zap.String("keyspaceName", cfg.KeyspaceName),
		zap.Int64("job ID", job.ID),
		zap.Int64("current memory usage", LitMemRoot.CurrentUsage()),
		zap.Int64("max memory quota", LitMemRoot.MaxMemoryQuota()),
		zap.Bool("has unique index", hasUnique))

	//nolint: forcetypeassert
	pdCli := store.(tikv.Storage).GetRegionCache().PDClient()
	be, err := local.NewBackend(ctx, tls, *cfg, pdCli.GetServiceDiscovery())
	return cfg, be, err
}

func hasUniqueIndex(job *model.Job) (bool, error) {
	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		return false, errors.Trace(err)
	}

	for _, a := range args.IndexArgs {
		if a.Unique {
			return true, nil
		}
	}
	return false, nil
}

const checkpointUpdateInterval = 10 * time.Minute

func newBackendContext(
	ctx context.Context,
	jobID int64,
	be *local.Backend,
	cfg *local.BackendConfig,
	vars map[string]string,
	memRoot MemRoot,
	etcdClient *clientv3.Client,
	initTS, importTS uint64,
	cpMgr *CheckpointManager,
) *litBackendCtx {
	bCtx := &litBackendCtx{
		engines:        make(map[int64]*engineInfo, 10),
		memRoot:        memRoot,
		jobID:          jobID,
		backend:        be,
		ctx:            ctx,
		cfg:            cfg,
		sysVars:        vars,
		updateInterval: checkpointUpdateInterval,
		etcdClient:     etcdClient,
		initTS:         initTS,
		importTS:       importTS,
		checkpointMgr:  cpMgr,
	}
	bCtx.timeOfLastFlush.Store(time.Now())
	return bCtx
}

// encodeBackendTag encodes the job ID to backend tag.
// The backend tag is also used as the file name of the local index data files.
func encodeBackendTag(jobID int64, checkDup bool) string {
	if checkDup {
		return fmt.Sprintf("%d-dup", jobID)
	}
	return strconv.FormatInt(jobID, 10)
}

// decodeBackendTag decodes the backend tag to job ID.
func decodeBackendTag(name string) (int64, error) {
	return strconv.ParseInt(name, 10, 64)
}
