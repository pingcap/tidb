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

package logclient

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/encryption"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/internal/rawkv"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/br/pkg/version"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/keepalive"
)

const MetaKVBatchSize = 64 * 1024 * 1024
const maxSplitKeysOnce = 10240

// rawKVBatchCount specifies the count of entries that the rawkv client puts into TiKV.
const rawKVBatchCount = 64

// session count for repairing ingest indexes. Currently only one TiDB node executes adding index jobs
// at the same time and the add-index job concurrency is about min(10, `TiDB CPUs / 4`).
const defaultRepairIndexSessionCount uint = 10

// LogRestoreManager is a comprehensive wrapper that encapsulates all logic related to log restoration,
// including concurrency management, checkpoint handling, and file importing for efficient log processing.
type LogRestoreManager struct {
	fileImporter     *LogFileImporter
	workerPool       *tidbutil.WorkerPool
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType]
}

func NewLogRestoreManager(
	ctx context.Context,
	fileImporter *LogFileImporter,
	poolSize uint,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (*LogRestoreManager, error) {
	// for compacted reason, user only set --concurrency for log file restore speed.
	log.Info("log restore worker pool", zap.Uint("size", poolSize))
	l := &LogRestoreManager{
		fileImporter: fileImporter,
		workerPool:   tidbutil.NewWorkerPool(poolSize, "log manager worker pool"),
	}

	if logCheckpointMetaManager != nil {
		log.Info("starting checkpoint runner for log restore")
		var err error
		l.checkpointRunner, err = checkpoint.StartCheckpointRunnerForLogRestore(ctx, logCheckpointMetaManager)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		log.Info("log checkpoint meta manager is disabled, checkpoint runner not started")
	}
	return l, nil
}

func (l *LogRestoreManager) Close(ctx context.Context) {
	if l.fileImporter != nil {
		if err := l.fileImporter.Close(); err != nil {
			log.Warn("failed to close file importer")
		}
	}
	if l.checkpointRunner != nil {
		l.checkpointRunner.WaitForFinish(ctx, true)
	}
}

// SstRestoreManager is a comprehensive wrapper that encapsulates all logic related to sst restoration,
// including concurrency management, checkpoint handling, and file importing(splitting) for efficient log processing.
type SstRestoreManager struct {
	restorer         restore.SstRestorer
	workerPool       *tidbutil.WorkerPool
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
}

func (s *SstRestoreManager) Close(ctx context.Context) {
	if s.restorer != nil {
		if err := s.restorer.Close(); err != nil {
			log.Warn("failed to close file restorer")
		}
	}
	if s.checkpointRunner != nil {
		s.checkpointRunner.WaitForFinish(ctx, true)
	}
}

func NewSstRestoreManager(
	ctx context.Context,
	metaClient split.SplitClient,
	snapFileImporter *snapclient.SnapFileImporter,
	concurrencyPerStore uint,
	storeCount uint,
	sstCheckpointMetaManager checkpoint.SnapshotMetaManagerT,
) (*SstRestoreManager, error) {
	var checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
	// This poolSize is similar to full restore, as both workflows are comparable.
	// The poolSize should be greater than concurrencyPerStore multiplied by the number of stores.
	poolSize := concurrencyPerStore * 32 * storeCount
	log.Info("sst restore worker pool", zap.Uint("size", poolSize))
	sstWorkerPool := tidbutil.NewWorkerPool(poolSize, "sst file")

	s := &SstRestoreManager{
		workerPool: tidbutil.NewWorkerPool(poolSize, "log manager worker pool"),
	}
	if sstCheckpointMetaManager != nil {
		var err error
		checkpointRunner, err = checkpoint.StartCheckpointRunnerForRestore(ctx, sstCheckpointMetaManager)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if snapFileImporter.GetMergeSst() {
		log.Info("create batch sst restorer to restore SST files")
		s.restorer = restore.NewBatchSstRestorer(ctx, snapFileImporter, metaClient, sstWorkerPool, checkpointRunner)
	} else {
		log.Info("create simple sst restorer to restore SST files")
		s.restorer = restore.NewSimpleSstRestorer(ctx, snapFileImporter, sstWorkerPool, checkpointRunner)
	}
	return s, nil
}

type LogClient struct {
	*LogFileManager

	logRestoreManager *LogRestoreManager
	sstRestoreManager *SstRestoreManager

	cipher        *backuppb.CipherInfo
	pdClient      pd.Client
	pdHTTPClient  pdhttp.Client
	clusterID     uint64
	dom           *domain.Domain
	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters

	rawKVClient *rawkv.RawKVBatchClient
	storage     storeapi.Storage

	// unsafeSession is not thread-safe.
	// Currently, it is only utilized in some initialization and post-handle functions.
	unsafeSession glue.Session

	// currentTS is used for rewrite meta kv when restore stream.
	// Can not use `restoreTS` directly, because schema created in `full backup` maybe is new than `restoreTS`.
	currentTS uint64

	upstreamClusterID uint64
	restoreID         uint64

	// the query to insert rows into table `gc_delete_range`, lack of ts.
	deleteRangeQuery          []*stream.PreDelRangeQuery
	deleteRangeQueryCh        chan *stream.PreDelRangeQuery
	deleteRangeQueryWaitGroup sync.WaitGroup

	// checkpoint information for log restore
	useCheckpoint bool

	logFilesStat logFilesStatistic
	restoreStat  restoreStatistics
}

func (rc *LogClient) SetRestoreID(restoreID uint64) {
	rc.restoreID = restoreID
}

type restoreStatistics struct {
	// restoreSSTKVSize is the total size (Original KV length) of KV pairs restored from SST files.
	restoreSSTKVSize uint64
	// restoreSSTKVCount is the total number of KV pairs restored from SST files.
	restoreSSTKVCount uint64
	// restoreSSTPhySize is the total size of SST files after encoding to SST files.
	// this may be smaller than kv length due to compression or common prefix optimization.
	restoreSSTPhySize uint64
	// restoreSSTTakes is the total time taken for restoring SST files.
	// the unit is nanoseconds, hence it can be converted between `time.Duration` directly.
	restoreSSTTakes uint64
}

// NewLogClient returns a new LogClient.
func NewLogClient(
	pdClient pd.Client,
	pdHTTPCli pdhttp.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
) *LogClient {
	return &LogClient{
		pdClient:           pdClient,
		pdHTTPClient:       pdHTTPCli,
		tlsConf:            tlsConf,
		keepaliveConf:      keepaliveConf,
		deleteRangeQuery:   make([]*stream.PreDelRangeQuery, 0),
		deleteRangeQueryCh: make(chan *stream.PreDelRangeQuery, 10),
	}
}

// Close a client.
func (rc *LogClient) Close(ctx context.Context) {
	// close the connection, and it must be succeeded when in SQL mode.
	if rc.unsafeSession != nil {
		rc.unsafeSession.Close()
	}

	if rc.LogFileManager != nil {
		rc.LogFileManager.Close()
	}

	if rc.rawKVClient != nil {
		rc.rawKVClient.Close()
	}

	if rc.logRestoreManager != nil {
		rc.logRestoreManager.Close(ctx)
	}

	if rc.sstRestoreManager != nil {
		rc.sstRestoreManager.Close(ctx)
	}
	log.Info("Log client closed")
}

func (rc *LogClient) rewriteRulesFor(sst SSTs, rules *restoreutils.RewriteRules) (*restoreutils.RewriteRules, error) {
	if r, ok := sst.(RewrittenSSTs); ok {
		rewritten := r.RewrittenTo()
		if rewritten != sst.TableID() {
			rewriteRules := rules.Clone()
			if !rewriteRules.RewriteSourceTableID(rewritten, sst.TableID()) {
				return nil, errors.Annotatef(
					berrors.ErrUnknown,
					"table rewritten from a table id (%d) to (%d) which doesn't exist in the stream",
					rewritten,
					sst.TableID(),
				)
			}
			log.Info("Rewritten rewrite rules.", zap.Stringer("rules", rewriteRules), zap.Int64("table_id", sst.TableID()), zap.Int64("rewritten_to", rewritten))
			return rewriteRules, nil
		}
	}
	// Need to set ts range for compacted sst to filter out irrelevant data.
	if sst.Type() == CompactedSSTsType && !rules.HasSetTs() {
		rules.SetTsRange(rc.shiftStartTS, rc.startTS, rc.restoreTS)
	}
	return rules, nil
}

func (rc *LogClient) RestoreSSTFiles(
	ctx context.Context,
	compactionsIter iter.TryNextor[SSTs],
	rules map[int64]*restoreutils.RewriteRules,
	importModeSwitcher *restore.ImportModeSwitcher,
	onProgress func(int64),
) error {
	begin := time.Now()
	backupFileSets := make([]restore.BackupFileSet, 0, 8)
	// Collect all items from the iterator in advance to avoid blocking during restoration.
	// This approach ensures that we have all necessary data ready for processing,
	// preventing any potential delays caused by waiting for the iterator to yield more items.
	start := time.Now()
	for r := compactionsIter.TryNext(ctx); !r.Finished; r = compactionsIter.TryNext(ctx) {
		if r.Err != nil {
			return r.Err
		}
		i := r.Item

		tid := i.TableID()
		if r, ok := i.(RewrittenSSTs); ok {
			tid = r.RewrittenTo()
		}
		rewriteRules, ok := rules[tid]
		if !ok {
			log.Warn("[Compacted SST Restore] Skipping excluded table during restore.", zap.Int64("table_id", i.TableID()))
			continue
		}
		newRules, err := rc.rewriteRulesFor(i, rewriteRules)
		if err != nil {
			return err
		}

		set := restore.BackupFileSet{
			TableID:      i.TableID(),
			SSTFiles:     i.GetSSTs(),
			RewriteRules: newRules,
		}
		backupFileSets = append(backupFileSets, set)
	}
	if len(backupFileSets) == 0 {
		log.Info("[Compacted SST Restore] No SST files found for restoration.")
		return nil
	}
	err := importModeSwitcher.GoSwitchToImportMode(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		switchErr := importModeSwitcher.SwitchToNormalMode(ctx)
		if switchErr != nil {
			log.Warn("[Compacted SST Restore] Failed to switch back to normal mode after restoration.", zap.Error(switchErr))
		}
	}()

	log.Info("[Compacted SST Restore] Start to restore SST files",
		zap.Int("sst-file-count", len(backupFileSets)), zap.Duration("iterate-take", time.Since(start)))
	start = time.Now()
	defer func() {
		log.Info("[Compacted SST Restore] Restore SST files finished", zap.Duration("restore-take", time.Since(start)))
	}()

	// To optimize performance and minimize cross-region downloads,
	// we are currently opting for a single restore approach instead of batch restoration.
	// This decision is similar to the handling of raw and txn restores,
	// where batch processing may lead to increased complexity and potential inefficiencies.
	// TODO: Future enhancements may explore the feasibility of reintroducing batch restoration
	// while maintaining optimal performance and resource utilization.
	err = rc.sstRestoreManager.restorer.GoRestore(onProgress, backupFileSets)
	if err != nil {
		return errors.Trace(err)
	}
	err = rc.sstRestoreManager.restorer.WaitUntilFinish()

	for _, files := range backupFileSets {
		for _, f := range files.SSTFiles {
			log.Info("Collected file.", zap.Uint64("total_kv", f.TotalKvs), zap.Uint64("total_bytes", f.TotalBytes), zap.Uint64("size", f.Size_))
			atomic.AddUint64(&rc.restoreStat.restoreSSTKVCount, f.TotalKvs)
			atomic.AddUint64(&rc.restoreStat.restoreSSTKVSize, f.TotalBytes)
			atomic.AddUint64(&rc.restoreStat.restoreSSTPhySize, f.Size_)
		}
	}
	atomic.AddUint64(&rc.restoreStat.restoreSSTTakes, uint64(time.Since(begin)))
	return err
}

func (rc *LogClient) RestoreSSTStatisticFields(pushTo *[]zapcore.Field) {
	takes := time.Duration(rc.restoreStat.restoreSSTTakes)
	fields := []zapcore.Field{
		zap.Uint64("restore-sst-kv-count", rc.restoreStat.restoreSSTKVCount),
		zap.Uint64("restore-sst-kv-size", rc.restoreStat.restoreSSTKVSize),
		zap.Uint64("restore-sst-physical-size (after compression)", rc.restoreStat.restoreSSTPhySize),
		zap.Duration("restore-sst-total-take", takes),
		zap.String("average-speed (sst)", units.HumanSize(float64(rc.restoreStat.restoreSSTKVSize)/takes.Seconds())+"/s"),
	}
	*pushTo = append(*pushTo, fields...)
}

func (rc *LogClient) SetRawKVBatchClient(
	ctx context.Context,
	pdAddrs []string,
	security config.Security,
) error {
	rawkvClient, err := rawkv.NewRawkvClient(ctx, pdAddrs, security)
	if err != nil {
		return errors.Trace(err)
	}

	rc.rawKVClient = rawkv.NewRawKVBatchClient(rawkvClient, rawKVBatchCount)
	return nil
}

func (rc *LogClient) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

func (rc *LogClient) SetUpstreamClusterID(upstreamClusterID uint64) {
	log.Info("upstream cluster id", zap.Uint64("cluster id", upstreamClusterID))
	rc.upstreamClusterID = upstreamClusterID
}

func (rc *LogClient) SetStorage(ctx context.Context, backend *backuppb.StorageBackend, opts *storeapi.Options) error {
	var err error
	rc.storage, err = objstore.New(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *LogClient) SetCurrentTS(ts uint64) error {
	if ts == 0 {
		return errors.Errorf("set rewrite ts to an invalid ts: %d", ts)
	}
	rc.currentTS = ts
	return nil
}

func (rc *LogClient) CurrentTS() uint64 {
	return rc.currentTS
}

// GetClusterID gets the cluster id from down-stream cluster.
func (rc *LogClient) GetClusterID(ctx context.Context) uint64 {
	if rc.clusterID <= 0 {
		rc.clusterID = rc.pdClient.GetClusterID(ctx)
	}
	return rc.clusterID
}

func (rc *LogClient) GetDomain() *domain.Domain {
	return rc.dom
}

func (rc *LogClient) UnsafeSession() glue.Session {
	return rc.unsafeSession
}

func (rc *LogClient) CleanUpKVFiles(
	ctx context.Context,
) error {
	// Current we only have v1 prefix.
	// In the future, we can add more operation for this interface.
	return rc.logRestoreManager.fileImporter.ClearFiles(ctx, rc.pdClient, "v1")
}

func createSession(ctx context.Context, g glue.Glue, store kv.Storage) (glue.Session, error) {
	unsafeSession, err := g.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Set SQL mode to None for avoiding SQL compatibility problem
	err = unsafeSession.Execute(ctx, "set @@sql_mode=''")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return unsafeSession, nil
}

func createSessions(ctx context.Context, g glue.Glue, store kv.Storage, count uint) (createdUnsafeSessions []glue.Session, createErr error) {
	unsafeSessions := make([]glue.Session, 0, count)
	defer func() {
		if createErr != nil {
			closeSessions(unsafeSessions)
		}
	}()
	for range count {
		unsafeSession, err := createSession(ctx, g, store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		unsafeSessions = append(unsafeSessions, unsafeSession)
	}
	return unsafeSessions, nil
}

func closeSessions(sessions []glue.Session) {
	for _, session := range sessions {
		if session != nil {
			session.Close()
		}
	}
}

// Init create db connection and domain for storage.
func (rc *LogClient) Init(ctx context.Context, g glue.Glue, store kv.Storage) error {
	var err error
	rc.unsafeSession, err = createSession(ctx, g, store)
	if err != nil {
		return errors.Trace(err)
	}

	rc.dom, err = g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (rc *LogClient) InitClients(
	ctx context.Context,
	backend *backuppb.StorageBackend,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
	sstCheckpointMetaManager checkpoint.SnapshotMetaManagerT,
	concurrency uint,
	concurrencyPerStore uint,
) error {
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, rc.pdClient, util.SkipTiFlash)
	if err != nil {
		log.Fatal("failed to get stores", zap.Error(err))
	}

	metaClient := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, len(stores)+1)
	importCli := importclient.NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)

	rc.logRestoreManager, err = NewLogRestoreManager(
		ctx,
		NewLogFileImporter(metaClient, importCli, backend),
		concurrency,
		logCheckpointMetaManager,
	)
	if err != nil {
		return errors.Trace(err)
	}
	var createCallBacks []func(*snapclient.SnapFileImporter) error
	var closeCallBacks []func(*snapclient.SnapFileImporter) error
	createCallBacks = append(createCallBacks, func(importer *snapclient.SnapFileImporter) error {
		return importer.CheckMultiIngestSupport(ctx, stores)
	})
	createCallBacks = append(createCallBacks, func(importer *snapclient.SnapFileImporter) error {
		return importer.CheckBatchDownloadSupport(ctx, stores)
	})

	opt := snapclient.NewSnapFileImporterOptions(
		rc.cipher, metaClient, importCli, backend,
		snapclient.RewriteModeKeyspace, stores, concurrencyPerStore, createCallBacks, closeCallBacks,
	)
	snapFileImporter, err := snapclient.NewSnapFileImporter(
		ctx, rc.dom.Store().GetCodec().GetAPIVersion(), snapclient.TiDBCompacted, opt)
	if err != nil {
		return errors.Trace(err)
	}
	rc.sstRestoreManager, err = NewSstRestoreManager(
		ctx,
		metaClient,
		snapFileImporter,
		concurrencyPerStore,
		uint(len(stores)),
		sstCheckpointMetaManager,
	)
	return errors.Trace(err)
}

func (rc *LogClient) InitCheckpointMetadataForCompactedSstRestore(
	ctx context.Context,
	sstCheckpointMetaManager checkpoint.SnapshotMetaManagerT,
) (map[string]struct{}, error) {
	sstCheckpointSets := make(map[string]struct{})

	exists, err := sstCheckpointMetaManager.ExistsCheckpointMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if exists {
		// we need to load the checkpoint data for the following restore
		_, err = sstCheckpointMetaManager.LoadCheckpointData(ctx, func(tableID int64, v checkpoint.RestoreValueType) error {
			sstCheckpointSets[v.Name] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// initialize the checkpoint metadata since it is the first time to restore.
		err = sstCheckpointMetaManager.SaveCheckpointMetadata(ctx, &checkpoint.CheckpointMetadataForSnapshotRestore{})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return sstCheckpointSets, nil
}

func (rc *LogClient) LoadOrCreateCheckpointMetadataForLogRestore(
	ctx context.Context,
	restoreStartTS, startTS, restoredTS uint64,
	gcRatio string,
	tiflashRecorder *tiflashrec.TiFlashRecorder,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (string, error) {
	rc.useCheckpoint = true

	// if the checkpoint metadata exists in the external storage, the restore is not
	// for the first time.
	exists, err := logCheckpointMetaManager.ExistsCheckpointMetadata(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	if exists {
		// load the checkpoint since this is not the first time to restore
		log.Info("loading existing log restore checkpoint")
		meta, err := logCheckpointMetaManager.LoadCheckpointMetadata(ctx)
		if err != nil {
			return "", errors.Trace(err)
		}

		log.Info("reuse gc ratio from checkpoint metadata", zap.String("old-gc-ratio", gcRatio),
			zap.String("checkpoint-gc-ratio", meta.GcRatio))
		return meta.GcRatio, nil
	}

	// initialize the checkpoint metadata since it is the first time to restore.
	var items map[int64]model.TiFlashReplicaInfo
	if tiflashRecorder != nil {
		items = tiflashRecorder.GetItems()
	}
	log.Info("save gc ratio into checkpoint metadata",
		zap.Uint64("start-ts", startTS), zap.Uint64("restored-ts", restoredTS), zap.Uint64("rewrite-ts", rc.currentTS),
		zap.String("gc-ratio", gcRatio), zap.Int("tiflash-item-count", len(items)))
	if err := logCheckpointMetaManager.SaveCheckpointMetadata(ctx, &checkpoint.CheckpointMetadataForLogRestore{
		UpstreamClusterID: rc.upstreamClusterID,
		RestoreStartTS:    restoreStartTS,
		RestoredTS:        restoredTS,
		StartTS:           startTS,
		RewriteTS:         rc.currentTS,
		GcRatio:           gcRatio,
		TiFlashItems:      items,
	}); err != nil {
		return gcRatio, errors.Trace(err)
	}

	return gcRatio, nil
}

type LockedMigrations struct {
	Migs     []*backuppb.Migration
	ReadLock objstore.RemoteLock
}

func (rc *LogClient) GetLockedMigrations(ctx context.Context) (*LockedMigrations, error) {
	ext := stream.MigrationExtension(rc.storage)
	readLock, err := ext.GetReadLock(ctx, "restore stream")
	if err != nil {
		return nil, err
	}

	migs, err := ext.Load(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ms := migs.ListAll()

	lms := &LockedMigrations{
		Migs:     ms,
		ReadLock: readLock,
	}
	return lms, nil
}

func (rc *LogClient) InstallLogFileManager(ctx context.Context, startTS, restoreTS uint64, metadataDownloadBatchSize uint,
	encryptionManager *encryption.Manager) error {
	init := LogFileManagerInit{
		StartTS:   startTS,
		RestoreTS: restoreTS,
		Storage:   rc.storage,

		MigrationsBuilder: &WithMigrationsBuilder{
			startTS:    startTS,
			restoredTS: restoreTS,
		},
		MetadataDownloadBatchSize: metadataDownloadBatchSize,
		EncryptionManager:         encryptionManager,
	}
	var err error
	rc.LogFileManager, err = CreateLogFileManager(ctx, init)
	if err != nil {
		return err
	}
	rc.logFilesStat = logFilesStatistic{}
	rc.LogFileManager.Stats = &rc.logFilesStat
	return nil
}

type FilesInRegion struct {
	defaultSize    uint64
	defaultKVCount int64
	writeSize      uint64
	writeKVCount   int64

	defaultFiles []*LogDataFileInfo
	writeFiles   []*LogDataFileInfo
	deleteFiles  []*LogDataFileInfo
}

type FilesInTable struct {
	regionMapFiles map[int64]*FilesInRegion
}

func ApplyKVFilesWithBatchMethod(
	ctx context.Context,
	logIter LogIter,
	batchCount int,
	batchSize uint64,
	applyFunc func(files []*LogDataFileInfo, kvCount int64, size uint64),
	applyWg *sync.WaitGroup,
) error {
	var (
		tableMapFiles        = make(map[int64]*FilesInTable)
		tmpFiles             = make([]*LogDataFileInfo, 0, batchCount)
		tmpSize       uint64 = 0
		tmpKVCount    int64  = 0
	)
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		if r.Err != nil {
			return r.Err
		}

		f := r.Item
		if f.GetType() == backuppb.FileType_Put && f.GetLength() >= batchSize {
			applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
			continue
		}

		fit, exist := tableMapFiles[f.TableId]
		if !exist {
			fit = &FilesInTable{
				regionMapFiles: make(map[int64]*FilesInRegion),
			}
			tableMapFiles[f.TableId] = fit
		}
		fs, exist := fit.regionMapFiles[f.RegionId]
		if !exist {
			fs = &FilesInRegion{}
			fit.regionMapFiles[f.RegionId] = fs
		}

		if f.GetType() == backuppb.FileType_Delete {
			if fs.defaultFiles == nil {
				fs.deleteFiles = make([]*LogDataFileInfo, 0)
			}
			fs.deleteFiles = append(fs.deleteFiles, f)
		} else {
			if f.GetCf() == consts.DefaultCF {
				if fs.defaultFiles == nil {
					fs.defaultFiles = make([]*LogDataFileInfo, 0, batchCount)
				}
				fs.defaultFiles = append(fs.defaultFiles, f)
				fs.defaultSize += f.Length
				fs.defaultKVCount += f.GetNumberOfEntries()
				if len(fs.defaultFiles) >= batchCount || fs.defaultSize >= batchSize {
					applyFunc(fs.defaultFiles, fs.defaultKVCount, fs.defaultSize)
					fs.defaultFiles = nil
					fs.defaultSize = 0
					fs.defaultKVCount = 0
				}
			} else {
				if fs.writeFiles == nil {
					fs.writeFiles = make([]*LogDataFileInfo, 0, batchCount)
				}
				fs.writeFiles = append(fs.writeFiles, f)
				fs.writeSize += f.GetLength()
				fs.writeKVCount += f.GetNumberOfEntries()
				if len(fs.writeFiles) >= batchCount || fs.writeSize >= batchSize {
					applyFunc(fs.writeFiles, fs.writeKVCount, fs.writeSize)
					fs.writeFiles = nil
					fs.writeSize = 0
					fs.writeKVCount = 0
				}
			}
		}
	}

	for _, fwt := range tableMapFiles {
		for _, fs := range fwt.regionMapFiles {
			if len(fs.defaultFiles) > 0 {
				applyFunc(fs.defaultFiles, fs.defaultKVCount, fs.defaultSize)
			}
			if len(fs.writeFiles) > 0 {
				applyFunc(fs.writeFiles, fs.writeKVCount, fs.writeSize)
			}
		}
	}

	applyWg.Wait()
	for _, fwt := range tableMapFiles {
		for _, fs := range fwt.regionMapFiles {
			for _, d := range fs.deleteFiles {
				tmpFiles = append(tmpFiles, d)
				tmpSize += d.GetLength()
				tmpKVCount += d.GetNumberOfEntries()

				if len(tmpFiles) >= batchCount || tmpSize >= batchSize {
					applyFunc(tmpFiles, tmpKVCount, tmpSize)
					tmpFiles = make([]*LogDataFileInfo, 0, batchCount)
					tmpSize = 0
					tmpKVCount = 0
				}
			}
			if len(tmpFiles) > 0 {
				applyFunc(tmpFiles, tmpKVCount, tmpSize)
				tmpFiles = make([]*LogDataFileInfo, 0, batchCount)
				tmpSize = 0
				tmpKVCount = 0
			}
		}
	}

	return nil
}

func ApplyKVFilesWithSingleMethod(
	ctx context.Context,
	files LogIter,
	applyFunc func(file []*LogDataFileInfo, kvCount int64, size uint64),
	applyWg *sync.WaitGroup,
) error {
	deleteKVFiles := make([]*LogDataFileInfo, 0)

	for r := files.TryNext(ctx); !r.Finished; r = files.TryNext(ctx) {
		if r.Err != nil {
			return r.Err
		}

		f := r.Item
		if f.GetType() == backuppb.FileType_Delete {
			deleteKVFiles = append(deleteKVFiles, f)
			continue
		}
		applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
	}

	applyWg.Wait()
	log.Info("restore delete files", zap.Int("count", len(deleteKVFiles)))
	for _, file := range deleteKVFiles {
		f := file
		applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
	}

	return nil
}

func (rc *LogClient) RestoreKVFiles(
	ctx context.Context,
	rules map[int64]*restoreutils.RewriteRules,
	logIter LogIter,
	pitrBatchCount uint32,
	pitrBatchSize uint32,
	updateStats func(kvCount uint64, size uint64),
	onProgress func(cnt int64),
	cipherInfo *backuppb.CipherInfo,
	masterKeys []*encryptionpb.MasterKey,
) error {
	var (
		err          error
		fileCount    = 0
		start        = time.Now()
		supportBatch = version.CheckPITRSupportBatchKVFiles()
		skipFile     = 0
	)
	defer func() {
		if err == nil {
			elapsed := time.Since(start)
			log.Info("Restored KV files", zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", fileCount, elapsed)
		}
	}()

	log.Info("starting to restore kv files")
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreKVFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var (
		applyWg  sync.WaitGroup
		eg, ectx = errgroup.WithContext(ctx)

		skipped    = metrics.KVApplyTasksEvents.WithLabelValues("skipped")
		submitted  = metrics.KVApplyTasksEvents.WithLabelValues("submitted")
		started    = metrics.KVApplyTasksEvents.WithLabelValues("started")
		finished   = metrics.KVApplyTasksEvents.WithLabelValues("finished")
		memApplied = metrics.KVLogFileEmittedMemory.WithLabelValues("2-applied")
	)
	applyFunc := func(files []*LogDataFileInfo, kvCount int64, size uint64) {
		cnt := 0
		if len(files) == 0 {
			return
		}
		// get rewrite rule from table id.
		// because the tableID of files is the same.
		rule, ok := rules[files[0].TableId]
		if !ok {
			skipped.Add(float64(len(files)))
			onProgress(kvCount)
			summary.CollectInt("FileSkip", len(files))
			log.Debug("skip file due to table id not matched", zap.Int64("table-id", files[0].TableId))
			skipFile += len(files)
		} else {
			submitted.Add(float64(len(files)))
			applyWg.Add(1)
			cnt += 1
			i := cnt
			ectx := logutil.ContextWithField(ectx, zap.Int("sn", i))
			rc.logRestoreManager.workerPool.ApplyOnErrorGroup(eg, func() (err error) {
				started.Add(float64(len(files)))
				fileStart := time.Now()
				defer applyWg.Done()
				defer func() {
					for _, file := range files {
						memApplied.Add(float64(file.Size()))
					}
					finished.Add(float64(len(files)))
					onProgress(kvCount)
					updateStats(uint64(kvCount), size)
					summary.CollectInt("File", len(files))

					maxTs, minTs := uint64(0), uint64(math.MaxUint64)
					if err == nil {
						filenames := make([]string, 0, len(files))
						for _, f := range files {
							maxTs = max(f.MaxTs, maxTs)
							minTs = min(f.MinTs, minTs)
							filenames = append(filenames, f.Path)
							if rc.logRestoreManager.checkpointRunner != nil {
								if e := checkpoint.AppendRangeForLogRestore(ectx, rc.logRestoreManager.checkpointRunner, f.MetaDataGroupName, rule.NewTableID, f.OffsetInMetaGroup, f.OffsetInMergedGroup); e != nil {
									err = errors.Annotate(e, "failed to append checkpoint data")
									break
								}
							}
						}
						logutil.CL(ectx).Info("import files done", zap.Int("batch-count", len(files)), zap.Uint64("batch-size", size),
							zap.Uint64("min-ts", minTs), zap.Uint64("max-ts", maxTs), zap.String("cf", files[0].Cf),
							zap.Duration("take", time.Since(fileStart)), zap.Strings("files", filenames))

						metrics.KVApplyBatchDuration.Observe(time.Since(fileStart).Seconds())
						metrics.KVApplyBatchSize.Observe(float64(size))
						metrics.KVApplyBatchFiles.Observe(float64(len(files)))
					}
				}()

				return rc.logRestoreManager.fileImporter.ImportKVFiles(ectx, files, rule, rc.shiftStartTS, rc.startTS, rc.restoreTS,
					supportBatch, cipherInfo, masterKeys)
			})
		}
	}

	rc.logRestoreManager.workerPool.ApplyOnErrorGroup(eg, func() error {
		if supportBatch {
			err = ApplyKVFilesWithBatchMethod(ectx, logIter, int(pitrBatchCount), uint64(pitrBatchSize), applyFunc, &applyWg)
		} else {
			err = ApplyKVFilesWithSingleMethod(ectx, logIter, applyFunc, &applyWg)
		}
		return errors.Trace(err)
	})

	if err = eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error("restore files failed", zap.Error(err))
	}

	log.Info("total skip files due to table id not matched", zap.Int("count", skipFile))
	if skipFile > 0 {
		log.Debug("table id in full backup storage", zap.Any("tables", rules))
	}

	return errors.Trace(err)
}

type FullBackupStorageConfig struct {
	Backend *backuppb.StorageBackend
	Opts    *storeapi.Options
}

// GetBaseIDMapAndMerge get the id map from following ways
// 1. from previously saved id map if the same task has been running and built/saved id map already but failed later
// 2. from previous different task. A PiTR job might be split into multiple runs/tasks and each task only restores
// a subset of the entire job.
func (rc *LogClient) GetBaseIDMapAndMerge(
	ctx context.Context,
	hasFullBackupStorageConfig,
	loadSavedIDMap bool,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (*SegmentedPiTRState, error) {
	var (
		err    error
		state  *SegmentedPiTRState
		dbMaps []*backuppb.PitrDBMap
	)

	// this is a retry, id map saved last time, load it from external storage
	if loadSavedIDMap {
		log.Info("try to load previously saved pitr id maps")
		state, err = rc.loadSegmentedPiTRState(ctx, rc.restoreTS, logCheckpointMetaManager, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if state != nil {
			dbMaps = state.DbMaps
		}
	}

	// a new task, but without full snapshot restore, tries to load
	// schemas map whose `restore-ts`` is the task's `start-ts`.
	if len(dbMaps) <= 0 && !hasFullBackupStorageConfig {
		log.Info("try to load pitr id maps of the previous task", zap.Uint64("start-ts", rc.startTS))
		state, err = rc.loadSegmentedPiTRState(ctx, rc.startTS, logCheckpointMetaManager, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if state != nil {
			dbMaps = state.DbMaps
		}
		if len(dbMaps) > 0 {
			if err := rc.validateNoTiFlashReplica(); err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if len(dbMaps) <= 0 && !hasFullBackupStorageConfig {
		log.Error("no id maps found")
		return nil, errors.New("no base id map found from saved id or last restored PiTR")
	}
	return state, nil
}

func SortMetaKVFiles(files []*backuppb.DataFileInfo) []*backuppb.DataFileInfo {
	slices.SortFunc(files, func(i, j *backuppb.DataFileInfo) int {
		if c := cmp.Compare(i.GetMinTs(), j.GetMinTs()); c != 0 {
			return c
		}
		if c := cmp.Compare(i.GetMaxTs(), j.GetMaxTs()); c != 0 {
			return c
		}
		return cmp.Compare(i.GetResolvedTs(), j.GetResolvedTs())
	})
	return files
}

// validateNoTiFlashReplica makes sure no table contains TiFlash replica
func (rc *LogClient) validateNoTiFlashReplica() error {
	existTiFlashTable := false
	rc.dom.InfoSchema().ListTablesWithSpecialAttribute(func(tableInfo *model.TableInfo) bool {
		if tableInfo.TiFlashReplica != nil && tableInfo.TiFlashReplica.Count > 0 {
			existTiFlashTable = true
		}
		return false
	})
	if existTiFlashTable {
		return errors.Errorf("exist table(s) have tiflash replica, please remove it before restore")
	}
	return nil
}

// SeparateAndSortFilesByCF filters and sorts files by column family.
// It separates files into write CF and default CF groups and then sorts them within each CF group.
func SeparateAndSortFilesByCF(files []*backuppb.DataFileInfo) ([]*backuppb.DataFileInfo, []*backuppb.DataFileInfo) {
	filesInWriteCF := make([]*backuppb.DataFileInfo, 0, len(files))
	filesInDefaultCF := make([]*backuppb.DataFileInfo, 0, len(files))

	// The k-v events in default CF should be restored firstly. The reason is that:
	// The error of transactions of meta could happen if restore write CF events successfully,
	// but failed to restore default CF events.
	for _, f := range files {
		if f.Cf == consts.WriteCF {
			filesInWriteCF = append(filesInWriteCF, f)
			continue
		}
		if f.Type == backuppb.FileType_Delete {
			log.Warn("internal error: detected delete file of meta key, skip it", zap.Any("file", f))
			continue
		}
		if f.Cf == consts.DefaultCF {
			filesInDefaultCF = append(filesInDefaultCF, f)
		}
	}

	filesInDefaultCF = SortMetaKVFiles(filesInDefaultCF)
	filesInWriteCF = SortMetaKVFiles(filesInWriteCF)

	return filesInDefaultCF, filesInWriteCF
}

// LoadAndProcessMetaKVFilesInBatch restores meta kv files to TiKV in strict TS order. It does so in batch and after
// success it triggers an update so every TiDB node can pick up the restored content.
func LoadAndProcessMetaKVFilesInBatch(
	ctx context.Context,
	defaultFiles []*backuppb.DataFileInfo,
	writeFiles []*backuppb.DataFileInfo,
	processor BatchMetaKVProcessor,
) error {
	// the average size of each KV is 2560 Bytes
	// kvEntries is kvs left by the previous batch
	const kvSize = 2560
	var (
		rangeMin uint64
		rangeMax uint64
		err      error

		batchSize  uint64 = 0
		defaultIdx int    = 0
		writeIdx   int    = 0

		defaultKvEntries = make([]*KvEntryWithTS, 0)
		writeKvEntries   = make([]*KvEntryWithTS, 0)
	)

	for i, f := range defaultFiles {
		if i == 0 {
			rangeMax = f.MaxTs
			rangeMin = f.MinTs
			batchSize = f.Length
		} else {
			if f.MinTs <= rangeMax && batchSize+f.Length <= MetaKVBatchSize {
				rangeMin = min(rangeMin, f.MinTs)
				rangeMax = max(rangeMax, f.MaxTs)
				batchSize += f.Length
			} else {
				// Either f.MinTS > rangeMax or f.MinTs is the filterTs we need.
				// So it is ok to pass f.MinTs as filterTs.
				logutil.CL(ctx).Info("Meta KV restore batch: default CF.",
					zap.Int("from-idx", defaultIdx), zap.Int("to-idx", i), zap.Uint64("range-min-ts", rangeMin),
					zap.Uint64("range-max-ts", rangeMax), zap.Uint64("total-batch-size", batchSize))
				defaultKvEntries, err = processor.ProcessBatch(ctx, defaultFiles[defaultIdx:i], defaultKvEntries, f.MinTs, consts.DefaultCF)
				if err != nil {
					return errors.Trace(err)
				}
				defaultIdx = i
				rangeMin = f.MinTs
				rangeMax = f.MaxTs
				// the initial batch size is the size of left kvs and the current file length.
				batchSize = uint64(len(defaultKvEntries)*kvSize) + f.Length

				// restore writeCF kv to f.MinTs
				var toWriteIdx int
				for toWriteIdx = writeIdx; toWriteIdx < len(writeFiles); toWriteIdx++ {
					if writeFiles[toWriteIdx].MinTs >= f.MinTs {
						break
					}
				}
				logutil.CL(ctx).Info("Meta KV restore batch: write CF.",
					zap.Int("from-idx", writeIdx), zap.Int("to-idx", toWriteIdx), zap.Uint64("range-min-ts", rangeMin),
					zap.Uint64("range-max-ts", rangeMax))
				writeKvEntries, err = processor.ProcessBatch(ctx, writeFiles[writeIdx:toWriteIdx], writeKvEntries, f.MinTs, consts.WriteCF)
				if err != nil {
					return errors.Trace(err)
				}
				writeIdx = toWriteIdx
			}
		}
	}

	// restore the left meta kv files and entries
	// Notice: restoreBatch needs to realize the parameter `files` and `kvEntries` might be empty
	// Assert: defaultIdx <= len(defaultFiles) && writeIdx <= len(writeFiles)
	logutil.CL(ctx).Info("Meta KV restore batch: last default CF.",
		zap.Int("from-idx", defaultIdx), zap.Uint64("range-min-ts", rangeMin), zap.Uint64("range-max-ts", rangeMax))
	_, err = processor.ProcessBatch(ctx, defaultFiles[defaultIdx:], defaultKvEntries, math.MaxUint64, consts.DefaultCF)
	if err != nil {
		return errors.Trace(err)
	}
	logutil.CL(ctx).Info("Meta KV restore batch: last write CF.",
		zap.Int("from-idx", writeIdx), zap.Uint64("range-min-ts", rangeMin), zap.Uint64("range-max-ts", rangeMax))
	_, err = processor.ProcessBatch(ctx, writeFiles[writeIdx:], writeKvEntries, math.MaxUint64, consts.WriteCF)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// RestoreBatchMetaKVFiles tries to restore and rewrite meta kv to TiKV from external storage. It reads out entries
// from the given files and only restores ones that's in filter range, then it returns those entries out of the filter
// range back to caller for next iteration of restore.
func (rc *LogClient) RestoreBatchMetaKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	kvEntries []*KvEntryWithTS,
	filterTS uint64,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
	cf string,
) ([]*KvEntryWithTS, error) {
	curSortedKvEntries, filteredOutKvEntries, err := rc.filterAndSortKvEntriesFromFiles(ctx, files, kvEntries, filterTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(curSortedKvEntries) == 0 {
		return filteredOutKvEntries, nil
	}

	// restore and rewrite these entries to TiKV with rawPut() method.
	kvCount, size, err := rc.restoreAndRewriteMetaKvEntries(ctx, schemasReplace, curSortedKvEntries, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	updateStats(kvCount, size)
	for range files {
		progressInc()
	}
	metrics.MetaKVBatchFiles.WithLabelValues(cf).Observe(float64(len(files)))
	metrics.MetaKVBatchFilteredKeys.WithLabelValues(cf).Observe(float64(len(filteredOutKvEntries)))
	metrics.MetaKVBatchKeys.WithLabelValues(cf).Observe(float64(kvCount))
	metrics.MetaKVBatchSize.WithLabelValues(cf).Observe(float64(size))

	return filteredOutKvEntries, nil
}

func (rc *LogClient) filterAndSortKvEntriesFromFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	kvEntries []*KvEntryWithTS,
	filterTS uint64,
) ([]*KvEntryWithTS, []*KvEntryWithTS, error) {
	filteredOutKvEntries := make([]*KvEntryWithTS, 0)
	curKvEntries := make([]*KvEntryWithTS, 0)
	if len(files) == 0 && len(kvEntries) == 0 {
		return curKvEntries, filteredOutKvEntries, nil
	}

	// filter the kv from kvEntries again.
	for _, kv := range kvEntries {
		if kv.Ts < filterTS {
			curKvEntries = append(curKvEntries, kv)
		} else {
			filteredOutKvEntries = append(filteredOutKvEntries, kv)
		}
	}

	// read all entries from files.
	for _, f := range files {
		es, filteredOutEs, err := rc.ReadFilteredEntriesFromFiles(ctx, f, filterTS)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		curKvEntries = append(curKvEntries, es...)
		filteredOutKvEntries = append(filteredOutKvEntries, filteredOutEs...)
	}

	// sort these entries.
	slices.SortFunc(curKvEntries, func(i, j *KvEntryWithTS) int {
		return cmp.Compare(i.Ts, j.Ts)
	})
	return curKvEntries, filteredOutKvEntries, nil
}

func (rc *LogClient) restoreAndRewriteMetaKvEntries(
	ctx context.Context,
	sr *stream.SchemasReplace,
	entries []*KvEntryWithTS,
	columnFamily string,
) (uint64, uint64, error) {
	var (
		kvCount uint64
		size    uint64
	)

	rc.rawKVClient.SetColumnFamily(columnFamily)

	for _, entry := range entries {
		log.Debug("before rewriting entry", zap.Uint64("key-ts", entry.Ts), zap.Int("key-len", len(entry.E.Key)),
			zap.Int("value-len", len(entry.E.Value)), zap.ByteString("key", entry.E.Key))

		newEntry, err := sr.RewriteMetaKvEntry(&entry.E, columnFamily)
		if err != nil {
			log.Error("rewrite txn entry failed", zap.Int("klen", len(entry.E.Key)),
				logutil.Key("txn-key", entry.E.Key))
			return 0, 0, errors.Trace(err)
		} else if newEntry == nil {
			continue
		}
		// sanity check key will never be nil, otherwise will write invalid format data to TiKV
		if len(newEntry.Key) == 0 {
			log.Error("invalid nil key during rewrite")
			return 0, 0, errors.Trace(err)
		}
		log.Debug("after rewrite entry", zap.Int("new-key-len", len(newEntry.Key)),
			zap.Int("new-value-len", len(entry.E.Value)), zap.ByteString("new-key", newEntry.Key))

		failpoint.Inject("failed-to-restore-metakv", func(_ failpoint.Value) {
			failpoint.Return(0, 0, errors.Errorf("failpoint: failed to restore metakv"))
		})
		if err := PutRawKvWithRetry(ctx, rc.rawKVClient, newEntry.Key, newEntry.Value, entry.Ts); err != nil {
			return 0, 0, errors.Trace(err)
		}
		// for failpoint, we need to flush the cache in rawKVClient every time
		failpoint.Inject("do-not-put-metakv-in-batch", func(_ failpoint.Value) {
			if err := rc.rawKVClient.PutRest(ctx); err != nil {
				failpoint.Return(0, 0, errors.Trace(err))
			}
		})
		kvCount++
		size += uint64(len(newEntry.Key) + len(newEntry.Value))
	}

	return kvCount, size, rc.rawKVClient.PutRest(ctx)
}

// GenGlobalID generates a global id by transaction way.
func (rc *LogClient) GenGlobalID(ctx context.Context) (int64, error) {
	var id int64
	storage := rc.GetDomain().Store()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMutator(txn)
			id, e = t.GenGlobalID()
			return e
		})

	return id, err
}

// GenGlobalIDs generates several global ids by transaction way.
func (rc *LogClient) GenGlobalIDs(ctx context.Context, n int) ([]int64, error) {
	return utils.GenGlobalIDs(ctx, n, rc.GetDomain().Store())
}

// UpdateSchemaVersionFullReload updates schema version to trigger a full reload in transaction way.
func (rc *LogClient) UpdateSchemaVersionFullReload(ctx context.Context) error {
	storage := rc.GetDomain().Store()
	var schemaVersion int64

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	if err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMutator(txn)
			var e error
			// To trigger full-reload instead of diff-reload, we need to increase the schema version
			// by at least `domain.LoadSchemaDiffVersionGapThreshold`.
			schemaVersion, e = t.GenSchemaVersions(64 + issyncer.LoadSchemaDiffVersionGapThreshold)
			if e != nil {
				return e
			}
			// add the diff key so that the domain won't retry to reload the schemas with `schemaVersion` frequently.
			return t.SetSchemaDiff(&model.SchemaDiff{
				Version:             schemaVersion,
				Type:                model.ActionNone,
				SchemaID:            -1,
				TableID:             -1,
				RegenerateSchemaMap: true,
			})
		},
	); err != nil {
		return errors.Trace(err)
	}

	log.Info("update global schema version", zap.Int64("global-schema-version", schemaVersion))

	ver := strconv.FormatInt(schemaVersion, 10)
	if err := ddlutil.PutKVToEtcd(
		ctx,
		rc.GetDomain().GetEtcdClient(),
		math.MaxInt,
		ddlutil.DDLGlobalSchemaVersion,
		ver,
	); err != nil {
		return errors.Annotatef(err, "failed to put global schema verson %v to etcd", ver)
	}

	return nil
}

// SetTableModeToNormal sets the table mode back to normal from restore mode if needed.
func (rc *LogClient) SetTableModeToNormal(ctx context.Context, schemaReplace *stream.SchemasReplace) error {
	infoSchema := rc.dom.InfoSchema()

	// collect all tables that need to be set to normal mode
	for _, dbReplace := range schemaReplace.DbReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}

		// verify schema exists in info schema
		_, exists := infoSchema.SchemaByID(dbReplace.DbID)
		if !exists {
			log.Info("schema doesn't exist in info schema, skipping",
				zap.Int64("schemaID", dbReplace.DbID), zap.String("schemaName", dbReplace.Name))
			continue
		}

		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut {
				continue
			}

			// verify table exists in info schema and is in restore mode
			tbl, exist := infoSchema.TableByID(ctx, tableReplace.TableID)
			if !exist || tbl.Meta().DBID != dbReplace.DbID {
				log.Info("table doesn't exist in info schema, skipping",
					zap.Int64("schemaID", dbReplace.DbID),
					zap.Int64("tableID", tableReplace.TableID),
					zap.String("tableName", tableReplace.Name))
				continue
			}

			// only set back tables that are in restore mode
			if tbl.Meta().Mode != model.TableModeRestore {
				log.Warn("table not in restore mode, skipping",
					zap.Int64("schemaID", dbReplace.DbID),
					zap.Int64("tableID", tableReplace.TableID),
					zap.String("tableName", tableReplace.Name),
					zap.Any("tableMode", tbl.Meta().Mode))
				continue
			}

			dbID := dbReplace.DbID
			tableID := tableReplace.TableID
			// TODO, use batch when available in DDL
			log.Info("altering table mode", zap.Int64("schemaID", dbID), zap.Any("table id", tableID))
			if err := rc.unsafeSession.AlterTableMode(ctx, dbID, tableID, model.TableModeNormal); err != nil {
				return errors.Annotatef(err, "failed to alter table mode for table with schemaID=%d, tableID=%d",
					dbID, tableID)
			}
		}
	}
	return nil
}

// WrapCompactedFilesIterWithSplitHelper applies a splitting strategy to the compacted files iterator.
// It uses a region splitter to handle the splitting logic based on the provided rules and checkpoint sets.
func (rc *LogClient) WrapCompactedFilesIterWithSplitHelper(
	ctx context.Context,
	compactedIter iter.TryNextor[SSTs],
	rules map[int64]*restoreutils.RewriteRules,
	checkpointSets map[string]struct{},
	updateStatsFn func(uint64, uint64),
	splitSize uint64,
	splitKeys int64,
) (iter.TryNextor[SSTs], error) {
	client := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, 3)
	wrapper := restore.PipelineRestorerWrapper[SSTs]{
		PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(client, splitSize, splitKeys),
	}
	strategy := NewCompactedFileSplitStrategy(rules, checkpointSets, updateStatsFn)
	return wrapper.WithSplit(ctx, compactedIter, strategy), nil
}

// WrapLogFilesIterWithSplitHelper applies a splitting strategy to the log files iterator.
// It uses a region splitter to handle the splitting logic based on the provided rules.
func (rc *LogClient) WrapLogFilesIterWithSplitHelper(
	ctx context.Context,
	logIter LogIter,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
	rules map[int64]*restoreutils.RewriteRules,
	updateStatsFn func(uint64, uint64),
	splitSize uint64,
	splitKeys int64,
) (LogIter, error) {
	client := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, 3)
	wrapper := restore.PipelineRestorerWrapper[*LogDataFileInfo]{
		PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(client, splitSize, splitKeys),
	}
	strategy, err := NewLogSplitStrategy(ctx, rc.useCheckpoint, logCheckpointMetaManager, rules, updateStatsFn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return wrapper.WithSplit(ctx, logIter, strategy), nil
}

func WrapLogFilesIterWithCheckpointFailpoint(
	v failpoint.Value,
	logIter LogIter,
	rules map[int64]*restoreutils.RewriteRules,
) (LogIter, error) {
	if cmd, ok := v.(string); ok {
		switch cmd {
		case "corrupt-last-table-files": // skip some files and eventually return an error to make the restore fail
			newLogIter := iter.FilterOut(logIter, func(d *LogDataFileInfo) bool {
				return d.OffsetInMergedGroup&1 > 0
			})
			return newLogIter, errors.Errorf("skip the last table files")
		case "only-last-table-files": // check whether all the files, except files skipped before, are skipped by checkpoint
			newLogIter := iter.FilterOut(logIter, func(d *LogDataFileInfo) bool {
				_, exists := rules[d.TableId]
				if d.OffsetInMergedGroup&1 == 0 && exists {
					log.Panic("has files but not the files skipped before")
				}
				return false
			})
			return newLogIter, nil
		}
	}
	return logIter, nil
}

// ResetTiflashReplicas set tiflash replicas by given sqls concurrently.
func (rc *LogClient) ResetTiflashReplicas(ctx context.Context, sqls []string, g glue.Glue) error {
	resetSessions, err := createSessions(ctx, g, rc.dom.Store(), 16)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		closeSessions(resetSessions)
	}()
	workerpool := tidbutil.NewWorkerPool(16, "repair ingest index")
	eg, ectx := errgroup.WithContext(ctx)
	for _, sql := range sqls {
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			resetSession := resetSessions[id%uint64(len(resetSessions))]
			log.Info("reset tiflash replica", zap.Uint64("task id", id), zap.String("sql", sql))
			var resetErr error
			for range 5 {
				resetErr = resetSession.ExecuteInternal(ectx, sql)
				if resetErr == nil {
					break
				}
				log.Warn("Failed to restore tiflash replica config", zap.Uint64("task id", id), zap.Error(resetErr))
				if ectx.Err() != nil {
					log.Warn("Stop retrying because context cancelled", zap.Error(ectx.Err()))
					break
				}
			}
			if resetErr != nil {
				logutil.WarnTerm("Failed to restore tiflash replica config, you may execute the sql restore it manually.",
					logutil.ShortError(resetErr),
					zap.String("sql", sql),
				)
			}
			return nil
		})
	}
	return eg.Wait()
}

func colsToStr(cols []ast.CIStr) string {
	var str strings.Builder
	for i, col := range cols {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(col.O)
	}
	return str.String()
}

const (
	alterTableDropIndexSQL         = "ALTER TABLE %n.%n DROP INDEX %n"
	alterTableAddIndexFormat       = "ALTER TABLE %%n.%%n ADD INDEX %%n(%s)"
	alterTableAddUniqueIndexFormat = "ALTER TABLE %%n.%%n ADD UNIQUE KEY %%n(%s)"
	alterTableAddPrimaryFormat     = "ALTER TABLE %%n.%%n ADD PRIMARY KEY (%s) NONCLUSTERED"
	alterTableAddForeignKeyFormat  = "ALTER TABLE %%n.%%n ADD CONSTRAINT %%n FOREIGN KEY (%s) REFERENCES %%n.%%n (%s)"
	alterTableDropForeignKeyFormat = "ALTER TABLE %n.%n DROP FOREIGN KEY %n"
)

func (rc *LogClient) generateRepairIngestIndexSQLs(
	ctx context.Context,
	ingestRecorder *ingestrec.IngestRecorder,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) ([]checkpoint.CheckpointIngestIndexRepairSQL, []checkpoint.CheckpointForeignKeyUpdateSQL, bool, error) {
	var sqls []checkpoint.CheckpointIngestIndexRepairSQL
	var fkSqls []checkpoint.CheckpointForeignKeyUpdateSQL
	if rc.useCheckpoint {
		exists, err := logCheckpointMetaManager.ExistsCheckpointIngestIndexRepairSQLs(ctx)
		if err != nil {
			return sqls, fkSqls, false, errors.Trace(err)
		}
		if exists {
			checkpointSQLs, err := logCheckpointMetaManager.LoadCheckpointIngestIndexRepairSQLs(ctx)
			if err != nil {
				return sqls, fkSqls, false, errors.Trace(err)
			}
			sqls = checkpointSQLs.SQLs
			fkSqls = checkpointSQLs.FKSQLs
			log.Info("load ingest index repair sqls from checkpoint", zap.String("category", "ingest"), zap.Reflect("sqls", sqls))
			return sqls, fkSqls, true, nil
		}
	}

	if err := ingestRecorder.UpdateIndexInfo(ctx, rc.dom.InfoSchema()); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}
	if err := ingestRecorder.IterateForeignKeys(func(fkRecord *ingestrec.ForeignKeyRecord) error {
		var (
			addSQL  strings.Builder
			addArgs []any = make([]any, 0, 7+len(fkRecord.Cols)+len(fkRecord.RefCols))
		)
		childCols := colsToStr(fkRecord.Cols)
		parentCols := colsToStr(fkRecord.RefCols)
		addSQL.WriteString(fmt.Sprintf(alterTableAddForeignKeyFormat, childCols, parentCols))
		addArgs = append(addArgs,
			fkRecord.ChildSchemaNameO, fkRecord.ChildTableNameO, fkRecord.Name.O, fkRecord.RefSchema.O, fkRecord.RefTable.O,
		)
		if onDelete := ast.ReferOptionType(fkRecord.OnDelete); onDelete != ast.ReferOptionNoOption {
			addSQL.WriteString(fmt.Sprintf(" ON DELETE %s", onDelete.String()))
		}
		if onUpdate := ast.ReferOptionType(fkRecord.OnUpdate); onUpdate != ast.ReferOptionNoOption {
			addSQL.WriteString(fmt.Sprintf(" ON UPDATE %s", onUpdate.String()))
		}
		fkSqls = append(fkSqls, checkpoint.CheckpointForeignKeyUpdateSQL{
			FKID:       fkRecord.ID,
			SchemaName: fkRecord.ChildSchemaNameO,
			TableName:  fkRecord.ChildTableNameO,
			FKName:     fkRecord.Name.O,
			AddSQL:     addSQL.String(),
			AddArgs:    addArgs,
		})
		return nil
	}); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}
	if err := ingestRecorder.Iterate(func(_, indexID int64, info *ingestrec.IngestIndexInfo) error {
		var (
			addSQL  strings.Builder
			addArgs []any = make([]any, 0, 5+len(info.ColumnArgs))
		)
		if info.IsPrimary {
			addSQL.WriteString(fmt.Sprintf(alterTableAddPrimaryFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else if info.IndexInfo.Unique {
			addSQL.WriteString(fmt.Sprintf(alterTableAddUniqueIndexFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else {
			addSQL.WriteString(fmt.Sprintf(alterTableAddIndexFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		}
		// WHERE CONDITION
		if len(info.IndexInfo.ConditionExprString) > 0 {
			addSQL.WriteString(" WHERE ")
			addSQL.WriteString(info.IndexInfo.ConditionExprString)
		}
		// USING BTREE/HASH/RTREE
		indexTypeStr := info.IndexInfo.Tp.String()
		if len(indexTypeStr) > 0 {
			addSQL.WriteString(" USING ")
			addSQL.WriteString(indexTypeStr)
		}
		// WITH PARSER [...]
		if info.IndexInfo.FullTextInfo != nil {
			addSQL.WriteString(" WITH PARSER ")
			addSQL.WriteString(info.IndexInfo.FullTextInfo.ParserType.SQLName())
		}
		// COMMENT [...]
		if len(info.IndexInfo.Comment) > 0 {
			addSQL.WriteString(" COMMENT %?")
			addArgs = append(addArgs, info.IndexInfo.Comment)
		}

		if info.IndexInfo.Invisible {
			addSQL.WriteString(" INVISIBLE")
		} else {
			addSQL.WriteString(" VISIBLE")
		}

		if info.IndexInfo.Global {
			addSQL.WriteString(" GLOBAL")
		} else {
			addSQL.WriteString(" LOCAL")
		}

		sqls = append(sqls, checkpoint.CheckpointIngestIndexRepairSQL{
			IndexID:    indexID,
			SchemaName: info.SchemaName,
			TableName:  info.TableName,
			IndexName:  info.IndexInfo.Name.O,
			AddSQL:     addSQL.String(),
			AddArgs:    addArgs,
		})

		return nil
	}); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}

	if rc.useCheckpoint && len(sqls) > 0 {
		if err := logCheckpointMetaManager.SaveCheckpointIngestIndexRepairSQLs(ctx, &checkpoint.CheckpointIngestIndexRepairSQLs{
			SQLs:   sqls,
			FKSQLs: fkSqls,
		}); err != nil {
			return sqls, fkSqls, false, errors.Trace(err)
		}
	}
	return sqls, fkSqls, false, nil
}

// RepairIngestIndex drops the indexes from IngestRecorder and re-add them.
func (rc *LogClient) RepairIngestIndex(
	ctx context.Context,
	ingestRecorder *ingestrec.IngestRecorder,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
	g glue.Glue,
) error {
	sqls, fkSqls, fromCheckpoint, err := rc.generateRepairIngestIndexSQLs(ctx, ingestRecorder, logCheckpointMetaManager)
	if err != nil {
		return errors.Trace(err)
	}

	info := rc.dom.InfoSchema()
	console := glue.GetConsole(g)
	for i, sql := range sqls {
		tableInfo, err := info.TableByName(ctx, sql.SchemaName, sql.TableName)
		if err != nil {
			return errors.Trace(err)
		}
		sqls[i].OldIndexIDFound = false
		sqls[i].IndexRepaired = false
		if fromCheckpoint {
			for _, idx := range tableInfo.Indices() {
				indexInfo := idx.Meta()
				if indexInfo.ID == sql.IndexID {
					// the original index id is not dropped
					sqls[i].OldIndexIDFound = true
					break
				}
				// what if index's state is not public?
				if indexInfo.Name.O == sql.IndexName {
					progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)
					// find the same name index, but not the same index id,
					// which means the repaired index id is created
					if _, err := fmt.Fprintf(console.Out(), "%s ... %s\n", progressTitle, color.HiGreenString("SKIPPED DUE TO CHECKPOINT MODE")); err != nil {
						return errors.Trace(err)
					}
					sqls[i].IndexRepaired = true
					break
				}
			}
		}
	}
	for i, sql := range fkSqls {
		tableInfo, err := info.TableByName(ctx, ast.NewCIStr(sql.SchemaName), ast.NewCIStr(sql.TableName))
		if err != nil {
			return errors.Trace(err)
		}
		fkSqls[i].OldForeignKeyFound = false
		fkSqls[i].ForeignKeyUpdated = false
		if fromCheckpoint {
			for _, fk := range tableInfo.Meta().ForeignKeys {
				if fk.ID == sql.FKID {
					// the original foreign key id is not dropped
					fkSqls[i].OldForeignKeyFound = true
					break
				}
				if fk.Name.O == sql.FKName {
					// find the same name foreign key, but not the same foreign key id,
					// which means the foreign key is updated and all the indexes are recreated
					fkSqls[i].ForeignKeyUpdated = true
					break
				}
			}
		}
	}

	sessionCount := defaultRepairIndexSessionCount
	indexSessions, err := createSessions(ctx, g, rc.dom.Store(), sessionCount)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		closeSessions(indexSessions)
	}()
	workerpool := tidbutil.NewWorkerPool(sessionCount, "repair ingest index")
	eg, ectx := errgroup.WithContext(ctx)
	for _, fk := range fkSqls {
		if fk.ForeignKeyUpdated {
			continue
		}
		if fromCheckpoint && !fk.OldForeignKeyFound {
			continue
		}
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			indexSession := indexSessions[id%uint64(len(indexSessions))]
			if err := indexSession.ExecuteInternal(ectx, alterTableDropForeignKeyFormat, fk.SchemaName, fk.TableName, fk.FKName); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("failed-before-repair-ingest-index", func(v failpoint.Value) {
		if v != nil && v.(bool) {
			failpoint.Return(errors.New("failed before repair ingest index"))
		}
	})
	eg, ectx = errgroup.WithContext(ctx)
	mp := console.StartMultiProgress()
	for _, sql := range sqls {
		if sql.IndexRepaired {
			continue
		}
		if ectx.Err() != nil {
			break
		}
		progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)
		w := mp.AddTextBar(progressTitle, 1)
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			defer w.Done()

			indexSession := indexSessions[id%uint64(len(indexSessions))]
			// TODO: When the TiDB supports the DROP and CREATE the same name index in one SQL,
			//   the checkpoint for ingest recorder can be removed and directly use the SQL:
			//      ALTER TABLE db.tbl DROP INDEX `i_1`, ADD IDNEX `i_1` ...
			//
			// This SQL is compatible with checkpoint: If one ingest index has been recreated by
			// the SQL, the index's id would be another one. In the next retry execution, BR can
			// not find the ingest index's dropped id so that BR regards it as a dropped index by
			// restored metakv and then skips repairing it.

			// only when first execution or old index id is not dropped
			if !fromCheckpoint || sql.OldIndexIDFound {
				if err := indexSession.ExecuteInternal(ectx, alterTableDropIndexSQL, sql.SchemaName.O, sql.TableName.O, sql.IndexName); err != nil {
					return errors.Trace(err)
				}
			}
			failpoint.Inject("failed-before-create-ingest-index", func(v failpoint.Value) {
				if v != nil && v.(bool) {
					failpoint.Return(errors.New("failed before create ingest index"))
				}
			})
			// create the repaired index when first execution or not found it
			if err := indexSession.ExecuteInternal(ectx, sql.AddSQL, sql.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			w.Increment()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	eg, ectx = errgroup.WithContext(ctx)
	for _, fk := range fkSqls {
		if fk.ForeignKeyUpdated {
			continue
		}
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			indexSession := indexSessions[id%uint64(len(indexSessions))]
			if err := indexSession.ExecuteInternal(ectx, fk.AddSQL, fk.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("failed-after-repair-ingest-index", func(v failpoint.Value) {
		if v != nil && v.(bool) {
			failpoint.Return(errors.New("failed after repair ingest index"))
		}
	})

	return nil
}

func (rc *LogClient) RecordDeleteRange(sql *stream.PreDelRangeQuery) {
	rc.deleteRangeQueryCh <- sql
}

// use channel to save the delete-range query to make it thread-safety.
func (rc *LogClient) RunGCRowsLoader(ctx context.Context) {
	rc.deleteRangeQueryWaitGroup.Add(1)

	go func() {
		defer rc.deleteRangeQueryWaitGroup.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case query, ok := <-rc.deleteRangeQueryCh:
				if !ok {
					return
				}
				rc.deleteRangeQuery = append(rc.deleteRangeQuery, query)
			}
		}
	}()
}

// InsertGCRows insert the querys into table `gc_delete_range`
func (rc *LogClient) InsertGCRows(ctx context.Context) error {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	ts, err := restore.GetTSWithRetry(ctx, rc.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	jobIDMap := make(map[int64]int64)
	for _, query := range rc.deleteRangeQuery {
		paramsList := make([]any, 0, len(query.ParamsList)*5)
		for _, params := range query.ParamsList {
			newJobID, exists := jobIDMap[params.JobID]
			if !exists {
				newJobID, err = rc.GenGlobalID(ctx)
				if err != nil {
					return errors.Trace(err)
				}
				jobIDMap[params.JobID] = newJobID
			}
			log.Info("insert into the delete range",
				zap.Int64("jobID", newJobID),
				zap.Int64("elemID", params.ElemID),
				zap.String("startKey", params.StartKey),
				zap.String("endKey", params.EndKey),
				zap.Uint64("ts", ts))
			// (job_id, elem_id, start_key, end_key, ts)
			paramsList = append(paramsList, newJobID, params.ElemID, params.StartKey, params.EndKey, ts)
		}
		if len(paramsList) > 0 {
			// trim the ',' behind the query.Sql if exists
			// that's when the rewrite rule of the last table id is not exist
			sql := strings.TrimSuffix(query.Sql, ",")
			if err := rc.unsafeSession.ExecuteInternal(ctx, sql, paramsList...); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// only for unit test
func (rc *LogClient) GetGCRows() []*stream.PreDelRangeQuery {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	return rc.deleteRangeQuery
}

func (rc *LogClient) SaveIdMapWithFailPoints(
	ctx context.Context,
	state *SegmentedPiTRState,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	failpoint.Inject("failed-before-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed before id maps saved"))
	})

	if err := rc.SaveSegmentedPiTRState(ctx, state, logCheckpointMetaManager); err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("failed-after-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed after id maps saved"))
	})
	return nil
}

// called by failpoint, only used for test
// it would print the checksum result into the log, and
// the auto-test script records them to compare another
// cluster's checksum.
func (rc *LogClient) FailpointDoChecksumForLogRestore(
	ctx context.Context,
	kvClient kv.Client,
	pdClient pd.Client,
	rewriteRules map[int64]*restoreutils.RewriteRules,
) (finalErr error) {
	startTS, err := restore.GetTSWithRetry(ctx, rc.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	// set gc safepoint for checksum
	sp := gc.BRServiceSafePoint{
		BackupTS: startTS,
		TTL:      gc.DefaultBRGCSafePointTTL,
		ID:       gc.MakeSafePointID(),
	}
	// Extract keyspaceID from domain storage
	store := rc.GetDomain().Store()
	keyspaceID := tikv.NullspaceID
	if store != nil {
		keyspaceID = store.GetCodec().GetKeyspaceID()
	}
	gcMgr := gc.NewManager(pdClient, keyspaceID)
	cctx, gcSafePointKeeperCancel := context.WithCancel(ctx)
	defer func() {
		log.Info("start to remove gc-safepoint keeper")
		// close the gc safe point keeper at first
		gcSafePointKeeperCancel()
		// remove the gc-safe-point
		if err := gcMgr.DeleteServiceSafePoint(ctx, sp); err != nil {
			log.Warn("failed to remove service safe point, backup may fail if gc triggered",
				zap.Error(err),
			)
		}
		log.Info("finish removing gc-safepoint keeper")
	}()
	err = gc.StartServiceSafePointKeeper(cctx, sp, gcMgr)
	if err != nil {
		return errors.Trace(err)
	}

	eg, ectx := errgroup.WithContext(ctx)
	pool := tidbutil.NewWorkerPool(4, "checksum for log restore")
	infoSchema := rc.GetDomain().InfoSchema()
	// downstream id -> upstream id
	reidRules := make(map[int64]int64)
	for upstreamID, r := range rewriteRules {
		reidRules[r.NewTableID] = upstreamID
	}
	for upstreamID, r := range rewriteRules {
		newTable, ok := infoSchema.TableByID(ctx, r.NewTableID)
		if !ok {
			// a dropped table
			continue
		}
		rewriteRule, ok := rewriteRules[upstreamID]
		if !ok {
			continue
		}
		newTableInfo := newTable.Meta()
		var definitions []model.PartitionDefinition
		if newTableInfo.Partition != nil {
			for _, def := range newTableInfo.Partition.Definitions {
				upid, ok := reidRules[def.ID]
				if !ok {
					log.Panic("no rewrite rule for parition table id", zap.Int64("id", def.ID))
				}
				definitions = append(definitions, model.PartitionDefinition{
					ID: upid,
				})
			}
		}
		oldPartition := &model.PartitionInfo{
			Definitions: definitions,
		}
		oldTable := &metautil.Table{
			Info: &model.TableInfo{
				ID:        upstreamID,
				Indices:   newTableInfo.Indices,
				Partition: oldPartition,
			},
		}
		pool.ApplyOnErrorGroup(eg, func() error {
			exe, err := checksum.NewExecutorBuilder(newTableInfo, startTS).
				SetOldTable(oldTable).
				SetConcurrency(4).
				SetOldKeyspace(rewriteRule.OldKeyspace).
				SetNewKeyspace(rewriteRule.NewKeyspace).
				SetExplicitRequestSourceType(kvutil.ExplicitTypeBR).
				Build()
			if err != nil {
				return errors.Trace(err)
			}
			checksumResp, err := exe.Execute(ectx, kvClient, func() {})
			if err != nil {
				return errors.Trace(err)
			}
			// print to log so that the test script can get final checksum
			log.Info("failpoint checksum completed",
				zap.String("table-name", newTableInfo.Name.O),
				zap.Int64("upstream-id", oldTable.Info.ID),
				zap.Uint64("checksum", checksumResp.Checksum),
				zap.Uint64("total-kvs", checksumResp.TotalKvs),
				zap.Uint64("total-bytes", checksumResp.TotalBytes),
			)
			return nil
		})
	}

	return eg.Wait()
}

func PutRawKvWithRetry(ctx context.Context, client *rawkv.RawKVBatchClient, key, value []byte, originTs uint64) error {
	err := utils.WithRetry(ctx, func() error {
		return client.Put(ctx, key, value, originTs)
	}, utils.NewRawClientBackoffStrategy())
	if err != nil {
		return errors.Errorf("failed to put raw kv after retry")
	}
	return nil
}

// RefreshMetaForTables refreshes metadata for all tables in the schemasReplace map.
// The ordering is critical for dependency management:
// - DELETE operations: tables first, then databases
// - ADD/UPDATE operations: databases first, then tables (to ensure parent exists)
func (rc *LogClient) RefreshMetaForTables(ctx context.Context, schemasReplace *stream.SchemasReplace) error {
	deletedTablesMap := schemasReplace.GetDeletedTables()

	// Step 1: Process DELETIONS in dependency-safe order (tables first, then databases)
	deletedTableCount := 0

	// First, delete all tables
	for upstreamDBID, tableIDsSet := range deletedTablesMap {
		if len(tableIDsSet) > 0 {
			// handle table deletions
			for upstreamTableID := range tableIDsSet {
				dbReplace, ok := schemasReplace.DbReplaceMap[upstreamDBID]
				if !ok {
					return errors.Errorf("the database(upstream ID: %d) of deleted table has no record in replace map", upstreamDBID)
				}
				tableReplace, ok := dbReplace.TableMap[upstreamTableID]
				if !ok {
					return errors.Errorf("the deleted table(upstream ID: %d) has no record in replace map", upstreamTableID)
				}

				args := &model.RefreshMetaArgs{
					SchemaID:      dbReplace.DbID,
					TableID:       tableReplace.TableID,
					InvolvedDB:    dbReplace.Name,
					InvolvedTable: tableReplace.Name,
				}

				log.Info("refreshing deleted table meta",
					zap.Int64("schemaID", dbReplace.DbID),
					zap.String("dbName", dbReplace.Name),
					zap.Any("tableID", tableReplace.TableID),
					zap.String("tableName", tableReplace.Name))
				if err := rc.unsafeSession.RefreshMeta(ctx, args); err != nil {
					return errors.Annotatef(err,
						"failed to refresh meta for deleted table with schemaID=%d, tableID=%d, dbName=%s, tableName=%s",
						dbReplace.DbID, tableReplace.TableID, dbReplace.Name, tableReplace.Name)
				}
				deletedTableCount++
			}
		}
	}

	// Then, delete databases if needed
	for upstreamDBID := range deletedTablesMap {
		// Get database name for logging
		dbReplace, ok := schemasReplace.DbReplaceMap[upstreamDBID]
		if !ok {
			return errors.Errorf("the deleted database(upstream ID: %d) has no record in replace map", upstreamDBID)
		}
		args := &model.RefreshMetaArgs{
			SchemaID:      dbReplace.DbID,
			TableID:       0, // 0 for database-only refresh
			InvolvedDB:    dbReplace.Name,
			InvolvedTable: model.InvolvingAll,
		}

		log.Info("refreshing potential deleted database meta",
			zap.Int64("schemaID", dbReplace.DbID),
			zap.String("dbName", dbReplace.Name))
		if err := rc.unsafeSession.RefreshMeta(ctx, args); err != nil {
			return errors.Annotatef(err, "failed to refresh meta for deleted database with schemaID=%d, dbName=%s",
				dbReplace.DbID, dbReplace.Name)
		}
	}

	log.Info("refreshed metadata for deleted items",
		zap.Int("deletedTableCount", deletedTableCount))

	// Step 2: Process ADD/UPDATE operations in dependency-safe order (databases first, then tables)
	regularCount := 0

	// First, handle database-only operations
	for upstreamDBID, dbReplace := range schemasReplace.DbReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}

		// Skip if we already processed this database in delete section
		if _, alreadyProcessed := deletedTablesMap[upstreamDBID]; alreadyProcessed {
			continue
		}

		args := &model.RefreshMetaArgs{
			SchemaID:      dbReplace.DbID,
			TableID:       0, // tableID = 0 for database-only refresh
			InvolvedDB:    dbReplace.Name,
			InvolvedTable: model.InvolvingAll,
		}
		log.Info("refreshing database-only meta",
			zap.Int64("schemaID", dbReplace.DbID),
			zap.String("dbName", dbReplace.Name))
		if err := rc.unsafeSession.RefreshMeta(ctx, args); err != nil {
			return errors.Annotatef(err, "failed to refresh meta for database with schemaID=%d, dbName=%s",
				dbReplace.DbID, dbReplace.Name)
		}
	}

	// Then, handle table operations
	for upstreamDBID, dbReplace := range schemasReplace.DbReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}

		if len(dbReplace.TableMap) > 0 {
			for upstreamTableID, tableReplace := range dbReplace.TableMap {
				if tableReplace.FilteredOut {
					continue
				}

				// skip if this table is in the deleted list
				if tableIDsSet, dbExists := deletedTablesMap[upstreamDBID]; dbExists {
					if _, isDeleted := tableIDsSet[upstreamTableID]; isDeleted {
						continue
					}
				}

				args := &model.RefreshMetaArgs{
					SchemaID:      dbReplace.DbID,
					TableID:       tableReplace.TableID,
					InvolvedDB:    dbReplace.Name,
					InvolvedTable: tableReplace.Name,
				}
				log.Info("refreshing regular table meta",
					zap.Int64("schemaID", dbReplace.DbID),
					zap.String("dbName", dbReplace.Name),
					zap.Any("tableID", tableReplace.TableID),
					zap.String("tableName", tableReplace.Name))
				if err := rc.unsafeSession.RefreshMeta(ctx, args); err != nil {
					return errors.Annotatef(err, "failed to refresh meta for table with schemaID=%d, tableID=%d, dbName=%s, tableName=%s",
						dbReplace.DbID, tableReplace.TableID, dbReplace.Name, tableReplace.Name)
				}
				regularCount++
			}
		}
	}

	log.Info("refreshed metadata for add/update operations",
		zap.Int("regularTableCount", regularCount))
	return nil
}
