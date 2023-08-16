// Copyright 2023 PingCAP, Inc.
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

package remote

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/sharedisk"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRetryBackoffSecond   = 30
	maxRetryTimes           = 5
	defaultRetryBackoffTime = 3 * time.Second
	// maxWriteAndIngestRetryTimes is the max retry times for write and ingest.
	// A large retry times is for tolerating tikv cluster failures.
	maxWriteAndIngestRetryTimes = 30
	// The max ranges count in a batch to split and scatter.
	maxBatchSplitRanges = 4096
	scanRegionLimit     = 128
	// suppose each KV is about 32 bytes, 16 * units.KiB / 32 = 512
	defaultKVBatchCount = 512
)

const (
	retrySplitMaxWaitTime = 4 * time.Second
)

var (
	// the max total key size in a split region batch.
	// our threshold should be smaller than TiKV's raft max entry size(default is 8MB).
	maxBatchSplitSize = 6 * units.MiB
	// the base exponential backoff time
	// the variable is only changed in unit test for running test faster.
	splitRegionBaseBackOffTime = time.Second
	// the max retry times to split regions.
	splitRetryTimes = 8
)

func NewRemoteBackend(ctx context.Context, remoteCfg *Config, cfg local.BackendConfig, tls *common.TLS,
	tempStorageURI string, jobID int64) (backend.Backend, error) {
	bd, err := storage.ParseBackend(tempStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.New(ctx, bd, &storage.ExternalStorageOptions{})
	pdCtl, err := pdutil.NewPdController(ctx, cfg.PDAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}
	splitCli := split.NewSplitClient(pdCtl.GetPDClient(), tls.TLSConfig(), false)
	importClientFactory := local.NewImportClientFactoryImpl(splitCli, tls, cfg.MaxConnPerStore, cfg.ConnCompressType)
	var pdCliForTiKV *tikvclient.CodecPDClient
	if cfg.KeyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCtl.GetPDClient())
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCtl.GetPDClient(), cfg.KeyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}
	tikvCodec := pdCliForTiKV.GetCodec()
	var writeLimiter local.StoreWriteLimiter
	if cfg.StoreWriteBWLimit > 0 {
		writeLimiter = local.NewStoreWriteLimiter(cfg.StoreWriteBWLimit)
	} else {
		writeLimiter = local.NoopStoreWriteLimiter{}
	}
	bc := &Backend{
		jobID:           jobID,
		externalStorage: extStore,
		config:          remoteCfg,
		mu: struct {
			sync.RWMutex
			maxWriterID int
			writersSeq  map[int]int
			minKey      kv.Key
			maxKey      kv.Key
			totalSize   uint64
		}{
			writersSeq: map[int]int{},
		},
		pdCtl:               pdCtl,
		tls:                 tls,
		BackendConfig:       cfg,
		splitCli:            splitCli,
		importClientFactory: importClientFactory,
		tikvCodec:           tikvCodec,
		bufferPool:          membuf.NewPool(),
		kvPairSlicePool:     newKvPairSlicePool(),
		writeLimiter:        writeLimiter,
		phase:               PhaseUpload,
	}
	if err = bc.checkMultiIngestSupport(ctx); err != nil {
		return nil, common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}
	return bc, nil
}

type Config struct {
	MemQuota       uint64
	ReadBufferSize uint64
	WriteBatchSize int64
	StatSampleKeys int64
	StatSampleSize uint64
	SubtaskCnt     int64
	S3ChunkSize    uint64
}

type Backend struct {
	jobID           int64
	externalStorage storage.ExternalStorage
	config          *Config
	mu              struct {
		sync.RWMutex
		maxWriterID int
		writersSeq  map[int]int
		minKey      kv.Key
		maxKey      kv.Key
		totalSize   uint64
	}
	pdCtl *pdutil.PdController
	tls   *common.TLS
	local.BackendConfig

	importClientFactory local.ImportClientFactory
	splitCli            split.SplitClient
	tikvCodec           tikvclient.Codec
	bufferPool          *membuf.Pool
	kvPairSlicePool     *kvPairSlicePool
	writeLimiter        local.StoreWriteLimiter
	supportMultiIngest  bool
	timestamp           uint64
	startKey            kv.Key
	endKey              kv.Key
	dataFiles           []string
	statsFiles          []string
	regionKeys          [][]byte
	phase               string
	imported            bool
	cnt                 int
	getFirst            bool
}

const (
	PhaseUpload = "upload"
	PhaseImport = "import"
)

func (remote *Backend) SetImportPhase() {
	remote.phase = PhaseImport
}

func (remote *Backend) Close() {
	log.FromContext(context.Background()).Info("close remote backend",
		zap.String("jobID", strconv.FormatInt(remote.jobID, 10)),
		zap.Int64("buffer pool size", remote.bufferPool.TotalSize()))
	remote.importClientFactory.Close()
	remote.pdCtl.Close()
	remote.bufferPool.Destroy()
}

func (remote *Backend) getRegionSplitSizeKeys(ctx context.Context) (regionSplitSize int64, regionSplitKeys int64, err error) {
	cli := remote.pdCtl.GetPDClient()
	tls := remote.tls
	stores, err := cli.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return 0, 0, err
	}
	for _, store := range stores {
		if store.StatusAddress == "" || engine.IsTiFlash(store) {
			continue
		}
		serverInfo := infoschema.ServerInfo{
			Address:    store.Address,
			StatusAddr: store.StatusAddress,
		}
		serverInfo.ResolveLoopBackAddr()
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStore(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		log.FromContext(ctx).Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}

func (remote *Backend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (remote *Backend) ShouldPostProcess() bool {
	return true
}

func (remote *Backend) OpenEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return remote.allocateTSIfNotExists(ctx)
}

func (remote *Backend) CloseEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (remote *Backend) SetRange(ctx context.Context, start, end kv.Key,
	dataFiles, statsFiles []string, regionKeys [][]byte) error {
	log.FromContext(ctx).Info("set ranges", zap.String("start", hex.EncodeToString(start)), zap.String("end", hex.EncodeToString(end)))
	if start.Cmp(end) > 0 {
		return errors.New("invalid range")
	}
	remote.startKey = start
	remote.endKey = end
	remote.dataFiles = dataFiles
	remote.statsFiles = statsFiles
	remote.regionKeys = regionKeys
	return nil
}

func (remote *Backend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	switch remote.phase {
	case PhaseUpload:
		// Do nothing for uploading stage.
		return nil
	case PhaseImport:
		if len(remote.startKey) == 0 || remote.imported {
			// No data.
			return nil
		}
		ranges := generateRanges(remote.regionKeys, remote.startKey, remote.endKey)
		err := remote.importEngine(ctx, engineUUID, ranges, regionSplitSize, regionSplitKeys)
		if err != nil {
			return err
		}
		remote.imported = true
		return nil
	default:
		panic("unreachable")
	}
}

func generateRanges(splitKeys [][]byte, start, end kv.Key) []Range {
	// Check split keys
	var lastKey []byte
	for _, k := range splitKeys {
		if bytes.Compare(k, lastKey) <= 0 {
			log.FromContext(context.Background()).Info("split keys are not sorted")
		}
		lastKey = k
	}
	if len(splitKeys) > 0 {
		if bytes.Compare(start, splitKeys[0]) > 0 {
			log.FromContext(context.Background()).Info("start key is larger than the first split key")
		}
		if bytes.Compare(splitKeys[len(splitKeys)-1], end) > 0 {
			log.FromContext(context.Background()).Info("end key is smaller than the last split key")
		}
	}
	ranges := make([]Range, 0, len(splitKeys)+1)
	ranges = append(ranges, Range{start: start})
	for i := 0; i < len(splitKeys); i++ {
		ranges[len(ranges)-1].end = splitKeys[i]
		var endK []byte
		if i < len(splitKeys)-1 {
			endK = splitKeys[i+1]
		}
		ranges = append(ranges, Range{start: splitKeys[i], end: endK})
	}
	ranges[len(ranges)-1].end = end
	for _, r := range ranges {
		log.FromContext(context.Background()).Info("range", zap.String("start", hex.EncodeToString(r.start)), zap.String("end", hex.EncodeToString(r.end)))
	}
	for _, r := range splitKeys {
		log.FromContext(context.Background()).Info("split key", zap.String("key", hex.EncodeToString(r)))
	}

	return ranges
}

func (remote *Backend) importEngine(ctx context.Context, engineUUID uuid.UUID, regionRanges []Range,
	regionSplitSize, regionSplitKeys int64) error {
	kvRegionSplitSize, kvRegionSplitKeys, err := getRegionSplitSizeKeys(ctx, remote.pdCtl.GetPDClient(), remote.tls)
	if err == nil {
		if kvRegionSplitSize > regionSplitSize {
			regionSplitSize = kvRegionSplitSize
		}
		if kvRegionSplitKeys > regionSplitKeys {
			regionSplitKeys = kvRegionSplitKeys
		}
	} else {
		log.FromContext(ctx).Warn("fail to get region split keys and size", zap.Error(err))
	}

	if len(regionRanges) > 0 && remote.PausePDSchedulerScope == config.PausePDSchedulerScopeTable {
		log.FromContext(ctx).Info("pause pd scheduler of table scope")
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var startKey, endKey []byte
		if len(regionRanges[0].start) > 0 {
			startKey = codec.EncodeBytes(nil, regionRanges[0].start)
		}
		if len(regionRanges[len(regionRanges)-1].end) > 0 {
			endKey = codec.EncodeBytes(nil, regionRanges[len(regionRanges)-1].end)
		}
		done, err := remote.pdCtl.PauseSchedulersByKeyRange(subCtx, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	log.FromContext(ctx).Info("start import engine", zap.Stringer("uuid", engineUUID))

	err = remote.doImport(ctx, regionRanges, regionSplitSize, regionSplitKeys)
	if err == nil {
		log.FromContext(ctx).Info("import engine success", zap.Stringer("uuid", engineUUID))
	}
	return err
}

func (remote *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// TODO(tangenta): remove the data files from distributed storage.
	return nil
}

func (remote *Backend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (remote *Backend) FlushAllEngines(ctx context.Context) error {
	return nil
}

func (remote *Backend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (remote *Backend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID, subtaskID int64) (backend.EngineWriter, error) {
	onClose := func(s *sharedisk.WriterSummary) {
		remote.handleWriterSummary(s)
		log.FromContext(ctx).Info("record boundary key on close",
			zap.Int("writerID", s.WriterID),
			zap.String("min", hex.EncodeToString(s.Min)),
			zap.String("max", hex.EncodeToString(s.Max)),
			zap.Uint64("totalSize", s.TotalSize))
	}
	remote.mu.totalSize = 0
	prefix := filepath.Join(strconv.Itoa(int(remote.jobID)), strconv.Itoa(int(subtaskID)))
	writer := sharedisk.NewWriter(ctx, remote.externalStorage, prefix,
		int(subtaskID), remote.bufferPool, remote.config.MemQuota, remote.config.StatSampleKeys,
		remote.config.StatSampleSize, remote.config.WriteBatchSize, onClose)
	return writer, nil
}

func (remote *Backend) allocWriterID() int {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	seq := remote.mu.maxWriterID
	remote.mu.maxWriterID++
	return seq
}

func (remote *Backend) handleWriterSummary(s *sharedisk.WriterSummary) {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	// Store the current writer sequence for the given writer so that
	// we can remove the correct file in external storage when cleaning up.
	remote.mu.writersSeq[s.WriterID] = s.Seq
	if remote.mu.minKey == nil || (len(s.Min) > 0 && s.Min.Cmp(remote.mu.minKey) < 0) {
		remote.mu.minKey = s.Min
	}
	if remote.mu.maxKey == nil || (len(s.Max) > 0 && s.Max.Cmp(remote.mu.maxKey) > 0) {
		remote.mu.maxKey = s.Max
	}
	remote.mu.totalSize += s.TotalSize
}

func (remote *Backend) GetSummary() (min, max kv.Key, totalSize uint64) {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	return remote.mu.minKey, remote.mu.maxKey, remote.mu.totalSize
}

func (remote *Backend) GetAllRemoteFiles(ctx context.Context) (dataFiles sharedisk.FilePathHandle, statFiles []string, err error) {
	subDir := strconv.Itoa(int(remote.jobID))
	return sharedisk.GetAllFileNames(ctx, remote.externalStorage, subDir)
}

// GetRangeSplitter returns a RangeSplitter that can be used to split the range into multiple subtasks.
func (remote *Backend) GetRangeSplitter(ctx context.Context, dataFiles sharedisk.FilePathHandle,
	statFiles []string, totalKVSize uint64, instanceCnt int) (*sharedisk.RangeSplitter, error) {
	for _, fname := range dataFiles.FlatSlice() {
		log.FromContext(ctx).Info("data file", zap.String("file", fname))
	}
	for _, sname := range statFiles {
		log.FromContext(ctx).Info("stat file", zap.String("file", sname))
	}
	mergePropIter, err := sharedisk.NewMergePropIter(ctx, statFiles, remote.externalStorage)
	if err != nil {
		return nil, err
	}
	// TODO(tangenta): determine the max key and max ways.
	var approxSubtaskCnt uint64
	if remote.config.SubtaskCnt < 1 {
		approxSubtaskCnt = uint64(instanceCnt)
	} else {
		approxSubtaskCnt = uint64(remote.config.SubtaskCnt)
	}
	maxSize := totalKVSize / approxSubtaskCnt
	log.FromContext(ctx).Info("split range", zap.Uint64("totalKVSize", totalKVSize), zap.Uint64("maxSize", maxSize))
	maxSplitRegionSize, maxSplitRegionKey, err := remote.getRegionSplitSizeKeys(ctx)
	if err != nil {
		return nil, err
	}
	rs := sharedisk.NewRangeSplitter(maxSize, math.MaxUint64, math.MaxUint64,
		mergePropIter, dataFiles, maxSplitRegionSize, maxSplitRegionKey)
	return rs, nil
}

func getRegionSplitSizeKeys(ctx context.Context, cli pd.Client, tls *common.TLS) (
	regionSplitSize int64, regionSplitKeys int64, err error) {
	stores, err := cli.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return 0, 0, err
	}
	for _, store := range stores {
		if store.StatusAddress == "" || engine.IsTiFlash(store) {
			continue
		}
		serverInfo := infoschema.ServerInfo{
			Address:    store.Address,
			StatusAddr: store.StatusAddress,
		}
		serverInfo.ResolveLoopBackAddr()
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStore(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		log.FromContext(ctx).Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}

// return region split size, region split keys, error
func getSplitConfFromStore(ctx context.Context, host string, tls *common.TLS) (
	splitSize int64, regionSplitKeys int64, err error) {
	var (
		nested struct {
			Coprocessor struct {
				RegionSplitSize string `json:"region-split-size"`
				RegionSplitKeys int64  `json:"region-split-keys"`
			} `json:"coprocessor"`
		}
	)
	if err := tls.WithHost(host).GetJSON(ctx, "/config", &nested); err != nil {
		return 0, 0, errors.Trace(err)
	}
	splitSize, err = units.FromHumanSize(nested.Coprocessor.RegionSplitSize)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return splitSize, nested.Coprocessor.RegionSplitKeys, nil
}

func (remote *Backend) doImport(ctx context.Context, regionRanges []Range, regionSplitSize, regionSplitKeys int64) error {
	/*
	   [prepareAndSendJob]-----jobToWorkerCh--->[workers]
	                        ^                       |
	                        |                jobFromWorkerCh
	                        |                       |
	                        |                       v
	               [regionJobRetryer]<--[dispatchJobGoroutine]-->done
	*/

	var (
		ctx2, workerCancel = context.WithCancel(ctx)
		// workerCtx.Done() means workflow is canceled by error. It may be caused
		// by calling workerCancel() or workers in workGroup meets error.
		workGroup, workerCtx = errgroup.WithContext(ctx2)
		firstErr             common.OnceError
		// jobToWorkerCh and jobFromWorkerCh are unbuffered so jobs will not be
		// owned by them.
		jobToWorkerCh   = make(chan *regionJob)
		jobFromWorkerCh = make(chan *regionJob)
		// jobWg tracks the number of jobs in this workflow.
		// prepareAndSendJob, workers and regionJobRetryer can own jobs.
		// When cancel on error, the goroutine of above three components have
		// responsibility to Done jobWg of their owning jobs.
		jobWg                sync.WaitGroup
		dispatchJobGoroutine = make(chan struct{})
	)
	defer workerCancel()

	retryer := startRegionJobRetryer(workerCtx, jobToWorkerCh, &jobWg)

	// dispatchJobGoroutine handles processed job from worker, it will only exit
	// when jobFromWorkerCh is closed to avoid worker is blocked on sending to
	// jobFromWorkerCh.
	defer func() {
		// use defer to close jobFromWorkerCh after all workers are exited
		close(jobFromWorkerCh)
		<-dispatchJobGoroutine
	}()
	go func() {
		defer close(dispatchJobGoroutine)
		for {
			job, ok := <-jobFromWorkerCh
			if !ok {
				return
			}
			switch job.stage {
			case regionScanned, wrote:
				job.retryCount++
				if job.retryCount > maxWriteAndIngestRetryTimes {
					firstErr.Set(job.lastRetryableErr)
					workerCancel()
					jobWg.Done()
					continue
				}
				// max retry backoff time: 2+4+8+16+30*26=810s
				sleepSecond := math.Pow(2, float64(job.retryCount))
				if sleepSecond > float64(maxRetryBackoffSecond) {
					sleepSecond = float64(maxRetryBackoffSecond)
				}
				job.waitUntil = time.Now().Add(time.Second * time.Duration(sleepSecond))
				log.FromContext(ctx).Info("put job back to jobCh to retry later",
					logutil.Key("startKey", job.keyRange.start),
					logutil.Key("endKey", job.keyRange.end),
					zap.Stringer("stage", job.stage),
					zap.Int("retryCount", job.retryCount),
					zap.Time("waitUntil", job.waitUntil))
				if !retryer.push(job) {
					// retryer is closed by worker error
					jobWg.Done()
				}
			case ingested:
				jobWg.Done()
			case needRescan:
				panic("should not reach here")
			}
		}
	}()

	for i := 0; i < remote.WorkerConcurrency; i++ {
		workGroup.Go(func() error {
			return remote.startWorker(workerCtx, jobToWorkerCh, jobFromWorkerCh, &jobWg)
		})
	}

	err := remote.prepareAndSendJob(
		workerCtx,
		regionRanges,
		regionSplitSize,
		regionSplitKeys,
		jobToWorkerCh,
		&jobWg,
	)
	if err != nil {
		firstErr.Set(err)
		workerCancel()
		_ = workGroup.Wait()
		return firstErr.Get()
	}

	jobWg.Wait()
	workerCancel()
	firstErr.Set(workGroup.Wait())
	firstErr.Set(ctx.Err())
	return firstErr.Get()
}

// regionJob is dedicated to import the data in [keyRange.start, keyRange.end)
// to a region. The keyRange may be changed when processing because of writing
// partial data to TiKV or region split.
type regionJob struct {
	writeBatch []kvPair
	memBuffer  *membuf.Buffer
	keyRange   Range
	// TODO: check the keyRange so that it's always included in region
	region *split.RegionInfo
	// stage should be updated only by convertStageTo
	stage jobStageTp
	// writeResult is available only in wrote and ingested stage
	writeResult *tikvWriteResult

	regionSplitSize int64
	regionSplitKeys int64
	metrics         *metric.Metrics

	retryCount       int
	waitUntil        time.Time
	lastRetryableErr error
}

type tikvWriteResult struct {
	sstMeta           []*sst.SSTMeta
	count             int64
	totalBytes        int64
	remainingStartKey []byte
}

type jobStageTp string

const (
	regionScanned jobStageTp = "regionScanned"
	wrote         jobStageTp = "wrote"
	ingested      jobStageTp = "ingested"
	needRescan    jobStageTp = "needRescan"
)

func (j jobStageTp) String() string {
	return string(j)
}

// regionJobRetryer is a concurrent-safe queue holding jobs that need to put
// back later, and put back when the regionJob.waitUntil is reached. It maintains
// a heap of jobs internally based on the regionJob.waitUntil field.
type regionJobRetryer struct {
	// lock acquiring order: protectedClosed > protectedQueue > protectedToPutBack
	protectedClosed struct {
		mu     sync.Mutex
		closed bool
	}
	protectedQueue struct {
		mu sync.Mutex
		q  regionJobRetryHeap
	}
	protectedToPutBack struct {
		mu        sync.Mutex
		toPutBack *regionJob
	}
	putBackCh chan<- *regionJob
	reload    chan struct{}
	jobWg     *sync.WaitGroup
}

type regionJobRetryHeap []*regionJob

var _ heap.Interface = (*regionJobRetryHeap)(nil)

func (h *regionJobRetryHeap) Len() int {
	return len(*h)
}

func (h *regionJobRetryHeap) Less(i, j int) bool {
	v := *h
	return v[i].waitUntil.Before(v[j].waitUntil)
}

func (h *regionJobRetryHeap) Swap(i, j int) {
	v := *h
	v[i], v[j] = v[j], v[i]
}

func (h *regionJobRetryHeap) Push(x any) {
	*h = append(*h, x.(*regionJob))
}

func (h *regionJobRetryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// startRegionJobRetryer starts a new regionJobRetryer and it will run in
// background to put the job back to `putBackCh` when job's waitUntil is reached.
// Cancel the `ctx` will stop retryer and `jobWg.Done` will be trigger for jobs
// that are not put back yet.
func startRegionJobRetryer(
	ctx context.Context,
	putBackCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) *regionJobRetryer {
	ret := &regionJobRetryer{
		putBackCh: putBackCh,
		reload:    make(chan struct{}, 1),
		jobWg:     jobWg,
	}
	ret.protectedQueue.q = make(regionJobRetryHeap, 0, 16)
	go ret.run(ctx)
	return ret
}

// run is only internally used, caller should not use it.
func (q *regionJobRetryer) run(ctx context.Context) {
	defer q.close()

	for {
		var front *regionJob
		q.protectedQueue.mu.Lock()
		if len(q.protectedQueue.q) > 0 {
			front = q.protectedQueue.q[0]
		}
		q.protectedQueue.mu.Unlock()

		switch {
		case front != nil:
			select {
			case <-ctx.Done():
				return
			case <-q.reload:
			case <-time.After(time.Until(front.waitUntil)):
				q.protectedQueue.mu.Lock()
				q.protectedToPutBack.mu.Lock()
				q.protectedToPutBack.toPutBack = heap.Pop(&q.protectedQueue.q).(*regionJob)
				// release the lock of queue to avoid blocking regionJobRetryer.push
				q.protectedQueue.mu.Unlock()

				// hold the lock of toPutBack to make sending to putBackCh and
				// resetting toPutBack atomic w.r.t. regionJobRetryer.close
				select {
				case <-ctx.Done():
					q.protectedToPutBack.mu.Unlock()
					return
				case q.putBackCh <- q.protectedToPutBack.toPutBack:
					q.protectedToPutBack.toPutBack = nil
					q.protectedToPutBack.mu.Unlock()
				}
			}
		default:
			// len(q.q) == 0
			select {
			case <-ctx.Done():
				return
			case <-q.reload:
			}
		}
	}
}

// close is only internally used, caller should not use it.
func (q *regionJobRetryer) close() {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	q.protectedClosed.closed = true

	count := len(q.protectedQueue.q)
	if q.protectedToPutBack.toPutBack != nil {
		count++
	}
	for count > 0 {
		q.jobWg.Done()
		count--
	}
}

// push should not be blocked for long time in any cases.
func (q *regionJobRetryer) push(job *regionJob) bool {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	if q.protectedClosed.closed {
		return false
	}

	q.protectedQueue.mu.Lock()
	heap.Push(&q.protectedQueue.q, job)
	q.protectedQueue.mu.Unlock()

	select {
	case q.reload <- struct{}{}:
	default:
	}
	return true
}

type Range struct {
	start []byte
	end   []byte // end is always exclusive except import_sstpb.SSTMeta
}

func (remote *Backend) startWorker(
	ctx context.Context,
	jobInCh, jobOutCh chan *regionJob,
	jobWg *sync.WaitGroup,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case job, ok := <-jobInCh:
			if !ok {
				// In fact we don't use close input channel to notify worker to
				// exit, because there's a cycle in workflow.
				return nil
			}

			err := remote.executeJob(ctx, job)
			switch job.stage {
			case regionScanned, wrote, ingested:
				//if len(job.writeBatch) > 0 && job.memBuffer != nil {
				//	job.memBuffer.Destroy()
				//	remote.kvPairSlicePool.put(job.writeBatch)
				//}
				jobOutCh <- job
			case needRescan:
				jobs, err2 := remote.generateJobForRange(
					ctx,
					job.keyRange,
					job.regionSplitSize,
					job.regionSplitKeys,
				)
				if err2 != nil {
					// Don't need to put the job back to retry, because generateJobForRange
					// has done the retry internally. Here just done for the "needRescan"
					// job and exit directly.
					jobWg.Done()
					return err2
				}
				// 1 "needRescan" job becomes len(jobs) "regionScanned" jobs.
				jobWg.Add(len(jobs) - 1)
				keyCount := 0
				for _, j := range jobs {
					hasKey := remote.fillRetryJobKVs(ctx, j, job)
					if !hasKey {
						// Skip the job if it doesn't have any key in the range.
						continue
					}
					keyCount += len(j.writeBatch)
					j.lastRetryableErr = job.lastRetryableErr
					jobOutCh <- j
				}

				log.FromContext(ctx).Info("check write batch key count",
					zap.Int("total count", len(job.writeBatch)),
					zap.Int("job key count", keyCount))
			}

			if err != nil {
				return err
			}
		}
	}
}

func (remote *Backend) fillRetryJobKVs(ctx context.Context, newJob, oldJob *regionJob) bool {
	startIdx := sort.Search(len(oldJob.writeBatch), func(i int) bool {
		return bytes.Compare(oldJob.writeBatch[i].key, newJob.keyRange.start) >= 0
	})
	if startIdx == -1 {
		return false
	}
	endIdx := sort.Search(len(oldJob.writeBatch), func(i int) bool {
		return bytes.Compare(oldJob.writeBatch[i].key, newJob.keyRange.end) > 0
	})
	if endIdx == -1 {
		newJob.writeBatch = oldJob.writeBatch[startIdx:]
	} else {
		newJob.writeBatch = oldJob.writeBatch[startIdx:endIdx]
	}
	if len(newJob.writeBatch) == 0 {
		return false
	}

	if bytes.Compare(newJob.writeBatch[0].key, newJob.keyRange.start) < 0 {
		log.FromContext(ctx).Info("write batch key range error")
	}
	if bytes.Compare(newJob.writeBatch[len(newJob.writeBatch)-1].key, newJob.keyRange.end) >= 0 {
		log.FromContext(ctx).Info("write batch key range error")
	}
	return true
}

func (remote *Backend) executeJob(
	ctx context.Context,
	job *regionJob,
) error {
	if remote.ShouldCheckTiKV {
		for _, peer := range job.region.Region.GetPeers() {
			var (
				store *pdtypes.StoreInfo
				err   error
			)
			for i := 0; i < maxRetryTimes; i++ {
				store, err = remote.pdCtl.GetStoreInfo(ctx, peer.StoreId)
				if err != nil {
					continue
				}
				if store.Status.Capacity > 0 {
					// The available disk percent of TiKV
					ratio := store.Status.Available * 100 / store.Status.Capacity
					if ratio < 10 {
						return errors.Errorf("the remaining storage capacity of TiKV(%s) is less than 10%%; please increase the storage capacity of TiKV and try again", store.Store.Address)
					}
				}
				break
			}
			if err != nil {
				log.FromContext(ctx).Error("failed to get StoreInfo from pd http api", zap.Error(err))
			}
		}
	}

	for {
		err := remote.writeToTiKV(ctx, job)
		if err != nil {
			if !remote.isRetryableImportTiKVError(err) {
				return err
			}
			// if it's retryable error, we retry from scanning region
			log.FromContext(ctx).Warn("meet retryable error when writing to TiKV",
				log.ShortError(err), zap.Stringer("job stage", job.stage))
			job.convertStageTo(needRescan)
			job.lastRetryableErr = err
			return nil
		}

		startTime := time.Now()
		err = remote.ingest(ctx, job)
		metrics.GlobalSortMergeDuration.WithLabelValues("ingest").Observe(time.Since(startTime).Seconds())

		if err != nil {
			if !remote.isRetryableImportTiKVError(err) {
				return err
			}
			log.FromContext(ctx).Warn("meet retryable error when ingesting",
				log.ShortError(err), zap.Stringer("job stage", job.stage))
			job.lastRetryableErr = err
			return nil
		}
		// if the job.stage successfully converted into "ingested", it means
		// these data are ingested into TiKV so we handle remaining data.
		// For other job.stage, the job should be sent back to caller to retry
		// later.
		if job.stage != ingested {
			return nil
		}

		if job.writeResult == nil || job.writeResult.remainingStartKey == nil {
			return nil
		}
		job.keyRange.start = job.writeResult.remainingStartKey
		job.convertStageTo(regionScanned)
	}
}

// generateJobForRange will scan the region in `keyRange` and generate region jobs.
// It will retry internally when scan region meet error.
func (remote *Backend) generateJobForRange(
	ctx context.Context,
	keyRange Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	start, end := keyRange.start, keyRange.end
	startKey := codec.EncodeBytes([]byte{}, start)
	endKey := codec.EncodeBytes([]byte{}, end)
	regions, err := split.PaginateScanRegion(ctx, remote.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		log.FromContext(ctx).Error("scan region failed",
			log.ShortError(err), zap.Int("region_len", len(regions)),
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey))
		return nil, err
	}

	jobs := make([]*regionJob, 0, len(regions))
	for _, region := range regions {
		//jobKeyRange := intersectRange(region.Region, Range{start: start, end: end})
		log.FromContext(ctx).Info("get region",
			zap.Uint64("id", region.Region.GetId()),
			zap.Stringer("epoch", region.Region.GetRegionEpoch()),
			logutil.Key("region start", region.Region.GetStartKey()),
			logutil.Key("region end", region.Region.GetEndKey()),
			logutil.Key("job start", startKey),
			logutil.Key("job end", codec.EncodeBytes([]byte{}, end)),
			zap.Reflect("peers", region.Region.GetPeers()))

		jobs = append(jobs, &regionJob{
			keyRange:        intersectRange(region.Region, Range{start: start, end: end}),
			region:          region,
			stage:           regionScanned,
			regionSplitSize: regionSplitSize,
			regionSplitKeys: regionSplitKeys,
		})
	}
	return jobs, nil
}

func (remote *Backend) createMergeIter(ctx context.Context, start kv.Key) (*sharedisk.MergeIter, error) {
	var offsets []uint64
	if len(remote.statsFiles) == 0 {
		offsets = make([]uint64, len(remote.dataFiles))
		log.FromContext(ctx).Info("no stats files",
			zap.String("startKey", hex.EncodeToString(start)))
	} else {
		offs, err := sharedisk.SeekPropsOffsets(ctx, start, remote.statsFiles, remote.externalStorage)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offsets = offs
		log.FromContext(ctx).Info("seek props offsets",
			zap.Uint64s("offsets", offsets),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.Strings("dataFiles", sharedisk.PrettyFileNames(remote.dataFiles)),
			zap.Strings("statsFiles", sharedisk.PrettyFileNames(remote.statsFiles)))
	}

	iter, err := sharedisk.NewMergeIter(ctx, remote.dataFiles, offsets, remote.externalStorage, 64*1024)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Fill in the first key value.
	iter.Next()
	return iter, nil
}

func (remote *Backend) fillJobKVs(j *regionJob, iter *sharedisk.MergeIter) {
	ts := time.Now()
	j.writeBatch = remote.kvPairSlicePool.get()
	memBuf := remote.bufferPool.NewBuffer()

	log.FromContext(context.Background()).Info("", zap.Duration("duration for prepare", time.Since(ts)))
	ts = time.Now()
	//var prevKey kv.Key
	for iter.Valid() {
		k, v := iter.Key(), iter.Value()
		//kBuf := memBuf.AllocBytes(len(k))
		//key := append(kBuf[:0], k...)
		//val := memBuf.AddBytes(v)
		//if len(prevKey) != 0 && bytes.Compare(prevKey, k) >= 0 {
		//	log.FromContext(context.Background()).Error("key is not in order", zap.ByteString("prevKey", prevKey), zap.ByteString("key", k))
		//}
		//prevKey = key
		if bytes.Compare(k, j.keyRange.end) >= 0 {
			if bytes.Compare(k, j.keyRange.start) == 0 {
				remote.cnt++
				kBuf := memBuf.AllocBytes(len(k))
				key := append(kBuf[:0], k...)
				val := memBuf.AddBytes(v)
				j.writeBatch = append(j.writeBatch, kvPair{key: key, val: val})
			}
			if len(j.writeBatch) > 0 {
				lastKey := j.writeBatch[len(j.writeBatch)-1].key
				j.keyRange.end = kv.Key(lastKey).Clone()
			}
			break
		}
		if bytes.Compare(k, j.keyRange.start) >= 0 {
			remote.cnt++
			remote.getFirst = true
			kBuf := memBuf.AllocBytes(len(k))
			key := append(kBuf[:0], k...)
			val := memBuf.AddBytes(v)
			j.writeBatch = append(j.writeBatch, kvPair{key: key, val: val})
		} else {
			if remote.getFirst {
				log.FromContext(context.Background()).Error("unexpected skip key", zap.ByteString("key", k), zap.ByteString("start", j.keyRange.start))
			}
		}
		if !iter.Next() {
			break
		}
	}
	log.FromContext(context.Background()).Info("", zap.Duration("duration for a job", time.Since(ts)))
	j.memBuffer = memBuf
}

// prepareAndSendJob will read the engine to get estimated key range,
// then split and scatter regions for these range and send region jobs to jobToWorkerCh.
// NOTE when ctx is Done, this function will NOT return error even if it hasn't sent
// all the jobs to jobToWorkerCh. This is because the "first error" can only be
// found by checking the work group LATER, we don't want to return an error to
// seize the "first" error.
func (remote *Backend) prepareAndSendJob(
	ctx context.Context,
	initialSplitRanges []Range,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	log.FromContext(ctx).Info("import engine ranges", zap.Int("count", len(initialSplitRanges)))
	if len(initialSplitRanges) == 0 {
		return nil
	}
	var err error
	logger := log.FromContext(ctx).Begin(zap.InfoLevel, "split and scatter ranges")
	for i := 0; i < maxRetryTimes; i++ {
		err = remote.SplitAndScatterRegionInBatches(ctx, initialSplitRanges, true, regionSplitSize, maxBatchSplitRanges)
		if err == nil || common.IsContextCanceledError(err) {
			break
		}
		log.FromContext(ctx).Warn("split and scatter failed in retry",
			log.ShortError(err), zap.Int("retry", i))
	}
	logger.End(zap.ErrorLevel, err)
	if err != nil {
		return err
	}

	return remote.generateAndSendJob(
		ctx,
		initialSplitRanges,
		regionSplitSize,
		regionSplitKeys,
		jobToWorkerCh,
		jobWg,
	)
}

// generateAndSendJob scans the region in ranges and send region jobs to jobToWorkerCh.
func (remote *Backend) generateAndSendJob(
	ctx context.Context,
	jobRanges []Range,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	logger := log.FromContext(ctx)

	// when use dynamic region feature, the region may be very big, we need
	// to split to smaller ranges to increase the concurrency.
	//if regionSplitSize > 2*int64(config.SplitRegionSize) {
	//	sizeProps, err := getSizePropertiesFn(logger, engine.getDB(), remote.keyAdapter)
	//	if err != nil {
	//		return errors.Trace(err)
	//	}
	//
	//	jobRanges = splitRangeBySizeProps(
	//		Range{start: jobRanges[0].start, end: jobRanges[len(jobRanges)-1].end},
	//		sizeProps,
	//		int64(config.SplitRegionSize),
	//		int64(config.SplitRegionKeys))
	//}
	logger.Debug("the ranges length write to tikv", zap.Int("length", len(jobRanges)))

	iter, err := remote.createMergeIter(ctx, jobRanges[0].start)
	if err != nil {
		if common.IsContextCanceledError(err) {
			return nil
		}
		return err
	}
	//nolint: errcheck
	defer iter.Close()
	for _, jobRange := range jobRanges {
		r := jobRange
		ts := time.Now()
		failpoint.Inject("beforeGenerateJob", nil)
		jobs, err := remote.generateJobForRange(ctx, r, regionSplitSize, regionSplitKeys)
		if err != nil {
			if common.IsContextCanceledError(err) {
				return nil
			}
			return err
		}
		for _, job := range jobs {
			log.FromContext(ctx).Info("fillJobKVs", zap.Time("time", time.Now()), zap.Duration("since", time.Since(ts)))
			remote.fillJobKVs(job, iter)
			if len(job.writeBatch) == 0 {
				continue
			}
			jobWg.Add(1)
			select {
			case jobToWorkerCh <- job:
			}
		}
	}
	if iter.Valid() {
		log.FromContext(ctx).Error("engine iterator", zap.ByteString("key", iter.Key()), zap.ByteString("endKey", remote.endKey), zap.Any("cnt", remote.cnt))
	}
	return nil
}

// SplitAndScatterRegionInBatches splits&scatter regions in batches.
// Too many split&scatter requests may put a lot of pressure on TiKV and PD.
func (remote *Backend) SplitAndScatterRegionInBatches(
	ctx context.Context,
	ranges []Range,
	needSplit bool,
	regionSplitSize int64,
	batchCnt int,
) error {
	for i := 0; i < len(ranges); i += batchCnt {
		batch := ranges[i:]
		if len(batch) > batchCnt {
			batch = batch[:batchCnt]
		}
		if err := remote.SplitAndScatterRegionByRanges(ctx, batch, needSplit, regionSplitSize); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SplitAndScatterRegionByRanges include region split & scatter operation just like br.
// we can simply call br function, but we need to change some function signature of br
// When the ranges total size is small, we can skip the split to avoid generate empty regions.
// TODO: remove this file and use br internal functions
func (remote *Backend) SplitAndScatterRegionByRanges(
	ctx context.Context,
	ranges []Range,
	needSplit bool,
	regionSplitSize int64,
) (err error) {
	if len(ranges) == 0 {
		return nil
	}

	if m, ok := metric.FromContext(ctx); ok {
		begin := time.Now()
		defer func() {
			if err == nil {
				m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessSplit).Observe(time.Since(begin).Seconds())
			}
		}()
	}

	minKey := codec.EncodeBytes([]byte{}, ranges[0].start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].end)

	scatterRegions := make([]*split.RegionInfo, 0)
	var retryKeys [][]byte
	waitTime := splitRegionBaseBackOffTime
	skippedKeys := 0
	for i := 0; i < splitRetryTimes; i++ {
		log.FromContext(ctx).Info("split and scatter region",
			logutil.Key("minKey", minKey),
			logutil.Key("maxKey", maxKey),
			zap.Int("retry", i),
		)
		err = nil
		if i > 0 {
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			waitTime *= 2
			if waitTime > retrySplitMaxWaitTime {
				waitTime = retrySplitMaxWaitTime
			}
		}
		var regions []*split.RegionInfo
		regions, err = split.PaginateScanRegion(ctx, remote.splitCli, minKey, maxKey, 128)
		log.FromContext(ctx).Info("paginate scan regions", zap.Int("count", len(regions)),
			logutil.Key("start", minKey), logutil.Key("end", maxKey))
		if err != nil {
			log.FromContext(ctx).Warn("paginate scan region failed", logutil.Key("minKey", minKey), logutil.Key("maxKey", maxKey),
				log.ShortError(err), zap.Int("retry", i))
			continue
		}

		log.FromContext(ctx).Info("paginate scan region finished", logutil.Key("minKey", minKey), logutil.Key("maxKey", maxKey),
			zap.Int("regions", len(regions)))

		if !needSplit {
			scatterRegions = append(scatterRegions, regions...)
			break
		}

		needSplitRanges := make([]Range, 0, len(ranges))
		startKey := make([]byte, 0)
		endKey := make([]byte, 0)
		for _, r := range ranges {
			startKey = codec.EncodeBytes(startKey, r.start)
			endKey = codec.EncodeBytes(endKey, r.end)
			idx := sort.Search(len(regions), func(i int) bool {
				return beforeEnd(startKey, regions[i].Region.EndKey)
			})
			if idx < 0 || idx >= len(regions) {
				log.FromContext(ctx).Error("target region not found", logutil.Key("start_key", startKey),
					logutil.RegionBy("first_region", regions[0].Region),
					logutil.RegionBy("last_region", regions[len(regions)-1].Region))
				return errors.New("target region not found")
			}
			if bytes.Compare(startKey, regions[idx].Region.StartKey) > 0 || bytes.Compare(endKey, regions[idx].Region.EndKey) < 0 {
				needSplitRanges = append(needSplitRanges, r)
			}
		}
		ranges = needSplitRanges
		if len(ranges) == 0 {
			log.FromContext(ctx).Info("no ranges need to be split, skipped.")
			return nil
		}

		//var tableRegionStats map[uint64]int64
		//if tableInfo != nil {
		//	tableRegionStats, err = remote.regionSizeGetter.GetTableRegionSize(ctx, tableInfo.ID)
		//	if err != nil {
		//		log.FromContext(ctx).Warn("fetch table region size statistics failed",
		//			zap.String("table", tableInfo.Name), zap.Error(err))
		//		tableRegionStats, err = make(map[uint64]int64), nil
		//	}
		//}

		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}

		var splitKeyMap map[uint64][][]byte
		if len(retryKeys) > 0 {
			firstKeyEnc := codec.EncodeBytes([]byte{}, retryKeys[0])
			lastKeyEnc := codec.EncodeBytes([]byte{}, retryKeys[len(retryKeys)-1])
			if bytes.Compare(firstKeyEnc, regions[0].Region.StartKey) < 0 || !beforeEnd(lastKeyEnc, regions[len(regions)-1].Region.EndKey) {
				log.FromContext(ctx).Warn("no valid key for split region",
					logutil.Key("firstKey", firstKeyEnc), logutil.Key("lastKey", lastKeyEnc),
					logutil.Key("firstRegionStart", regions[0].Region.StartKey),
					logutil.Key("lastRegionEnd", regions[len(regions)-1].Region.EndKey))
				return errors.New("check split keys failed")
			}
			splitKeyMap = getSplitKeys(retryKeys, regions, log.FromContext(ctx))
			retryKeys = retryKeys[:0]
		} else {
			splitKeyMap = getSplitKeysByRanges(ranges, regions, log.FromContext(ctx))
		}

		type splitInfo struct {
			region *split.RegionInfo
			keys   [][]byte
		}

		var syncLock sync.Mutex
		size := mathutil.Min(len(splitKeyMap), remote.RegionSplitConcurrency)
		ch := make(chan *splitInfo, size)
		eg, splitCtx := errgroup.WithContext(ctx)

		for splitWorker := 0; splitWorker < size; splitWorker++ {
			eg.Go(func() error {
				for sp := range ch {
					var newRegions []*split.RegionInfo
					var err1 error
					region := sp.region
					keys := sp.keys
					slices.SortFunc(keys, func(i, j []byte) bool {
						return bytes.Compare(i, j) < 0
					})
					splitRegion := region
					startIdx := 0
					endIdx := 0
					batchKeySize := 0
					for endIdx <= len(keys) {
						if endIdx == len(keys) ||
							batchKeySize+len(keys[endIdx]) > maxBatchSplitSize ||
							endIdx-startIdx >= remote.RegionSplitBatchSize {
							splitRegionStart := codec.EncodeBytes([]byte{}, keys[startIdx])
							splitRegionEnd := codec.EncodeBytes([]byte{}, keys[endIdx-1])
							if bytes.Compare(splitRegionStart, splitRegion.Region.StartKey) < 0 || !beforeEnd(splitRegionEnd, splitRegion.Region.EndKey) {
								log.FromContext(ctx).Fatal("no valid key in region",
									logutil.Key("startKey", splitRegionStart), logutil.Key("endKey", splitRegionEnd),
									logutil.Key("regionStart", splitRegion.Region.StartKey), logutil.Key("regionEnd", splitRegion.Region.EndKey),
									logutil.Region(splitRegion.Region), logutil.Leader(splitRegion.Leader))
							}
							splitRegion, newRegions, err1 = remote.BatchSplitRegions(splitCtx, splitRegion, keys[startIdx:endIdx])
							if err1 != nil {
								if strings.Contains(err1.Error(), "no valid key") {
									for _, key := range keys {
										log.FromContext(ctx).Warn("no valid key",
											logutil.Key("startKey", region.Region.StartKey),
											logutil.Key("endKey", region.Region.EndKey),
											logutil.Key("key", codec.EncodeBytes([]byte{}, key)))
									}
									return err1
								} else if common.IsContextCanceledError(err1) {
									// do not retry on context.Canceled error
									return err1
								}
								log.FromContext(ctx).Warn("split regions", log.ShortError(err1), zap.Int("retry time", i),
									zap.Uint64("region_id", region.Region.Id))

								syncLock.Lock()
								retryKeys = append(retryKeys, keys[startIdx:]...)
								// set global error so if we exceed retry limit, the function will return this error
								err = multierr.Append(err, err1)
								syncLock.Unlock()
								break
							}
							log.FromContext(ctx).Info("batch split region", zap.Uint64("region_id", splitRegion.Region.Id),
								zap.Int("keys", endIdx-startIdx), zap.Binary("firstKey", keys[startIdx]),
								zap.Binary("end", keys[endIdx-1]))
							slices.SortFunc(newRegions, func(i, j *split.RegionInfo) bool {
								return bytes.Compare(i.Region.StartKey, j.Region.StartKey) < 0
							})
							syncLock.Lock()
							scatterRegions = append(scatterRegions, newRegions...)
							syncLock.Unlock()
							// the region with the max start key is the region need to be further split.
							if bytes.Compare(splitRegion.Region.StartKey, newRegions[len(newRegions)-1].Region.StartKey) < 0 {
								splitRegion = newRegions[len(newRegions)-1]
							}

							batchKeySize = 0
							startIdx = endIdx
						}
						if endIdx < len(keys) {
							batchKeySize += len(keys[endIdx])
						}
						endIdx++
					}
				}
				return nil
			})
		}
	sendLoop:
		for regionID, keys := range splitKeyMap {
			// if region not in tableRegionStats, that means this region is newly split, so
			// we can skip split it again.
			//regionSize, ok := tableRegionStats[regionID]
			//if !ok {
			//	log.FromContext(ctx).Warn("region stats not found", zap.Uint64("region", regionID))
			//}
			if len(keys) == 1 {
				skippedKeys++
			}
			select {
			case ch <- &splitInfo{region: regionMap[regionID], keys: keys}:
			case <-ctx.Done():
				// outer context is canceled, can directly return
				close(ch)
				return ctx.Err()
			case <-splitCtx.Done():
				// met critical error, stop process
				break sendLoop
			}
		}
		close(ch)
		if splitError := eg.Wait(); splitError != nil {
			retryKeys = retryKeys[:0]
			err = splitError
			continue
		}

		if len(retryKeys) == 0 {
			break
		}
		slices.SortFunc(retryKeys, func(i, j []byte) bool {
			return bytes.Compare(i, j) < 0
		})
		minKey = codec.EncodeBytes([]byte{}, retryKeys[0])
		maxKey = codec.EncodeBytes([]byte{}, nextKey(retryKeys[len(retryKeys)-1]))
	}
	if err != nil {
		return errors.Trace(err)
	}

	startTime := time.Now()
	scatterCount, err := remote.waitForScatterRegions(ctx, scatterRegions)
	if scatterCount == len(scatterRegions) {
		log.FromContext(ctx).Info("waiting for scattering regions done",
			zap.Int("skipped_keys", skippedKeys),
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.FromContext(ctx).Info("waiting for scattering regions timeout",
			zap.Int("skipped_keys", skippedKeys),
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)),
			zap.Error(err))
	}
	return nil
}

// BatchSplitRegions will split regions by the given split keys and tries to
// scatter new regions. If split/scatter fails because new region is not ready,
// this function will not return error.
func (remote *Backend) BatchSplitRegions(
	ctx context.Context,
	region *split.RegionInfo,
	keys [][]byte,
) (*split.RegionInfo, []*split.RegionInfo, error) {
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		failpoint.Return(nil, nil, errors.New("retryable error"))
	})
	region, newRegions, err := remote.splitCli.BatchSplitRegionsWithOrigin(ctx, region, keys)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "batch split regions failed")
	}
	var failedErr error
	scatterRegions := newRegions
	backoffer := split.NewWaitRegionOnlineBackoffer().(*split.WaitRegionOnlineBackoffer)
	_ = utils.WithRetry(ctx, func() error {
		retryRegions := make([]*split.RegionInfo, 0)
		for _, region := range scatterRegions {
			// Wait for a while until the regions successfully splits.
			ok, err2 := remote.hasRegion(ctx, region.Region.Id)
			if !ok || err2 != nil {
				failedErr = err2
				if failedErr == nil {
					failedErr = errors.Errorf("region %d not found", region.Region.Id)
				}
				retryRegions = append(retryRegions, region)
				continue
			}
			if err = remote.splitCli.ScatterRegion(ctx, region); err != nil {
				failedErr = err
				retryRegions = append(retryRegions, region)
			}
		}
		if len(retryRegions) == 0 {
			return nil
		}
		// if the number of becomes smaller, we can infer TiKV side really
		// made some progress so don't increase the retry times.
		if len(retryRegions) < len(scatterRegions) {
			backoffer.Stat.ReduceRetry()
		}
		// the scatter operation likely fails because region replicate not finish yet
		// pack them to one log to avoid printing a lot warn logs.
		log.FromContext(ctx).Warn("scatter region failed", zap.Int("regionCount", len(newRegions)),
			zap.Int("failedCount", len(retryRegions)), zap.Error(failedErr))
		scatterRegions = retryRegions
		// although it's not PDBatchScanRegion, WaitRegionOnlineBackoffer will only
		// check this error class so we simply reuse it. Will refine WaitRegionOnlineBackoffer
		// later
		failedErr = errors.Annotatef(berrors.ErrPDBatchScanRegion, "scatter region failed")
		return failedErr
	}, backoffer)

	// TODO: there's still change that we may skip scatter if the retry is timeout.
	return region, newRegions, ctx.Err()
}

func (remote *Backend) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := remote.splitCli.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func getSplitKeysByRanges(ranges []Range, regions []*split.RegionInfo, logger log.Logger) map[uint64][][]byte {
	checkKeys := make([][]byte, 0)
	var lastEnd []byte
	for _, rg := range ranges {
		if !bytes.Equal(lastEnd, rg.start) {
			checkKeys = append(checkKeys, rg.start)
		}
		checkKeys = append(checkKeys, rg.end)
		lastEnd = rg.end
	}
	return getSplitKeys(checkKeys, regions, logger)
}

func getSplitKeys(checkKeys [][]byte, regions []*split.RegionInfo, logger log.Logger) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	for _, key := range checkKeys {
		if region := needSplit(key, regions, logger); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			logger.Debug("get key for split region",
				zap.Binary("key", key),
				zap.Binary("startKey", region.Region.StartKey),
				zap.Binary("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// needSplit checks whether a key is necessary to split, if true returns the split region
func needSplit(key []byte, regions []*split.RegionInfo, logger log.Logger) *split.RegionInfo {
	// If splitKey is the max key.
	if len(key) == 0 {
		return nil
	}
	splitKey := codec.EncodeBytes([]byte{}, key)

	idx := sort.Search(len(regions), func(i int) bool {
		return beforeEnd(splitKey, regions[i].Region.EndKey)
	})
	if idx < len(regions) {
		// If splitKey is in a region
		if bytes.Compare(splitKey, regions[idx].Region.GetStartKey()) > 0 && beforeEnd(splitKey, regions[idx].Region.GetEndKey()) {
			logger.Debug("need split",
				zap.Binary("splitKey", key),
				zap.Binary("encodedKey", splitKey),
				zap.Binary("region start", regions[idx].Region.GetStartKey()),
				zap.Binary("region end", regions[idx].Region.GetEndKey()),
			)
			return regions[idx]
		}
	}
	return nil
}

// return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	// we only do this for IntHandle, which is checked by length
	if tablecodec.IsRecordKey(key) && len(key) == tablecodec.RecordRowKeyLen {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		nextHandle := handle.Next()
		// int handle overflow, use the next table prefix as nextKey
		if nextHandle.Compare(handle) <= 0 {
			return tablecodec.EncodeTablePrefix(tableID + 1)
		}
		return tablecodec.EncodeRowKeyWithHandle(tableID, nextHandle)
	}

	// for index key and CommonHandle, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}

// writeToTiKV writes the data to TiKV and mark this job as wrote stage.
// if any write logic has error, writeToTiKV will set job to a proper stage and return nil. TODO: <-check this
// if any underlying logic has error, writeToTiKV will return an error.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will take the responsibility to do so.
// TODO: let client-go provide a high-level write interface.
func (remote *Backend) writeToTiKV(ctx context.Context, j *regionJob) error {
	if j.stage != regionScanned {
		return nil
	}

	apiVersion := remote.tikvCodec.GetAPIVersion()
	clientFactory := remote.importClientFactory
	kvBatchSize := remote.KVWriteBatchSize
	bufferPool := remote.bufferPool
	writeLimiter := remote.writeLimiter

	begin := time.Now()
	region := j.region.Region

	firstKey := codec.EncodeBytes([]byte{}, j.writeBatch[0].key)
	lastKey := codec.EncodeBytes([]byte{}, j.writeBatch[len(j.writeBatch)-1].key)

	u := uuid.New()
	meta := &sst.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
		ApiVersion: apiVersion,
	}

	annotateErr := func(in error, peer *metapb.Peer) error {
		// annotate the error with peer/store/region info to help debug.
		return errors.Annotatef(in, "peer %d, store %d, region %d, epoch %s", peer.Id, peer.StoreId, region.Id, region.RegionEpoch.String())
	}

	leaderID := j.region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.GetPeers()))
	allPeers := make([]*metapb.Peer, 0, len(region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		cli, err := clientFactory.Create(ctx, peer.StoreId)
		if err != nil {
			return annotateErr(err, peer)
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return annotateErr(err, peer)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return annotateErr(err, peer)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: remote.timestamp,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
		allPeers = append(allPeers, peer)
	}

	bytesBuf := bufferPool.NewBuffer()
	defer bytesBuf.Destroy()
	pairs := make([]*sst.Pair, 0, defaultKVBatchCount)
	count := 0
	size := int64(0)
	totalSize := int64(0)
	totalCount := int64(0)
	// if region-split-size <= 96MiB, we bump the threshold a bit to avoid too many retry split
	// because the range-properties is not 100% accurate
	regionMaxSize := j.regionSplitSize
	if j.regionSplitSize <= int64(config.SplitRegionSize) {
		regionMaxSize = j.regionSplitSize * 4 / 3
	}

	flushKVs := func() error {
		for i := range clients {
			if err := writeLimiter.WaitN(ctx, allPeers[i].StoreId, int(size)); err != nil {
				return errors.Trace(err)
			}
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				res := sst.WriteResponse{}
				newErr := clients[i].RecvMsg(&res)
				log.FromContext(ctx).Error("err", zap.Error(newErr), zap.Any("res", res.Error))

				return annotateErr(err, allPeers[i])
			}
		}
		failpoint.Inject("afterFlushKVs", func() {
			log.FromContext(ctx).Info(fmt.Sprintf("afterFlushKVs count=%d,size=%d", count, size))
		})
		return nil
	}

	log.FromContext(ctx).Info("use writeBatch to create iterator",
		logutil.Key("regionStart", region.StartKey),
		logutil.Key("regionEnd", region.EndKey),
		logutil.Key("jobStart", j.keyRange.start),
		logutil.Key("jobEnd", j.keyRange.end),
		logutil.Key("writeBatchStart", j.writeBatch[0].key),
		logutil.Key("writeBatchEnd", j.writeBatch[len(j.writeBatch)-1].key))
	iter := newSliceIterator(j.writeBatch)

	var remainingStartKey []byte
	startTime := time.Now()
	for iter.Next() {
		//readableKey := hex.EncodeToString(iter.Key())
		//_, _, vals, err := tablecodec.DecodeIndexKey(iter.Key())
		//log.FromContext(ctx).Info("iter", zap.String("key", readableKey), zap.String("colVal", vals[0]), zap.Error(err))
		kvSize := int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if count < len(pairs) {
			pairs[count].Key = bytesBuf.AddBytes(iter.Key())
			pairs[count].Value = bytesBuf.AddBytes(iter.Value())
		} else {
			pair := &sst.Pair{
				Key:   bytesBuf.AddBytes(iter.Key()),
				Value: bytesBuf.AddBytes(iter.Value()),
			}
			pairs = append(pairs, pair)
		}
		count++
		totalCount++
		size += kvSize
		totalSize += kvSize

		if size >= kvBatchSize {
			if err := flushKVs(); err != nil {
				return errors.Trace(err)
			}
			count = 0
			size = 0
			bytesBuf.Reset()
		}
		if totalSize >= regionMaxSize || totalCount >= j.regionSplitKeys {
			// we will shrink the key range of this job to real written range
			if iter.Next() {
				remainingStartKey = append([]byte{}, iter.Key()...)
				j.writeBatch = j.writeBatch[totalCount:]
				log.FromContext(ctx).Info("write to tikv partial finish",
					zap.Int64("count", totalCount),
					zap.Int64("size", totalSize),
					logutil.Key("startKey", j.keyRange.start),
					logutil.Key("endKey", j.keyRange.end),
					logutil.Key("remainStart", remainingStartKey),
					logutil.Region(region),
					logutil.Leader(j.region.Leader))
			}
			break
		}
	}

	if err := iter.Close(); err != nil {
		return errors.Trace(err)
	}

	if iter.Error() != nil {
		return errors.Trace(iter.Error())
	}

	if count > 0 {
		if err := flushKVs(); err != nil {
			return errors.Trace(err)
		}
		count = 0
		size = 0
		bytesBuf.Reset()
	}

	rate := float64(totalSize) / 1024.0 / 1024.0 / (float64(time.Since(startTime).Microseconds()) / 1000000.0)
	log.FromContext(ctx).Info("global sort rate", zap.Any("m/s", rate))
	metrics.GlobalSortMergeSortRate.WithLabelValues("sort before write to TiKV").Observe(rate)
	metrics.GlobalSortMergeSortThroughput.WithLabelValues("sort before write to TiKV").Add(float64(totalSize) / 1024.0 / 1024.0)

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		resp, closeErr := wStream.CloseAndRecv()
		if closeErr != nil {
			return annotateErr(closeErr, allPeers[i])
		}
		if resp.Error != nil {
			return annotateErr(errors.New(resp.Error.Message), allPeers[i])
		}
		if leaderID == region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.FromContext(ctx).Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we don't forward the stage to wrote and let caller
	// handle the retry.
	if len(leaderPeerMetas) == 0 {
		log.FromContext(ctx).Warn("write to tikv no leader",
			logutil.Region(region), logutil.Leader(j.region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize))
		return errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			region.Id, leaderID)
	}

	takeTime := time.Since(begin)
	log.FromContext(ctx).Info("write to kv", zap.Reflect("region", j.region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize),
		zap.Int64("buf_size", bytesBuf.TotalSize()),
		zap.Stringer("takeTime", takeTime))
	if m, ok := metric.FromContext(ctx); ok {
		m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessWrite).Observe(takeTime.Seconds())
	}

	j.writeResult = &tikvWriteResult{
		sstMeta:           leaderPeerMetas,
		count:             totalCount,
		totalBytes:        totalSize,
		remainingStartKey: remainingStartKey,
	}
	j.convertStageTo(wrote)

	metrics.GlobalSortMergeDuration.WithLabelValues("write").Observe(time.Since(startTime).Seconds())
	return nil
}

func (*Backend) isRetryableImportTiKVError(err error) bool {
	err = errors.Cause(err)
	// io.EOF is not retryable in normal case
	// but on TiKV restart, if we're writing to TiKV(through GRPC)
	// it might return io.EOF(it's GRPC Unavailable in most case),
	// we need to retry on this error.
	// see SendMsg in https://pkg.go.dev/google.golang.org/grpc#ClientStream
	if err == io.EOF {
		return true
	}
	return common.IsRetryableError(err)
}

func (j *regionJob) convertStageTo(stage jobStageTp) {
	j.stage = stage
	switch stage {
	case regionScanned:
		j.writeResult = nil
	case ingested:
		// when writing is skipped because key range is empty
		if j.writeResult == nil {
			return
		}
	case needRescan:
		j.region = nil
	}
}

// ingest tries to finish the regionJob.
// if any ingest logic has error, ingest may retry sometimes to resolve it and finally
// set job to a proper stage with nil error returned.
// if any underlying logic has error, ingest will return an error to let caller
// handle it.
func (remote *Backend) ingest(ctx context.Context, j *regionJob) (err error) {
	if j.stage != wrote {
		return nil
	}

	if len(j.writeResult.sstMeta) == 0 {
		j.convertStageTo(ingested)
		return nil
	}

	if m, ok := metric.FromContext(ctx); ok {
		begin := time.Now()
		defer func() {
			if err == nil {
				m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessIngest).Observe(time.Since(begin).Seconds())
			}
		}()
	}

	for retry := 0; retry < maxRetryTimes; retry++ {
		resp, err := remote.doIngest(ctx, j)
		if err == nil && resp.GetError() == nil {
			j.convertStageTo(ingested)
			return nil
		}
		if err != nil {
			if common.IsContextCanceledError(err) {
				return err
			}
			log.FromContext(ctx).Warn("meet underlying error, will retry ingest",
				log.ShortError(err), logutil.SSTMetas(j.writeResult.sstMeta),
				logutil.Region(j.region.Region), logutil.Leader(j.region.Leader))
			continue
		}
		canContinue, err := j.convertStageOnIngestError(resp)
		if common.IsContextCanceledError(err) {
			return err
		}
		if !canContinue {
			log.FromContext(ctx).Warn("meet error and handle the job later",
				zap.Stringer("job stage", j.stage),
				logutil.ShortError(j.lastRetryableErr),
				j.region.ToZapFields(),
				logutil.Key("start", j.keyRange.start),
				logutil.Key("end", j.keyRange.end))
			return nil
		}
		log.FromContext(ctx).Warn("meet error and will doIngest region again",
			logutil.ShortError(j.lastRetryableErr),
			j.region.ToZapFields(),
			logutil.Key("start", j.keyRange.start),
			logutil.Key("end", j.keyRange.end))
	}
	return nil
}

// doIngest send ingest commands to TiKV based on regionJob.writeResult.sstMeta.
// When meet error, it will remove finished sstMetas before return.
func (remote *Backend) doIngest(ctx context.Context, j *regionJob) (*sst.IngestResponse, error) {
	clientFactory := remote.importClientFactory
	supportMultiIngest := remote.supportMultiIngest
	shouldCheckWriteStall := remote.ShouldCheckWriteStall
	if shouldCheckWriteStall {
		writeStall, resp, err := remote.checkWriteStall(ctx, j.region)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if writeStall {
			return resp, nil
		}
	}

	batch := 1
	if supportMultiIngest {
		batch = len(j.writeResult.sstMeta)
	}

	var resp *sst.IngestResponse
	for start := 0; start < len(j.writeResult.sstMeta); start += batch {
		end := mathutil.Min(start+batch, len(j.writeResult.sstMeta))
		ingestMetas := j.writeResult.sstMeta[start:end]

		log.FromContext(ctx).Debug("ingest meta", zap.Reflect("meta", ingestMetas))

		failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
			// only inject the error once
			var resp *sst.IngestResponse

			switch val.(string) {
			case "notleader":
				resp = &sst.IngestResponse{
					Error: &errorpb.Error{
						NotLeader: &errorpb.NotLeader{
							RegionId: j.region.Region.Id,
							Leader:   j.region.Leader,
						},
					},
				}
			case "epochnotmatch":
				resp = &sst.IngestResponse{
					Error: &errorpb.Error{
						EpochNotMatch: &errorpb.EpochNotMatch{
							CurrentRegions: []*metapb.Region{j.region.Region},
						},
					},
				}
			}
			failpoint.Return(resp, nil)
		})

		leader := j.region.Leader
		if leader == nil {
			leader = j.region.Region.GetPeers()[0]
		}

		cli, err := clientFactory.Create(ctx, leader.StoreId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqCtx := &kvrpcpb.Context{
			RegionId:    j.region.Region.GetId(),
			RegionEpoch: j.region.Region.GetRegionEpoch(),
			Peer:        leader,
		}

		if supportMultiIngest {
			req := &sst.MultiIngestRequest{
				Context: reqCtx,
				Ssts:    ingestMetas,
			}
			resp, err = cli.MultiIngest(ctx, req)
		} else {
			req := &sst.IngestRequest{
				Context: reqCtx,
				Sst:     ingestMetas[0],
			}
			resp, err = cli.Ingest(ctx, req)
		}
		if resp.GetError() != nil || err != nil {
			// remove finished sstMetas
			j.writeResult.sstMeta = j.writeResult.sstMeta[start:]
			return resp, errors.Trace(err)
		}
	}
	return resp, nil
}

func (remote *Backend) checkWriteStall(
	ctx context.Context,
	region *split.RegionInfo,
) (bool, *sst.IngestResponse, error) {
	clientFactory := remote.importClientFactory
	for _, peer := range region.Region.GetPeers() {
		cli, err := clientFactory.Create(ctx, peer.StoreId)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		// currently we use empty MultiIngestRequest to check if TiKV is busy.
		// If in future the rate limit feature contains more metrics we can switch to use it.
		resp, err := cli.MultiIngest(ctx, &sst.MultiIngestRequest{})
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if resp.Error != nil && resp.Error.ServerIsBusy != nil {
			return true, resp, nil
		}
	}
	return false, nil, nil
}

// convertStageOnIngestError will try to fix the error contained in ingest response.
// Return (_, error) when another error occurred.
// Return (true, nil) when the job can retry ingesting immediately.
// Return (false, nil) when the job should be put back to queue.
func (j *regionJob) convertStageOnIngestError(
	resp *sst.IngestResponse,
) (bool, error) {
	if resp.GetError() == nil {
		return true, nil
	}

	var newRegion *split.RegionInfo
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		j.lastRetryableErr = common.ErrKVNotLeader.GenWithStack(errPb.GetMessage())

		// meet a problem that the region leader+peer are all updated but the return
		// error is only "NotLeader", we should update the whole region info.
		j.convertStageTo(needRescan)
		return false, nil
	case errPb.EpochNotMatch != nil:
		j.lastRetryableErr = common.ErrKVEpochNotMatch.GenWithStack(errPb.GetMessage())

		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, j.writeResult.sstMeta) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == j.region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &split.RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		if newRegion != nil {
			j.region = newRegion
			j.convertStageTo(regionScanned)
			return false, nil
		}
		j.convertStageTo(needRescan)
		return false, nil
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		j.lastRetryableErr = common.ErrKVRaftProposalDropped.GenWithStack(errPb.GetMessage())

		j.convertStageTo(needRescan)
		return false, nil
	case errPb.ServerIsBusy != nil:
		j.lastRetryableErr = common.ErrKVServerIsBusy.GenWithStack(errPb.GetMessage())

		return false, nil
	case errPb.RegionNotFound != nil:
		j.lastRetryableErr = common.ErrKVRegionNotFound.GenWithStack(errPb.GetMessage())

		j.convertStageTo(needRescan)
		return false, nil
	case errPb.ReadIndexNotReady != nil:
		j.lastRetryableErr = common.ErrKVReadIndexNotReady.GenWithStack(errPb.GetMessage())

		// this error happens when this region is splitting, the error might be:
		//   read index not ready, reason can not read index due to split, region 64037
		// we have paused schedule, but it's temporary,
		// if next request takes a long time, there's chance schedule is enabled again
		// or on key range border, another engine sharing this region tries to split this
		// region may cause this error too.
		j.convertStageTo(needRescan)
		return false, nil
	case errPb.DiskFull != nil:
		j.lastRetryableErr = common.ErrKVIngestFailed.GenWithStack(errPb.GetMessage())

		return false, errors.Errorf("non-retryable error: %s", resp.GetError().GetMessage())
	}
	// all others doIngest error, such as stale command, etc. we'll retry it again from writeAndIngestByRange
	j.lastRetryableErr = common.ErrKVIngestFailed.GenWithStack(resp.GetError().GetMessage())
	j.convertStageTo(regionScanned)
	return false, nil
}

func insideRegion(region *metapb.Region, metas []*sst.SSTMeta) bool {
	inside := true
	for _, meta := range metas {
		rg := meta.GetRange()
		inside = inside && (keyInsideRegion(region, rg.GetStart()) && keyInsideRegion(region, rg.GetEnd()))
	}
	return inside
}

func keyInsideRegion(region *metapb.Region, key []byte) bool {
	return bytes.Compare(key, region.GetStartKey()) >= 0 && (beforeEnd(key, region.GetEndKey()))
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func intersectRange(region *metapb.Region, rg Range) Range {
	var startKey, endKey []byte
	if len(region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.StartKey, []byte{})
	}
	if bytes.Compare(startKey, rg.start) < 0 {
		startKey = rg.start
	}
	if len(region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.EndKey, []byte{})
	}
	if beforeEnd(rg.end, endKey) {
		endKey = rg.end
	}

	return Range{start: startKey, end: endKey}
}

func (remote *Backend) waitForScatterRegions(ctx context.Context, regions []*split.RegionInfo) (scatterCount int, _ error) {
	var (
		retErr    error
		backoffer = split.NewWaitRegionOnlineBackoffer().(*split.WaitRegionOnlineBackoffer)
	)
	// WithRetry will return multierr which is hard to use, so we use `retErr`
	// to save the error needed to return.
	_ = utils.WithRetry(ctx, func() error {
		var retryRegions []*split.RegionInfo
		for _, region := range regions {
			scattered, err := remote.checkRegionScatteredOrReScatter(ctx, region)
			if scattered {
				scatterCount++
				continue
			}
			if err != nil {
				if !common.IsRetryableError(err) {
					log.FromContext(ctx).Warn("wait for scatter region encountered non-retryable error", logutil.Region(region.Region), zap.Error(err))
					retErr = err
					// return nil to stop retry, the error is saved in `retErr`
					return nil
				}
				log.FromContext(ctx).Warn("wait for scatter region encountered error, will retry again", logutil.Region(region.Region), zap.Error(err))
			}
			retryRegions = append(retryRegions, region)
		}
		if len(retryRegions) == 0 {
			regions = retryRegions
			return nil
		}
		if len(retryRegions) < len(regions) {
			backoffer.Stat.ReduceRetry()
		}

		regions = retryRegions
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "wait for scatter region failed")
	}, backoffer)

	if len(regions) > 0 && retErr == nil {
		retErr = errors.Errorf("wait for scatter region timeout, print the first unfinished region %v",
			regions[0].Region.String())
	}
	return scatterCount, retErr
}

func (remote *Backend) checkRegionScatteredOrReScatter(ctx context.Context, regionInfo *split.RegionInfo) (bool, error) {
	resp, err := remote.splitCli.GetOperator(ctx, regionInfo.Region.GetId())
	if err != nil {
		return false, err
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		// TODO: why this is OK?
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Errorf(
			"failed to get region operator, error type: %s, error message: %s",
			respErr.GetType().String(), respErr.GetMessage())
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished.
	if string(resp.GetDesc()) != "scatter-region" {
		return true, nil
	}
	switch resp.GetStatus() {
	case pdpb.OperatorStatus_RUNNING:
		return false, nil
	case pdpb.OperatorStatus_SUCCESS:
		return true, nil
	default:
		log.FromContext(ctx).Debug("scatter-region operator status is abnormal, will scatter region again",
			logutil.Region(regionInfo.Region), zap.Stringer("status", resp.GetStatus()))
		return false, remote.splitCli.ScatterRegion(ctx, regionInfo)
	}
}

func (remote *Backend) checkMultiIngestSupport(ctx context.Context) error {
	stores, err := remote.pdCtl.GetPDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}

	hasTiFlash := false
	for _, s := range stores {
		if s.State == metapb.StoreState_Up && engine.IsTiFlash(s) {
			hasTiFlash = true
			break
		}
	}

	for _, s := range stores {
		// skip stores that are not online
		if s.State != metapb.StoreState_Up || engine.IsTiFlash(s) {
			continue
		}
		var err error
		for i := 0; i < maxRetryTimes; i++ {
			if i > 0 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			client, err1 := remote.getImportClient(ctx, s.Id)
			if err1 != nil {
				err = err1
				log.FromContext(ctx).Warn("get import client failed", zap.Error(err), zap.String("store", s.Address))
				continue
			}
			_, err = client.MultiIngest(ctx, &sst.MultiIngestRequest{})
			if err == nil {
				break
			}
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unimplemented {
					log.FromContext(ctx).Info("multi ingest not support", zap.Any("unsupported store", s))
					remote.supportMultiIngest = false
					return nil
				}
			}
			log.FromContext(ctx).Warn("check multi ingest support failed", zap.Error(err), zap.String("store", s.Address),
				zap.Int("retry", i))
		}
		if err != nil {
			// if the cluster contains no TiFlash store, we don't need the multi-ingest feature,
			// so in this condition, downgrade the logic instead of return an error.
			if hasTiFlash {
				return errors.Trace(err)
			}
			log.FromContext(ctx).Warn("check multi failed all retry, fallback to false", log.ShortError(err))
			remote.supportMultiIngest = false
			return nil
		}
	}

	remote.supportMultiIngest = true
	log.FromContext(ctx).Info("multi ingest support")
	return nil
}

func (remote *Backend) getImportClient(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	return remote.importClientFactory.Create(ctx, storeID)
}

func (remote *Backend) allocateTSIfNotExists(ctx context.Context) error {
	if remote.timestamp > 0 {
		return nil
	}
	physical, logical, err := remote.pdCtl.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)
	remote.timestamp = ts
	return nil
}
