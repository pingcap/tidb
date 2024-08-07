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

package snapclient

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/codec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvMode int

const (
	TiDB KvMode = iota
	Raw
	Txn
)

const (
	// Todo: make it configable
	gRPCTimeOut = 25 * time.Minute
)

// RewriteMode is a mode flag that tells the TiKV how to handle the rewrite rules.
type RewriteMode int

const (
	// RewriteModeLegacy means no rewrite rule is applied.
	RewriteModeLegacy RewriteMode = iota

	// RewriteModeKeyspace means the rewrite rule could be applied to keyspace.
	RewriteModeKeyspace
)

type storeTokenChannelMap struct {
	sync.RWMutex
	tokens map[uint64]chan struct{}
}

func (s *storeTokenChannelMap) acquireTokenCh(storeID uint64, bufferSize uint) chan struct{} {
	s.RLock()
	tokenCh, ok := s.tokens[storeID]
	// handle the case that the store is new-scaled in the cluster
	if !ok {
		s.RUnlock()
		s.Lock()
		// Notice: worker channel can't replaced, because it is still used after unlock.
		if tokenCh, ok = s.tokens[storeID]; !ok {
			tokenCh = utils.BuildWorkerTokenChannel(bufferSize)
			s.tokens[storeID] = tokenCh
		}
		s.Unlock()
	} else {
		s.RUnlock()
	}
	return tokenCh
}

func (s *storeTokenChannelMap) ShouldBlock() bool {
	s.RLock()
	defer s.RUnlock()
	if len(s.tokens) == 0 {
		// never block if there is no store worker pool
		return false
	}
	for _, pool := range s.tokens {
		if len(pool) > 0 {
			// At least one store worker pool has available worker
			return false
		}
	}
	return true
}

func newStoreTokenChannelMap(stores []*metapb.Store, bufferSize uint) *storeTokenChannelMap {
	storeTokenChannelMap := &storeTokenChannelMap{
		sync.RWMutex{},
		make(map[uint64]chan struct{}),
	}
	if bufferSize == 0 {
		return storeTokenChannelMap
	}
	for _, store := range stores {
		ch := utils.BuildWorkerTokenChannel(bufferSize)
		storeTokenChannelMap.tokens[store.Id] = ch
	}
	return storeTokenChannelMap
}

type SnapFileImporter struct {
	metaClient   split.SplitClient
	importClient importclient.ImporterClient
	backend      *backuppb.StorageBackend

	downloadTokensMap *storeTokenChannelMap
	ingestTokensMap   *storeTokenChannelMap

	concurrencyPerStore uint

	kvMode      KvMode
	rawStartKey []byte
	rawEndKey   []byte
	rewriteMode RewriteMode

	cacheKey string
	cond     *sync.Cond
}

func NewSnapFileImporter(
	ctx context.Context,
	metaClient split.SplitClient,
	importClient importclient.ImporterClient,
	backend *backuppb.StorageBackend,
	isRawKvMode bool,
	isTxnKvMode bool,
	tikvStores []*metapb.Store,
	rewriteMode RewriteMode,
	concurrencyPerStore uint,
) (*SnapFileImporter, error) {
	kvMode := TiDB
	if isRawKvMode {
		kvMode = Raw
	}
	if isTxnKvMode {
		kvMode = Txn
	}

	fileImporter := &SnapFileImporter{
		metaClient:          metaClient,
		backend:             backend,
		importClient:        importClient,
		downloadTokensMap:   newStoreTokenChannelMap(tikvStores, concurrencyPerStore),
		ingestTokensMap:     newStoreTokenChannelMap(tikvStores, concurrencyPerStore),
		kvMode:              kvMode,
		rewriteMode:         rewriteMode,
		cacheKey:            fmt.Sprintf("BR-%s-%d", time.Now().Format("20060102150405"), rand.Int63()),
		concurrencyPerStore: concurrencyPerStore,
		cond:                sync.NewCond(new(sync.Mutex)),
	}

	err := fileImporter.checkMultiIngestSupport(ctx, tikvStores)
	return fileImporter, errors.Trace(err)
}

func (importer *SnapFileImporter) WaitUntilUnblock() {
	importer.cond.L.Lock()
	for importer.ShouldBlock() {
		// wait for download worker notified
		importer.cond.Wait()
	}
	importer.cond.L.Unlock()
}

func (importer *SnapFileImporter) ShouldBlock() bool {
	if importer != nil {
		return importer.downloadTokensMap.ShouldBlock() || importer.ingestTokensMap.ShouldBlock()
	}
	return false
}

func (importer *SnapFileImporter) releaseToken(tokenCh chan struct{}) {
	tokenCh <- struct{}{}
	// finish the task, notify the main goroutine to continue
	importer.cond.L.Lock()
	importer.cond.Signal()
	importer.cond.L.Unlock()
}

func (importer *SnapFileImporter) Close() error {
	if importer != nil && importer.importClient != nil {
		return importer.importClient.CloseGrpcClient()
	}
	return nil
}

func (importer *SnapFileImporter) SetDownloadSpeedLimit(ctx context.Context, storeID, rateLimit uint64) error {
	req := &import_sstpb.SetDownloadSpeedLimitRequest{
		SpeedLimit: rateLimit,
	}
	_, err := importer.importClient.SetDownloadSpeedLimit(ctx, storeID, req)
	return errors.Trace(err)
}

// checkMultiIngestSupport checks whether all stores support multi-ingest
func (importer *SnapFileImporter) checkMultiIngestSupport(ctx context.Context, tikvStores []*metapb.Store) error {
	storeIDs := make([]uint64, 0, len(tikvStores))
	for _, s := range tikvStores {
		if s.State != metapb.StoreState_Up {
			continue
		}
		storeIDs = append(storeIDs, s.Id)
	}

	if err := importer.importClient.CheckMultiIngestSupport(ctx, storeIDs); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SetRawRange sets the range to be restored in raw kv mode.
func (importer *SnapFileImporter) SetRawRange(startKey, endKey []byte) error {
	if importer.kvMode != Raw {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "file importer is not in raw kv mode")
	}
	importer.rawStartKey = startKey
	importer.rawEndKey = endKey
	return nil
}

func getKeyRangeByMode(mode KvMode) func(f *backuppb.File, rules *restoreutils.RewriteRules) ([]byte, []byte, error) {
	switch mode {
	case Raw:
		return func(f *backuppb.File, rules *restoreutils.RewriteRules) ([]byte, []byte, error) {
			return f.GetStartKey(), f.GetEndKey(), nil
		}
	case Txn:
		return func(f *backuppb.File, rules *restoreutils.RewriteRules) ([]byte, []byte, error) {
			start, end := f.GetStartKey(), f.GetEndKey()
			if len(start) != 0 {
				start = codec.EncodeBytes([]byte{}, f.GetStartKey())
			}
			if len(end) != 0 {
				end = codec.EncodeBytes([]byte{}, f.GetEndKey())
			}
			return start, end, nil
		}
	default:
		return func(f *backuppb.File, rules *restoreutils.RewriteRules) ([]byte, []byte, error) {
			return restoreutils.GetRewriteRawKeys(f, rules)
		}
	}
}

// getKeyRangeForFiles gets the maximum range on files.
func (importer *SnapFileImporter) getKeyRangeForFiles(
	files []*backuppb.File,
	rewriteRules *restoreutils.RewriteRules,
) ([]byte, []byte, error) {
	var (
		startKey, endKey []byte
		start, end       []byte
		err              error
	)
	getRangeFn := getKeyRangeByMode(importer.kvMode)
	for _, f := range files {
		start, end, err = getRangeFn(f, rewriteRules)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(startKey) == 0 || bytes.Compare(start, startKey) < 0 {
			startKey = start
		}
		if len(endKey) == 0 || bytes.Compare(endKey, end) < 0 {
			endKey = end
		}
	}

	log.Debug("rewrite file keys", logutil.Files(files),
		logutil.Key("startKey", startKey), logutil.Key("endKey", endKey))
	return startKey, endKey, nil
}

// ImportSSTFiles tries to import a file.
// All rules must contain encoded keys.
func (importer *SnapFileImporter) ImportSSTFiles(
	ctx context.Context,
	files []*backuppb.File,
	rewriteRules *restoreutils.RewriteRules,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) error {
	start := time.Now()
	log.Debug("import file", logutil.Files(files))

	// Rewrite the start key and end key of file to scan regions
	startKey, endKey, err := importer.getKeyRangeForFiles(files, rewriteRules)
	if err != nil {
		return errors.Trace(err)
	}

	err = utils.WithRetry(ctx, func() error {
		// Scan regions covered by the file range
		regionInfos, errScanRegion := split.PaginateScanRegion(
			ctx, importer.metaClient, startKey, endKey, split.ScanRegionPaginationLimit)
		if errScanRegion != nil {
			return errors.Trace(errScanRegion)
		}

		log.Debug("scan regions", logutil.Files(files), zap.Int("count", len(regionInfos)))
		// Try to download and ingest the file in every region
	regionLoop:
		for _, regionInfo := range regionInfos {
			info := regionInfo
			// Try to download file.
			downloadMetas, errDownload := importer.download(ctx, info, files, rewriteRules, cipher, apiVersion)
			if errDownload != nil {
				for _, e := range multierr.Errors(errDownload) {
					switch errors.Cause(e) { // nolint:errorlint
					case berrors.ErrKVRewriteRuleNotFound, berrors.ErrKVRangeIsEmpty:
						// Skip this region
						log.Warn("download file skipped",
							logutil.Files(files),
							logutil.Region(info.Region),
							logutil.Key("startKey", startKey),
							logutil.Key("endKey", endKey),
							logutil.Key("file-simple-start", files[0].StartKey),
							logutil.Key("file-simple-end", files[0].EndKey),
							logutil.ShortError(e))
						continue regionLoop
					}
				}
				log.Warn("download file failed, retry later",
					logutil.Files(files),
					logutil.Region(info.Region),
					logutil.Key("startKey", startKey),
					logutil.Key("endKey", endKey),
					logutil.ShortError(errDownload))
				return errors.Trace(errDownload)
			}
			log.Debug("download file done",
				zap.String("file-sample", files[0].Name), zap.Stringer("take", time.Since(start)),
				logutil.Key("start", files[0].StartKey), logutil.Key("end", files[0].EndKey))
			start = time.Now()
			if errIngest := importer.ingest(ctx, files, info, downloadMetas); errIngest != nil {
				log.Warn("ingest file failed, retry later",
					logutil.Files(files),
					logutil.SSTMetas(downloadMetas),
					logutil.Region(info.Region),
					zap.Error(errIngest))
				return errors.Trace(errIngest)
			}
			log.Debug("ingest file done", zap.String("file-sample", files[0].Name), zap.Stringer("take", time.Since(start)))
		}

		for _, f := range files {
			summary.CollectSuccessUnit(summary.TotalKV, 1, f.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, 1, f.TotalBytes)
		}
		return nil
	}, utils.NewImportSSTBackoffer())
	if err != nil {
		log.Error("import sst file failed after retry, stop the whole progress", logutil.Files(files), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// getSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst conn.
// The range of the returned sst meta is [regionRule.NewKeyPrefix, append(regionRule.NewKeyPrefix, 0xff)].
func getSSTMetaFromFile(
	id []byte,
	file *backuppb.File,
	region *metapb.Region,
	regionRule *import_sstpb.RewriteRule,
	rewriteMode RewriteMode,
) (meta *import_sstpb.SSTMeta, err error) {
	r := *region
	// If the rewrite mode is for keyspace, then the region bound should be decoded.
	if rewriteMode == RewriteModeKeyspace {
		if len(region.GetStartKey()) > 0 {
			_, r.StartKey, err = codec.DecodeBytes(region.GetStartKey(), nil)
			if err != nil {
				return
			}
		}
		if len(region.GetEndKey()) > 0 {
			_, r.EndKey, err = codec.DecodeBytes(region.GetEndKey(), nil)
			if err != nil {
				return
			}
		}
	}

	// Get the column family of the file by the file name.
	var cfName string
	if strings.Contains(file.GetName(), restoreutils.DefaultCFName) {
		cfName = restoreutils.DefaultCFName
	} else if strings.Contains(file.GetName(), restoreutils.WriteCFName) {
		cfName = restoreutils.WriteCFName
	}
	// Find the overlapped part between the file and the region.
	// Here we rewrites the keys to compare with the keys of the region.
	rangeStart := regionRule.GetNewKeyPrefix()
	//  rangeStart = max(rangeStart, region.StartKey)
	if bytes.Compare(rangeStart, r.GetStartKey()) < 0 {
		rangeStart = r.GetStartKey()
	}

	// Append 10 * 0xff to make sure rangeEnd cover all file key
	// If choose to regionRule.NewKeyPrefix + 1, it may cause WrongPrefix here
	// https://github.com/tikv/tikv/blob/970a9bf2a9ea782a455ae579ad237aaf6cb1daec/
	// components/sst_importer/src/sst_importer.rs#L221
	suffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), suffix...)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(r.GetEndKey()) > 0 && bytes.Compare(rangeEnd, r.GetEndKey()) > 0 {
		rangeEnd = r.GetEndKey()
	}

	if bytes.Compare(rangeStart, rangeEnd) > 0 {
		log.Panic("range start exceed range end",
			logutil.File(file),
			logutil.Key("startKey", rangeStart),
			logutil.Key("endKey", rangeEnd))
	}

	log.Debug("get sstMeta",
		logutil.Region(region),
		logutil.File(file),
		logutil.Key("startKey", rangeStart),
		logutil.Key("endKey", rangeEnd))

	return &import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
		Length:      file.GetSize_(),
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		CipherIv:    file.GetCipherIv(),
	}, nil
}

// a new way to download ssts files
// 1. download write + default sst files at peer level.
// 2. control the download concurrency per store.
func (importer *SnapFileImporter) download(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	files []*backuppb.File,
	rewriteRules *restoreutils.RewriteRules,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) ([]*import_sstpb.SSTMeta, error) {
	var (
		downloadMetas = make([]*import_sstpb.SSTMeta, 0, len(files))
	)
	errDownload := utils.WithRetry(ctx, func() error {
		var e error
		// we treat Txn kv file as Raw kv file. because we don't have table id to decode
		if importer.kvMode == Raw || importer.kvMode == Txn {
			downloadMetas, e = importer.downloadRawKVSST(ctx, regionInfo, files, cipher, apiVersion)
		} else {
			downloadMetas, e = importer.downloadSST(ctx, regionInfo, files, rewriteRules, cipher, apiVersion)
		}

		failpoint.Inject("restore-storage-error", func(val failpoint.Value) {
			msg := val.(string)
			log.Debug("failpoint restore-storage-error injected.", zap.String("msg", msg))
			e = errors.Annotate(e, msg)
		})
		failpoint.Inject("restore-gRPC-error", func(_ failpoint.Value) {
			log.Warn("the connection to TiKV has been cut by a neko, meow :3")
			e = status.Error(codes.Unavailable, "the connection to TiKV has been cut by a neko, meow :3")
		})
		if isDecryptSstErr(e) {
			log.Info("fail to decrypt when download sst, try again with no-crypt", logutil.Files(files))
			if importer.kvMode == Raw || importer.kvMode == Txn {
				downloadMetas, e = importer.downloadRawKVSST(ctx, regionInfo, files, nil, apiVersion)
			} else {
				downloadMetas, e = importer.downloadSST(ctx, regionInfo, files, rewriteRules, nil, apiVersion)
			}
		}
		if e != nil {
			return errors.Trace(e)
		}

		return nil
	}, utils.NewDownloadSSTBackoffer())

	return downloadMetas, errDownload
}

func (importer *SnapFileImporter) buildDownloadRequest(
	file *backuppb.File,
	rewriteRules *restoreutils.RewriteRules,
	regionInfo *split.RegionInfo,
	cipher *backuppb.CipherInfo,
) (*import_sstpb.DownloadRequest, import_sstpb.SSTMeta, error) {
	uid := uuid.New()
	id := uid[:]
	// Get the rewrite rule for the file.
	fileRule := restoreutils.FindMatchedRewriteRule(file, rewriteRules)
	if fileRule == nil {
		return nil, import_sstpb.SSTMeta{}, errors.Trace(berrors.ErrKVRewriteRuleNotFound)
	}

	// For the legacy version of TiKV, we need to encode the key prefix, since in the legacy
	// version, the TiKV will rewrite the key with the encoded prefix without decoding the keys in
	// the SST file. For the new version of TiKV that support keyspace rewrite, we don't need to
	// encode the key prefix. The TiKV will decode the keys in the SST file and rewrite the keys
	// with the plain prefix and encode the keys before writing to SST.

	// for the keyspace rewrite mode
	rule := *fileRule
	// for the legacy rewrite mode
	if importer.rewriteMode == RewriteModeLegacy {
		rule.OldKeyPrefix = restoreutils.EncodeKeyPrefix(fileRule.GetOldKeyPrefix())
		rule.NewKeyPrefix = restoreutils.EncodeKeyPrefix(fileRule.GetNewKeyPrefix())
	}

	sstMeta, err := getSSTMetaFromFile(id, file, regionInfo.Region, &rule, importer.rewriteMode)
	if err != nil {
		return nil, import_sstpb.SSTMeta{}, err
	}

	req := &import_sstpb.DownloadRequest{
		Sst:            *sstMeta,
		StorageBackend: importer.backend,
		Name:           file.GetName(),
		RewriteRule:    rule,
		CipherInfo:     cipher,
		StorageCacheId: importer.cacheKey,
		// For the older version of TiDB, the request type will  be default to `import_sstpb.RequestType_Legacy`
		RequestType: import_sstpb.DownloadRequestType_Keyspace,
		Context: &kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: "", // TODO,
			},
			RequestSource: kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR),
		},
	}
	return req, *sstMeta, nil
}

func (importer *SnapFileImporter) downloadSST(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	files []*backuppb.File,
	rewriteRules *restoreutils.RewriteRules,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) ([]*import_sstpb.SSTMeta, error) {
	var mu sync.Mutex
	downloadMetasMap := make(map[string]import_sstpb.SSTMeta)
	resultMetasMap := make(map[string]*import_sstpb.SSTMeta)
	downloadReqsMap := make(map[string]*import_sstpb.DownloadRequest)
	for _, file := range files {
		req, sstMeta, err := importer.buildDownloadRequest(file, rewriteRules, regionInfo, cipher)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sstMeta.ApiVersion = apiVersion
		downloadMetasMap[file.Name] = sstMeta
		downloadReqsMap[file.Name] = req
	}

	eg, ectx := errgroup.WithContext(ctx)
	for _, p := range regionInfo.Region.GetPeers() {
		peer := p
		eg.Go(func() error {
			tokenCh := importer.downloadTokensMap.acquireTokenCh(peer.GetStoreId(), importer.concurrencyPerStore)
			select {
			case <-ectx.Done():
				return ectx.Err()
			case <-tokenCh:
			}
			defer func() {
				importer.releaseToken(tokenCh)
			}()
			for _, file := range files {
				req, ok := downloadReqsMap[file.Name]
				if !ok {
					return errors.New("not found file key for download request")
				}
				var err error
				var resp *import_sstpb.DownloadResponse
				resp, err = utils.WithRetryV2(ectx, utils.NewDownloadSSTBackoffer(), func(ctx context.Context) (*import_sstpb.DownloadResponse, error) {
					dctx, cancel := context.WithTimeout(ctx, gRPCTimeOut)
					defer cancel()
					return importer.importClient.DownloadSST(dctx, peer.GetStoreId(), req)
				})
				if err != nil {
					return errors.Trace(err)
				}
				if resp.GetError() != nil {
					return errors.Annotate(berrors.ErrKVDownloadFailed, resp.GetError().GetMessage())
				}
				if resp.GetIsEmpty() {
					return errors.Trace(berrors.ErrKVRangeIsEmpty)
				}

				mu.Lock()
				sstMeta, ok := downloadMetasMap[file.Name]
				if !ok {
					mu.Unlock()
					return errors.Errorf("not found file %s for download sstMeta", file.Name)
				}
				sstMeta.Range = &import_sstpb.Range{
					Start: restoreutils.TruncateTS(resp.Range.GetStart()),
					End:   restoreutils.TruncateTS(resp.Range.GetEnd()),
				}
				resultMetasMap[file.Name] = &sstMeta
				mu.Unlock()

				log.Debug("download from peer",
					logutil.Region(regionInfo.Region),
					logutil.File(file),
					logutil.Peer(peer),
					logutil.Key("resp-range-start", resp.Range.Start),
					logutil.Key("resp-range-end", resp.Range.End),
					zap.Bool("resp-isempty", resp.IsEmpty),
					zap.Uint32("resp-crc32", resp.Crc32),
					zap.Int("len files", len(files)),
				)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return maps.Values(resultMetasMap), nil
}

func (importer *SnapFileImporter) downloadRawKVSST(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	files []*backuppb.File,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) ([]*import_sstpb.SSTMeta, error) {
	downloadMetas := make([]*import_sstpb.SSTMeta, 0, len(files))
	for _, file := range files {
		uid := uuid.New()
		id := uid[:]
		// Empty rule
		var rule import_sstpb.RewriteRule
		sstMeta, err := getSSTMetaFromFile(id, file, regionInfo.Region, &rule, RewriteModeLegacy)
		if err != nil {
			return nil, err
		}

		// Cut the SST file's range to fit in the restoring range.
		if bytes.Compare(importer.rawStartKey, sstMeta.Range.GetStart()) > 0 {
			sstMeta.Range.Start = importer.rawStartKey
		}
		if len(importer.rawEndKey) > 0 &&
			(len(sstMeta.Range.GetEnd()) == 0 || bytes.Compare(importer.rawEndKey, sstMeta.Range.GetEnd()) <= 0) {
			sstMeta.Range.End = importer.rawEndKey
			sstMeta.EndKeyExclusive = true
		}
		if bytes.Compare(sstMeta.Range.GetStart(), sstMeta.Range.GetEnd()) > 0 {
			return nil, errors.Trace(berrors.ErrKVRangeIsEmpty)
		}

		req := &import_sstpb.DownloadRequest{
			Sst:            *sstMeta,
			StorageBackend: importer.backend,
			Name:           file.GetName(),
			RewriteRule:    rule,
			IsRawKv:        true,
			CipherInfo:     cipher,
			StorageCacheId: importer.cacheKey,
		}
		log.Debug("download SST", logutil.SSTMeta(sstMeta), logutil.Region(regionInfo.Region))

		var atomicResp atomic.Pointer[import_sstpb.DownloadResponse]
		eg, ectx := errgroup.WithContext(ctx)
		for _, p := range regionInfo.Region.GetPeers() {
			peer := p
			eg.Go(func() error {
				resp, err := importer.importClient.DownloadSST(ectx, peer.GetStoreId(), req)
				if err != nil {
					return errors.Trace(err)
				}
				if resp.GetError() != nil {
					return errors.Annotate(berrors.ErrKVDownloadFailed, resp.GetError().GetMessage())
				}
				if resp.GetIsEmpty() {
					return errors.Trace(berrors.ErrKVRangeIsEmpty)
				}

				atomicResp.Store(resp)
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		downloadResp := atomicResp.Load()
		sstMeta.Range.Start = downloadResp.Range.GetStart()
		sstMeta.Range.End = downloadResp.Range.GetEnd()
		sstMeta.ApiVersion = apiVersion
		downloadMetas = append(downloadMetas, sstMeta)
	}
	return downloadMetas, nil
}

func (importer *SnapFileImporter) ingest(
	ctx context.Context,
	files []*backuppb.File,
	info *split.RegionInfo,
	downloadMetas []*import_sstpb.SSTMeta,
) error {
	tokenCh := importer.ingestTokensMap.acquireTokenCh(info.Leader.GetStoreId(), importer.concurrencyPerStore)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tokenCh:
	}
	defer func() {
		importer.releaseToken(tokenCh)
	}()
	for {
		ingestResp, errIngest := importer.ingestSSTs(ctx, downloadMetas, info)
		if errIngest != nil {
			return errors.Trace(errIngest)
		}

		errPb := ingestResp.GetError()
		switch {
		case errPb == nil:
			return nil
		case errPb.NotLeader != nil:
			// If error is `NotLeader`, update the region info and retry
			var newInfo *split.RegionInfo
			if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
				newInfo = &split.RegionInfo{
					Leader: newLeader,
					Region: info.Region,
				}
			} else {
				for {
					// Slow path, get region from PD
					newInfo, errIngest = importer.metaClient.GetRegion(
						ctx, info.Region.GetStartKey())
					if errIngest != nil {
						return errors.Trace(errIngest)
					}
					if newInfo != nil {
						break
					}
					// do not get region info, wait a second and GetRegion() again.
					log.Warn("ingest get region by key return nil", logutil.Region(info.Region),
						logutil.Files(files),
						logutil.SSTMetas(downloadMetas),
					)
					time.Sleep(time.Second)
				}
			}

			if !split.CheckRegionEpoch(newInfo, info) {
				return errors.Trace(berrors.ErrKVEpochNotMatch)
			}
			log.Debug("ingest sst returns not leader error, retry it",
				logutil.Files(files),
				logutil.SSTMetas(downloadMetas),
				logutil.Region(info.Region),
				zap.Stringer("newLeader", newInfo.Leader))
			info = newInfo
		case errPb.EpochNotMatch != nil:
			// TODO handle epoch not match error
			//      1. retry download if needed
			//      2. retry ingest
			return errors.Trace(berrors.ErrKVEpochNotMatch)
		case errPb.KeyNotInRegion != nil:
			return errors.Trace(berrors.ErrKVKeyNotInRegion)
		default:
			// Other errors like `ServerIsBusy`, `RegionNotFound`, etc. should be retryable
			return errors.Annotatef(berrors.ErrKVIngestFailed, "ingest error %s", errPb)
		}
	}
}

func (importer *SnapFileImporter) ingestSSTs(
	ctx context.Context,
	sstMetas []*import_sstpb.SSTMeta,
	regionInfo *split.RegionInfo,
) (*import_sstpb.IngestResponse, error) {
	leader := regionInfo.Leader
	if leader == nil {
		return nil, errors.Annotatef(berrors.ErrPDLeaderNotFound,
			"region id %d has no leader", regionInfo.Region.Id)
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.GetId(),
		RegionEpoch: regionInfo.Region.GetRegionEpoch(),
		Peer:        leader,
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: "", // TODO,
		},
		RequestSource: kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR),
	}

	req := &import_sstpb.MultiIngestRequest{
		Context: reqCtx,
		Ssts:    sstMetas,
	}
	log.Debug("ingest SSTs", logutil.SSTMetas(sstMetas), logutil.Leader(leader))
	resp, err := importer.importClient.MultiIngest(ctx, leader.GetStoreId(), req)
	return resp, errors.Trace(err)
}

func isDecryptSstErr(err error) bool {
	return err != nil &&
		strings.Contains(err.Error(), "Engine Engine") &&
		strings.Contains(err.Error(), "Corruption: Bad table magic number")
}
