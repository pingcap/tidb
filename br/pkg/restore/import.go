// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	importScanRegionTime = 10 * time.Second
	gRPCBackOffMaxDelay  = 3 * time.Second
)

// RewriteMode is a mode flag that tells the TiKV how to handle the rewrite rules.
type RewriteMode int

const (
	// RewriteModeLegacy means no rewrite rule is applied.
	RewriteModeLegacy RewriteMode = iota

	// RewriteModeKeyspace means the rewrite rule could be applied to keyspace.
	RewriteModeKeyspace
)

// ImporterClient is used to import a file to TiKV.
type ImporterClient interface {
	ClearFiles(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.ClearRequest,
	) (*import_sstpb.ClearResponse, error)

	ApplyKVFile(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.ApplyRequest,
	) (*import_sstpb.ApplyResponse, error)

	DownloadSST(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.DownloadRequest,
	) (*import_sstpb.DownloadResponse, error)

	IngestSST(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.IngestRequest,
	) (*import_sstpb.IngestResponse, error)
	MultiIngest(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.MultiIngestRequest,
	) (*import_sstpb.IngestResponse, error)

	SetDownloadSpeedLimit(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.SetDownloadSpeedLimitRequest,
	) (*import_sstpb.SetDownloadSpeedLimitResponse, error)

	GetImportClient(
		ctx context.Context,
		storeID uint64,
	) (import_sstpb.ImportSSTClient, error)

	SupportMultiIngest(ctx context.Context, stores []uint64) (bool, error)
}

type importClient struct {
	mu         sync.Mutex
	metaClient split.SplitClient
	clients    map[uint64]import_sstpb.ImportSSTClient
	tlsConf    *tls.Config

	keepaliveConf keepalive.ClientParameters
}

// NewImportClient returns a new ImporterClient.
func NewImportClient(metaClient split.SplitClient, tlsConf *tls.Config, keepaliveConf keepalive.ClientParameters) ImporterClient {
	return &importClient{
		metaClient:    metaClient,
		clients:       make(map[uint64]import_sstpb.ImportSSTClient),
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
	}
}

func (ic *importClient) ClearFiles(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.ClearRequest,
) (*import_sstpb.ClearResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.ClearFiles(ctx, req)
}

func (ic *importClient) ApplyKVFile(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.ApplyRequest,
) (*import_sstpb.ApplyResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.Apply(ctx, req)
}

func (ic *importClient) DownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.Download(ctx, req)
}

func (ic *importClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.SetDownloadSpeedLimit(ctx, req)
}

func (ic *importClient) IngestSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.IngestRequest,
) (*import_sstpb.IngestResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.Ingest(ctx, req)
}

func (ic *importClient) MultiIngest(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.MultiIngestRequest,
) (*import_sstpb.IngestResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.MultiIngest(ctx, req)
}

func (ic *importClient) GetImportClient(
	ctx context.Context,
	storeID uint64,
) (import_sstpb.ImportSSTClient, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	client, ok := ic.clients[storeID]
	if ok {
		return client, nil
	}
	store, err := ic.metaClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if ic.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(ic.tlsConf))
	}
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(ic.keepaliveConf),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client = import_sstpb.NewImportSSTClient(conn)
	ic.clients[storeID] = client
	return client, errors.Trace(err)
}

func (ic *importClient) SupportMultiIngest(ctx context.Context, stores []uint64) (bool, error) {
	for _, storeID := range stores {
		_, err := ic.MultiIngest(ctx, storeID, &import_sstpb.MultiIngestRequest{})
		if err != nil {
			if s, ok := status.FromError(err); ok {
				if s.Code() == codes.Unimplemented {
					return false, nil
				}
			}
			return false, errors.Trace(err)
		}
	}
	return true, nil
}

// FileImporter used to import a file to TiKV.
type FileImporter struct {
	metaClient   split.SplitClient
	importClient ImporterClient
	backend      *backuppb.StorageBackend

	isRawKvMode        bool
	rawStartKey        []byte
	rawEndKey          []byte
	supportMultiIngest bool
	rewriteMode        RewriteMode

	cacheKey string
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	metaClient split.SplitClient,
	importClient ImporterClient,
	backend *backuppb.StorageBackend,
	isRawKvMode bool,
	rewriteMode RewriteMode,
) FileImporter {
	return FileImporter{
		metaClient:   metaClient,
		backend:      backend,
		importClient: importClient,
		isRawKvMode:  isRawKvMode,
		rewriteMode:  rewriteMode,
		cacheKey:     fmt.Sprintf("BR-%s-%d", time.Now().Format("20060102150405"), rand.Int63()),
	}
}

// CheckMultiIngestSupport checks whether all stores support multi-ingest
func (importer *FileImporter) CheckMultiIngestSupport(ctx context.Context, pdClient pd.Client) error {
	allStores, err := util.GetAllTiKVStores(ctx, pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	storeIDs := make([]uint64, 0, len(allStores))
	for _, s := range allStores {
		if s.State != metapb.StoreState_Up {
			continue
		}
		storeIDs = append(storeIDs, s.Id)
	}

	support, err := importer.importClient.SupportMultiIngest(ctx, storeIDs)
	if err != nil {
		return errors.Trace(err)
	}
	importer.supportMultiIngest = support
	log.L().Info("multi ingest support", zap.Bool("support", support))
	return nil
}

// SetRawRange sets the range to be restored in raw kv mode.
func (importer *FileImporter) SetRawRange(startKey, endKey []byte) error {
	if !importer.isRawKvMode {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "file importer is not in raw kv mode")
	}
	importer.rawStartKey = startKey
	importer.rawEndKey = endKey
	return nil
}

// getKeyRangeForFiles gets the maximum range on files.
func (importer *FileImporter) getKeyRangeForFiles(
	files []*backuppb.File,
	rewriteRules *RewriteRules,
) ([]byte, []byte, error) {
	var (
		startKey, endKey []byte
		start, end       []byte
		err              error
	)

	for _, f := range files {
		if importer.isRawKvMode {
			start, end = f.GetStartKey(), f.GetEndKey()
		} else {
			start, end, err = GetRewriteRawKeys(f, rewriteRules)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
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

// Import tries to import a file.
func (importer *FileImporter) ImportKVFileForRegion(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	rule *RewriteRules,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	info *split.RegionInfo,
	supportBatch bool,
) RPCResult {
	// Try to download file.
	result := importer.downloadAndApplyKVFile(ctx, files, rule, info, shiftStartTS, startTS, restoreTS, supportBatch)
	if !result.OK() {
		errDownload := result.Err
		for _, e := range multierr.Errors(errDownload) {
			switch errors.Cause(e) { // nolint:errorlint
			case berrors.ErrKVRewriteRuleNotFound, berrors.ErrKVRangeIsEmpty:
				// Skip this region
				logutil.CL(ctx).Warn("download file skipped",
					logutil.Region(info.Region),
					logutil.ShortError(e))
				return RPCResultOK()
			}
		}
		logutil.CL(ctx).Warn("download and apply file failed",
			logutil.ShortError(&result))
		return result
	}
	summary.CollectInt("RegionInvolved", 1)
	return RPCResultOK()
}

func (importer *FileImporter) ClearFiles(ctx context.Context, pdClient pd.Client, prefix string) error {
	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range allStores {
		if s.State != metapb.StoreState_Up {
			continue
		}
		req := &import_sstpb.ClearRequest{
			Prefix: prefix,
		}
		_, err = importer.importClient.ClearFiles(ctx, s.GetId(), req)
		if err != nil {
			log.Warn("cleanup kv files failed", zap.Uint64("store", s.GetId()), zap.Error(err))
		}
	}
	return nil
}

func FilterFilesByRegion(
	files []*backuppb.DataFileInfo,
	ranges []kv.KeyRange,
	r *split.RegionInfo,
) ([]*backuppb.DataFileInfo, error) {
	if len(files) != len(ranges) {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument,
			"count of files no equals count of ranges, file-count:%v, ranges-count:%v",
			len(files), len(ranges))
	}

	output := make([]*backuppb.DataFileInfo, 0, len(files))
	if r != nil && r.Region != nil {
		for i, f := range files {
			if bytes.Compare(r.Region.StartKey, ranges[i].EndKey) <= 0 &&
				(len(r.Region.EndKey) == 0 || bytes.Compare(r.Region.EndKey, ranges[i].StartKey) >= 0) {
				output = append(output, f)
			}
		}
	} else {
		output = files
	}

	return output, nil
}

// ImportKVFiles restores the kv events.
func (importer *FileImporter) ImportKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	rule *RewriteRules,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	supportBatch bool,
) error {
	var (
		startKey []byte
		endKey   []byte
		ranges   = make([]kv.KeyRange, len(files))
		err      error
	)

	if !supportBatch && len(files) > 1 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"do not support batch apply but files count:%v > 1", len(files))
	}
	log.Debug("import kv files", zap.Int("batch file count", len(files)))

	for i, f := range files {
		ranges[i].StartKey, ranges[i].EndKey, err = GetRewriteEncodedKeys(f, rule)
		if err != nil {
			return errors.Trace(err)
		}

		if len(startKey) == 0 || bytes.Compare(ranges[i].StartKey, startKey) < 0 {
			startKey = ranges[i].StartKey
		}
		if len(endKey) == 0 || bytes.Compare(ranges[i].EndKey, endKey) > 0 {
			endKey = ranges[i].EndKey
		}
	}

	log.Debug("rewrite file keys",
		logutil.Key("startKey", startKey), logutil.Key("endKey", endKey))

	// This RetryState will retry 45 time, about 10 min.
	rs := utils.InitialRetryState(45, 100*time.Millisecond, 15*time.Second)
	ctl := OverRegionsInRange(startKey, endKey, importer.metaClient, &rs)
	err = ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		subfiles, errFilter := FilterFilesByRegion(files, ranges, r)
		if errFilter != nil {
			return RPCResultFromError(errFilter)
		}
		if len(subfiles) == 0 {
			return RPCResultOK()
		}
		return importer.ImportKVFileForRegion(ctx, subfiles, rule, shiftStartTS, startTS, restoreTS, r, supportBatch)
	})
	return errors.Trace(err)
}

// ImportSSTFiles tries to import a file.
// All rules must contain encoded keys.
func (importer *FileImporter) ImportSSTFiles(
	ctx context.Context,
	files []*backuppb.File,
	rewriteRules *RewriteRules,
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
		tctx, cancel := context.WithTimeout(ctx, importScanRegionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, errScanRegion := split.PaginateScanRegion(
			tctx, importer.metaClient, startKey, endKey, split.ScanRegionPaginationLimit)
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
				log.Error("download file failed",
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
			if errIngest := importer.ingest(ctx, info, downloadMetas); errIngest != nil {
				log.Error("ingest file failed",
					logutil.Files(files),
					logutil.SSTMetas(downloadMetas),
					logutil.Region(info.Region),
					zap.Error(errIngest))
				return errors.Trace(errIngest)
			}
		}

		log.Debug("ingest file done", zap.String("file-sample", files[0].Name), zap.Stringer("take", time.Since(start)))
		for _, f := range files {
			summary.CollectSuccessUnit(summary.TotalKV, 1, f.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, 1, f.TotalBytes)
		}
		return nil
	}, utils.NewImportSSTBackoffer())
	return errors.Trace(err)
}

func (importer *FileImporter) setDownloadSpeedLimit(ctx context.Context, storeID, rateLimit uint64) error {
	req := &import_sstpb.SetDownloadSpeedLimitRequest{
		SpeedLimit: rateLimit,
	}
	_, err := importer.importClient.SetDownloadSpeedLimit(ctx, storeID, req)
	return errors.Trace(err)
}

func (importer *FileImporter) download(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	files []*backuppb.File,
	rewriteRules *RewriteRules,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) ([]*import_sstpb.SSTMeta, error) {
	var (
		downloadMetas = make([]*import_sstpb.SSTMeta, 0, len(files))
		remainFiles   = files
	)
	errDownload := utils.WithRetry(ctx, func() error {
		var e error
		for i, f := range remainFiles {
			var downloadMeta *import_sstpb.SSTMeta
			if importer.isRawKvMode {
				downloadMeta, e = importer.downloadRawKVSST(ctx, regionInfo, f, cipher, apiVersion)
			} else {
				downloadMeta, e = importer.downloadSST(ctx, regionInfo, f, rewriteRules, cipher, apiVersion)
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
				log.Info("fail to decrypt when download sst, try again with no-crypt", logutil.File(f))
				if importer.isRawKvMode {
					downloadMeta, e = importer.downloadRawKVSST(ctx, regionInfo, f, nil, apiVersion)
				} else {
					downloadMeta, e = importer.downloadSST(ctx, regionInfo, f, rewriteRules, nil, apiVersion)
				}
			}

			if e != nil {
				remainFiles = remainFiles[i:]
				return errors.Trace(e)
			}

			downloadMetas = append(downloadMetas, downloadMeta)
		}

		return nil
	}, utils.NewDownloadSSTBackoffer())

	return downloadMetas, errDownload
}

func (importer *FileImporter) downloadSST(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	file *backuppb.File,
	rewriteRules *RewriteRules,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) (*import_sstpb.SSTMeta, error) {
	uid := uuid.New()
	id := uid[:]
	// Get the rewrite rule for the file.
	fileRule := findMatchedRewriteRule(file, rewriteRules)
	if fileRule == nil {
		return nil, errors.Trace(berrors.ErrKVRewriteRuleNotFound)
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
		rule.OldKeyPrefix = encodeKeyPrefix(fileRule.GetOldKeyPrefix())
		rule.NewKeyPrefix = encodeKeyPrefix(fileRule.GetNewKeyPrefix())
	}

	sstMeta, err := GetSSTMetaFromFile(id, file, regionInfo.Region, &rule, importer.rewriteMode)
	if err != nil {
		return nil, err
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
	}
	log.Debug("download SST",
		logutil.SSTMeta(sstMeta),
		logutil.File(file),
		logutil.Region(regionInfo.Region),
		logutil.Leader(regionInfo.Leader),
	)

	var atomicResp atomic.Value
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

			log.Debug("download from peer",
				logutil.Region(regionInfo.Region),
				logutil.Peer(peer),
				logutil.Key("resp-range-start", resp.Range.Start),
				logutil.Key("resp-range-end", resp.Range.End),
				zap.Bool("resp-isempty", resp.IsEmpty),
				zap.Uint32("resp-crc32", resp.Crc32),
			)
			atomicResp.Store(resp)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	downloadResp := atomicResp.Load().(*import_sstpb.DownloadResponse)
	sstMeta.Range.Start = TruncateTS(downloadResp.Range.GetStart())
	sstMeta.Range.End = TruncateTS(downloadResp.Range.GetEnd())
	sstMeta.ApiVersion = apiVersion
	return sstMeta, nil
}

func (importer *FileImporter) downloadRawKVSST(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	file *backuppb.File,
	cipher *backuppb.CipherInfo,
	apiVersion kvrpcpb.APIVersion,
) (*import_sstpb.SSTMeta, error) {
	uid := uuid.New()
	id := uid[:]
	// Empty rule
	var rule import_sstpb.RewriteRule
	sstMeta, err := GetSSTMetaFromFile(id, file, regionInfo.Region, &rule, RewriteModeLegacy)
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

	var atomicResp atomic.Value
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

	downloadResp := atomicResp.Load().(*import_sstpb.DownloadResponse)
	sstMeta.Range.Start = downloadResp.Range.GetStart()
	sstMeta.Range.End = downloadResp.Range.GetEnd()
	sstMeta.ApiVersion = apiVersion
	return sstMeta, nil
}

func (importer *FileImporter) ingest(
	ctx context.Context,
	info *split.RegionInfo,
	downloadMetas []*import_sstpb.SSTMeta,
) error {
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
					log.Warn("get region by key return nil", logutil.Region(info.Region))
					time.Sleep(time.Second)
				}
			}

			if !split.CheckRegionEpoch(newInfo, info) {
				return errors.Trace(berrors.ErrKVEpochNotMatch)
			}
			log.Debug("ingest sst returns not leader error, retry it",
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

func (importer *FileImporter) ingestSSTs(
	ctx context.Context,
	sstMetas []*import_sstpb.SSTMeta,
	regionInfo *split.RegionInfo,
) (*import_sstpb.IngestResponse, error) {
	leader := regionInfo.Leader
	if leader == nil {
		leader = regionInfo.Region.GetPeers()[0]
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.GetId(),
		RegionEpoch: regionInfo.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	if !importer.supportMultiIngest {
		// TODO: not sure we need this check
		if len(sstMetas) != 1 {
			panic("do not support batch ingest")
		}
		req := &import_sstpb.IngestRequest{
			Context: reqCtx,
			Sst:     sstMetas[0],
		}
		log.Debug("ingest SST", logutil.SSTMeta(sstMetas[0]), logutil.Leader(leader))
		resp, err := importer.importClient.IngestSST(ctx, leader.GetStoreId(), req)
		return resp, errors.Trace(err)
	}

	req := &import_sstpb.MultiIngestRequest{
		Context: reqCtx,
		Ssts:    sstMetas,
	}
	log.Debug("ingest SSTs", logutil.SSTMetas(sstMetas), logutil.Leader(leader))
	resp, err := importer.importClient.MultiIngest(ctx, leader.GetStoreId(), req)
	return resp, errors.Trace(err)
}

func (importer *FileImporter) downloadAndApplyKVFile(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	rules *RewriteRules,
	regionInfo *split.RegionInfo,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	supportBatch bool,
) RPCResult {
	leader := regionInfo.Leader
	if leader == nil {
		return RPCResultFromError(errors.Annotatef(berrors.ErrPDLeaderNotFound,
			"region id %d has no leader", regionInfo.Region.Id))
	}

	metas := make([]*import_sstpb.KVMeta, 0, len(files))
	rewriteRules := make([]*import_sstpb.RewriteRule, 0, len(files))

	for _, file := range files {
		// Get the rewrite rule for the file.
		fileRule := findMatchedRewriteRule(file, rules)
		if fileRule == nil {
			return RPCResultFromError(errors.Annotatef(berrors.ErrKVRewriteRuleNotFound,
				"rewrite rule for file %+v not find (in %+v)", file, rules))
		}
		rule := import_sstpb.RewriteRule{
			OldKeyPrefix: encodeKeyPrefix(fileRule.GetOldKeyPrefix()),
			NewKeyPrefix: encodeKeyPrefix(fileRule.GetNewKeyPrefix()),
		}

		meta := &import_sstpb.KVMeta{
			Name:        file.Path,
			Cf:          file.Cf,
			RangeOffset: file.RangeOffset,
			Length:      file.Length,
			RangeLength: file.RangeLength,
			IsDelete:    file.Type == backuppb.FileType_Delete,
			StartTs: func() uint64 {
				if file.Cf == stream.DefaultCF {
					return shiftStartTS
				}
				return startTS
			}(),
			RestoreTs:       restoreTS,
			StartKey:        regionInfo.Region.GetStartKey(),
			EndKey:          regionInfo.Region.GetEndKey(),
			Sha256:          file.GetSha256(),
			CompressionType: file.CompressionType,
		}

		metas = append(metas, meta)
		rewriteRules = append(rewriteRules, &rule)
	}

	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.GetId(),
		RegionEpoch: regionInfo.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	var req *import_sstpb.ApplyRequest
	if supportBatch {
		req = &import_sstpb.ApplyRequest{
			Metas:          metas,
			StorageBackend: importer.backend,
			RewriteRules:   rewriteRules,
			Context:        reqCtx,
		}
	} else {
		req = &import_sstpb.ApplyRequest{
			Meta:           metas[0],
			StorageBackend: importer.backend,
			RewriteRule:    *rewriteRules[0],
			Context:        reqCtx,
		}
	}

	log.Debug("apply kv file", logutil.Leader(leader))
	resp, err := importer.importClient.ApplyKVFile(ctx, leader.GetStoreId(), req)
	if err != nil {
		return RPCResultFromError(errors.Trace(err))
	}
	if resp.GetError() != nil {
		logutil.CL(ctx).Warn("import meet error", zap.Stringer("error", resp.GetError()))
		return RPCResultFromPBError(resp.GetError())
	}
	return RPCResultOK()
}

func isDecryptSstErr(err error) bool {
	return err != nil &&
		strings.Contains(err.Error(), "Engine Engine") &&
		strings.Contains(err.Error(), "Corruption: Bad table magic number")
}
