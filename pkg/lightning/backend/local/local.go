// Copyright 2020 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"io"
	"math"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/intest"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/retry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout             = 5 * time.Minute
	maxRetryTimes           = 20
	defaultRetryBackoffTime = 3 * time.Second

	gRPCKeepAliveTime    = 10 * time.Minute
	gRPCKeepAliveTimeout = 5 * time.Minute
	gRPCBackOffMaxDelay  = 10 * time.Minute

	// The max ranges count in a batch to split and scatter.
	maxBatchSplitRanges = 4096

	propRangeIndex = "tikv.range_index"

	defaultPropSizeIndexDistance = 4 * units.MiB
	defaultPropKeysIndexDistance = 40 * 1024

	// the lower threshold of max open files for pebble db.
	openFilesLowerThreshold = 128

	duplicateDBName = "duplicates"
	scanRegionLimit = 128
)

var (
	// Local backend is compatible with TiDB [4.0.0, NextMajorVersion).
	localMinTiDBVersion    = *semver.New("4.0.0")
	localMinTiKVVersion    = *semver.New("4.0.0")
	localMinPDVersion      = *semver.New("4.0.0")
	localMaxTiDBVersion    = version.NextMajorVersion()
	localMaxTiKVVersion    = version.NextMajorVersion()
	localMaxPDVersion      = version.NextMajorVersion()
	tiFlashMinVersion      = *semver.New("4.0.5")
	tikvSideFreeSpaceCheck = *semver.New("8.0.0")

	errorEngineClosed     = errors.New("engine is closed")
	maxRetryBackoffSecond = 30

	// MaxWriteAndIngestRetryTimes is the max retry times for write and ingest.
	// A large retry times is for tolerating tikv cluster failures.
	MaxWriteAndIngestRetryTimes = 30

	// Unlimited RPC receive message size for TiKV importer
	unlimitedRPCRecvMsgSize = math.MaxInt32
)

// importClientFactory is factory to create new import client for specific store.
type importClientFactory interface {
	create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error)
	close()
}

type importClientFactoryImpl struct {
	conns           *common.GRPCConns
	splitCli        split.SplitClient
	tls             *common.TLS
	tcpConcurrency  int
	compressionType config.CompressionType
}

func newImportClientFactoryImpl(
	splitCli split.SplitClient,
	tls *common.TLS,
	tcpConcurrency int,
	compressionType config.CompressionType,
) *importClientFactoryImpl {
	return &importClientFactoryImpl{
		conns:           common.NewGRPCConns(),
		splitCli:        splitCli,
		tls:             tls,
		tcpConcurrency:  tcpConcurrency,
		compressionType: compressionType,
	}
}

func (f *importClientFactoryImpl) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := f.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var opts []grpc.DialOption
	if f.tls.TLSConfig() != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(f.tls.TLSConfig())))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	opts = append(opts,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(unlimitedRPCRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	switch f.compressionType {
	case config.CompressionNone:
		// do nothing
	case config.CompressionGzip:
		// Use custom compressor/decompressor to speed up compression/decompression.
		// Note that here we don't use grpc.UseCompressor option although it's the recommended way.
		// Because gprc-go uses a global registry to store compressor/decompressor, we can't make sure
		// the compressor/decompressor is not registered by other components.
		opts = append(opts, grpc.WithCompressor(&gzipCompressor{}), grpc.WithDecompressor(&gzipDecompressor{}))
	default:
		return nil, common.ErrInvalidConfig.GenWithStack("unsupported compression type %s", f.compressionType)
	}

	failpoint.Inject("LoggingImportBytes", func() {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
			if err != nil {
				return nil, err
			}
			return &loggingConn{Conn: conn}, nil
		}))
	})

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (f *importClientFactoryImpl) getGrpcConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	return f.conns.GetGrpcConn(ctx, storeID, f.tcpConcurrency,
		func(ctx context.Context) (*grpc.ClientConn, error) {
			return f.makeConn(ctx, storeID)
		})
}

// create creates a new import client for specific store.
func (f *importClientFactoryImpl) create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

// close closes the factory.
func (f *importClientFactoryImpl) close() {
	f.conns.Close()
}

type loggingConn struct {
	net.Conn
}

// Write implements net.Conn.Write
func (c loggingConn) Write(b []byte) (int, error) {
	log.L().Debug("import write", zap.Int("bytes", len(b)))
	return c.Conn.Write(b)
}

type encodingBuilder struct {
	metrics *metric.Metrics
}

// NewEncodingBuilder creates an KVEncodingBuilder with local backend implementation.
func NewEncodingBuilder(ctx context.Context) encode.EncodingBuilder {
	result := new(encodingBuilder)
	if m, ok := metric.FromContext(ctx); ok {
		result.metrics = m
	}
	return result
}

// NewEncoder creates a KV encoder.
// It implements the `backend.EncodingBuilder` interface.
func (b *encodingBuilder) NewEncoder(_ context.Context, config *encode.EncodingConfig) (encode.Encoder, error) {
	return kv.NewTableKVEncoder(config, b.metrics)
}

// MakeEmptyRows creates an empty KV rows.
// It implements the `backend.EncodingBuilder` interface.
func (*encodingBuilder) MakeEmptyRows() encode.Rows {
	return kv.MakeRowsFromKvPairs(nil)
}

type targetInfoGetter struct {
	tls       *common.TLS
	targetDB  *sql.DB
	pdHTTPCli pdhttp.Client
}

// NewTargetInfoGetter creates an TargetInfoGetter with local backend
// implementation. `pdHTTPCli` should not be nil when need to check component
// versions in CheckRequirements.
func NewTargetInfoGetter(
	tls *common.TLS,
	db *sql.DB,
	pdHTTPCli pdhttp.Client,
) backend.TargetInfoGetter {
	return &targetInfoGetter{
		tls:       tls,
		targetDB:  db,
		pdHTTPCli: pdHTTPCli,
	}
}

// FetchRemoteDBModels implements the `backend.TargetInfoGetter` interface.
func (g *targetInfoGetter) FetchRemoteDBModels(ctx context.Context) ([]*model.DBInfo, error) {
	return tikv.FetchRemoteDBModelsFromTLS(ctx, g.tls)
}

// FetchRemoteTableModels obtains the models of all tables given the schema name.
// It implements the `TargetInfoGetter` interface.
func (g *targetInfoGetter) FetchRemoteTableModels(
	ctx context.Context,
	schemaName string,
	tableNames []string,
) (map[string]*model.TableInfo, error) {
	allTablesInDB, err := tikv.FetchRemoteTableModelsFromTLS(ctx, g.tls, schemaName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableNamesSet := make(map[string]struct{}, len(tableNames))
	for _, name := range tableNames {
		tableNamesSet[strings.ToLower(name)] = struct{}{}
	}
	ret := make(map[string]*model.TableInfo, len(tableNames))
	for _, tbl := range allTablesInDB {
		if _, ok := tableNamesSet[tbl.Name.L]; ok {
			ret[tbl.Name.L] = tbl
		}
	}
	return ret, nil
}

// CheckRequirements performs the check whether the backend satisfies the version requirements.
// It implements the `TargetInfoGetter` interface.
func (g *targetInfoGetter) CheckRequirements(ctx context.Context, checkCtx *backend.CheckCtx) error {
	// TODO: support lightning via SQL
	versionStr, err := version.FetchVersion(ctx, g.targetDB)
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkTiDBVersion(ctx, versionStr, localMinTiDBVersion, localMaxTiDBVersion); err != nil {
		return err
	}
	if g.pdHTTPCli == nil {
		return common.ErrUnknown.GenWithStack("pd HTTP client is required for component version check in local backend")
	}
	if err := tikv.CheckPDVersion(ctx, g.pdHTTPCli, localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, g.pdHTTPCli, localMinTiKVVersion, localMaxTiKVVersion); err != nil {
		return err
	}

	serverInfo := version.ParseServerInfo(versionStr)
	return checkTiFlashVersion(ctx, g.targetDB, checkCtx, *serverInfo.ServerVersion)
}

func checkTiDBVersion(_ context.Context, versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	return version.CheckTiDBVersion(versionStr, requiredMinVersion, requiredMaxVersion)
}

var tiFlashReplicaQuery = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TIFLASH_REPLICA WHERE REPLICA_COUNT > 0;"

// TiFlashReplicaQueryForTest is only used for tests.
var TiFlashReplicaQueryForTest = tiFlashReplicaQuery

type tblName struct {
	schema string
	name   string
}

type tblNames []tblName

// String implements fmt.Stringer
func (t tblNames) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, n := range t {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(common.UniqueTable(n.schema, n.name))
	}
	b.WriteByte(']')
	return b.String()
}

// CheckTiFlashVersionForTest is only used for tests.
var CheckTiFlashVersionForTest = checkTiFlashVersion

// check TiFlash replicas.
// local backend doesn't support TiFlash before tidb v4.0.5
func checkTiFlashVersion(ctx context.Context, db *sql.DB, checkCtx *backend.CheckCtx, tidbVersion semver.Version) error {
	if tidbVersion.Compare(tiFlashMinVersion) >= 0 {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:     db,
		Logger: log.FromContext(ctx),
	}

	res, err := exec.QueryStringRows(ctx, "fetch tiflash replica info", tiFlashReplicaQuery)
	if err != nil {
		return errors.Annotate(err, "fetch tiflash replica info failed")
	}

	tiFlashTablesMap := make(map[tblName]struct{}, len(res))
	for _, tblInfo := range res {
		name := tblName{schema: tblInfo[0], name: tblInfo[1]}
		tiFlashTablesMap[name] = struct{}{}
	}

	tiFlashTables := make(tblNames, 0)
	for _, dbMeta := range checkCtx.DBMetas {
		for _, tblMeta := range dbMeta.Tables {
			if len(tblMeta.DataFiles) == 0 {
				continue
			}
			name := tblName{schema: tblMeta.DB, name: tblMeta.Name}
			if _, ok := tiFlashTablesMap[name]; ok {
				tiFlashTables = append(tiFlashTables, name)
			}
		}
	}

	if len(tiFlashTables) > 0 {
		helpInfo := "Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data."
		return errors.Errorf("lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: %s. "+helpInfo, tiFlashTables)
	}
	return nil
}

// BackendConfig is the config for local backend.
type BackendConfig struct {
	// comma separated list of PD endpoints.
	PDAddr        string
	LocalStoreDir string
	// max number of cached grpc.ClientConn to a store.
	// note: this is not the limit of actual connections, each grpc.ClientConn can have one or more of it.
	MaxConnPerStore int
	// compress type when write or ingest into tikv
	ConnCompressType config.CompressionType
	// concurrency of generateJobForRange and import(write & ingest) workers
	WorkerConcurrency int
	// batch kv size when writing to TiKV
	KVWriteBatchSize       int64
	RegionSplitBatchSize   int
	RegionSplitConcurrency int
	CheckpointEnabled      bool
	// memory table size of pebble. since pebble can have multiple mem tables, the max memory used is
	// MemTableSize * MemTableStopWritesThreshold, see pebble.Options for more details.
	MemTableSize int
	// LocalWriterMemCacheSize is the memory threshold for one local writer of
	// engines. If the KV payload size exceeds LocalWriterMemCacheSize, local writer
	// will flush them into the engine.
	//
	// It has lower priority than LocalWriterConfig.Local.MemCacheSize.
	LocalWriterMemCacheSize int64
	// whether check TiKV capacity before write & ingest.
	ShouldCheckTiKV    bool
	DupeDetectEnabled  bool
	DuplicateDetectOpt common.DupDetectOpt
	// max write speed in bytes per second to each store(burst is allowed), 0 means no limit
	StoreWriteBWLimit int
	// When TiKV is in normal mode, ingesting too many SSTs will cause TiKV write stall.
	// To avoid this, we should check write stall before ingesting SSTs. Note that, we
	// must check both leader node and followers in client side, because followers will
	// not check write stall as long as ingest command is accepted by leader.
	ShouldCheckWriteStall bool
	// soft limit on the number of open files that can be used by pebble DB.
	// the minimum value is 128.
	MaxOpenFiles int
	KeyspaceName string
	// the scope when pause PD schedulers.
	PausePDSchedulerScope     config.PausePDSchedulerScope
	ResourceGroupName         string
	TaskType                  string
	RaftKV2SwitchModeDuration time.Duration
	// whether disable automatic compactions of pebble db of engine.
	// deduplicate pebble db is not affected by this option.
	// see DisableAutomaticCompactions of pebble.Options for more details.
	// default true.
	DisableAutomaticCompactions bool
	BlockSize                   int
}

// NewBackendConfig creates a new BackendConfig.
func NewBackendConfig(cfg *config.Config, maxOpenFiles int, keyspaceName, resourceGroupName, taskType string, raftKV2SwitchModeDuration time.Duration) BackendConfig {
	return BackendConfig{
		PDAddr:                      cfg.TiDB.PdAddr,
		LocalStoreDir:               cfg.TikvImporter.SortedKVDir,
		MaxConnPerStore:             cfg.TikvImporter.RangeConcurrency,
		ConnCompressType:            cfg.TikvImporter.CompressKVPairs,
		WorkerConcurrency:           cfg.TikvImporter.RangeConcurrency * 2,
		BlockSize:                   int(cfg.TikvImporter.BlockSize),
		KVWriteBatchSize:            int64(cfg.TikvImporter.SendKVSize),
		RegionSplitBatchSize:        cfg.TikvImporter.RegionSplitBatchSize,
		RegionSplitConcurrency:      cfg.TikvImporter.RegionSplitConcurrency,
		CheckpointEnabled:           cfg.Checkpoint.Enable,
		MemTableSize:                int(cfg.TikvImporter.EngineMemCacheSize),
		LocalWriterMemCacheSize:     int64(cfg.TikvImporter.LocalWriterMemCacheSize),
		ShouldCheckTiKV:             cfg.App.CheckRequirements,
		DupeDetectEnabled:           cfg.Conflict.Strategy != config.NoneOnDup,
		DuplicateDetectOpt:          common.DupDetectOpt{ReportErrOnDup: cfg.Conflict.Strategy == config.ErrorOnDup},
		StoreWriteBWLimit:           int(cfg.TikvImporter.StoreWriteBWLimit),
		ShouldCheckWriteStall:       cfg.Cron.SwitchMode.Duration == 0,
		MaxOpenFiles:                maxOpenFiles,
		KeyspaceName:                keyspaceName,
		PausePDSchedulerScope:       cfg.TikvImporter.PausePDSchedulerScope,
		ResourceGroupName:           resourceGroupName,
		TaskType:                    taskType,
		RaftKV2SwitchModeDuration:   raftKV2SwitchModeDuration,
		DisableAutomaticCompactions: true,
	}
}

func (c *BackendConfig) adjust() {
	c.MaxOpenFiles = max(c.MaxOpenFiles, openFilesLowerThreshold)
}

// Backend is a local backend.
type Backend struct {
	pdCli     pd.Client
	pdHTTPCli pdhttp.Client
	splitCli  split.SplitClient
	tikvCli   *tikvclient.KVStore
	tls       *common.TLS
	tikvCodec tikvclient.Codec

	BackendConfig
	engineMgr *engineManager

	supportMultiIngest  bool
	importClientFactory importClientFactory

	metrics      *metric.Common
	writeLimiter StoreWriteLimiter
	logger       log.Logger
	// This mutex is used to do some mutual exclusion work in the backend, flushKVs() in writer for now.
	mu sync.Mutex
}

var _ DiskUsage = (*Backend)(nil)
var _ StoreHelper = (*Backend)(nil)
var _ backend.Backend = (*Backend)(nil)

const (
	pdCliMaxMsgSize = int(128 * units.MiB) // pd.ScanRegion may return a large response
)

var (
	maxCallMsgSize = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pdCliMaxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(pdCliMaxMsgSize)),
	}
)

// NewBackend creates new connections to tikv.
func NewBackend(
	ctx context.Context,
	tls *common.TLS,
	config BackendConfig,
	pdSvcDiscovery pd.ServiceDiscovery,
) (b *Backend, err error) {
	var (
		pdCli                pd.Client
		spkv                 *tikvclient.EtcdSafePointKV
		pdCliForTiKV         *tikvclient.CodecPDClient
		rpcCli               tikvclient.Client
		tikvCli              *tikvclient.KVStore
		pdHTTPCli            pdhttp.Client
		importClientFactory  *importClientFactoryImpl
		multiIngestSupported bool
	)
	defer func() {
		if err == nil {
			return
		}
		if importClientFactory != nil {
			importClientFactory.close()
		}
		if pdHTTPCli != nil {
			pdHTTPCli.Close()
		}
		if tikvCli != nil {
			// tikvCli uses pdCliForTiKV(which wraps pdCli) , spkv and rpcCli, so
			// close tikvCli will close all of them.
			_ = tikvCli.Close()
		} else {
			if rpcCli != nil {
				_ = rpcCli.Close()
			}
			if spkv != nil {
				_ = spkv.Close()
			}
			// pdCliForTiKV wraps pdCli, so we only need close pdCli
			if pdCli != nil {
				pdCli.Close()
			}
		}
	}()
	config.adjust()
	var pdAddrs []string
	if pdSvcDiscovery != nil {
		pdAddrs = pdSvcDiscovery.GetServiceURLs()
		// TODO(lance6716): if PD client can support creating a client with external
		// service discovery, we can directly pass pdSvcDiscovery.
	} else {
		pdAddrs = strings.Split(config.PDAddr, ",")
	}
	pdCli, err = pd.NewClientWithContext(
		ctx, pdAddrs, tls.ToPDSecurityOption(),
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		pd.WithCustomTimeoutOption(60*time.Second),
	)
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}

	// The following copies tikv.NewTxnClient without creating yet another pdClient.
	spkv, err = tikvclient.NewEtcdSafePointKV(strings.Split(config.PDAddr, ","), tls.TLSConfig())
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	if config.KeyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCli)
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCli, config.KeyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	rpcCli = tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err = tikvclient.NewKVStore("lightning-local-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	pdHTTPCli = pdhttp.NewClientWithServiceDiscovery(
		"lightning",
		pdCli.GetServiceDiscovery(),
		pdhttp.WithTLSConfig(tls.TLSConfig()),
	).WithBackoffer(retry.InitialBackoffer(time.Second, time.Second, pdutil.PDRequestRetryTime*time.Second))
	splitCli := split.NewClient(pdCli, pdHTTPCli, tls.TLSConfig(), config.RegionSplitBatchSize, config.RegionSplitConcurrency)
	importClientFactory = newImportClientFactoryImpl(splitCli, tls, config.MaxConnPerStore, config.ConnCompressType)

	multiIngestSupported, err = checkMultiIngestSupport(ctx, pdCli, importClientFactory)
	if err != nil {
		return nil, common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}

	writeLimiter := newStoreWriteLimiter(config.StoreWriteBWLimit)
	local := &Backend{
		pdCli:     pdCli,
		pdHTTPCli: pdHTTPCli,
		splitCli:  splitCli,
		tikvCli:   tikvCli,
		tls:       tls,
		tikvCodec: tikvCodec,

		BackendConfig: config,

		supportMultiIngest:  multiIngestSupported,
		importClientFactory: importClientFactory,
		writeLimiter:        writeLimiter,
		logger:              log.FromContext(ctx),
	}
	local.engineMgr, err = newEngineManager(config, local, local.logger)
	if err != nil {
		return nil, err
	}
	if m, ok := metric.GetCommonMetric(ctx); ok {
		local.metrics = m
	}
	local.tikvSideCheckFreeSpace(ctx)

	return local, nil
}

// NewBackendForTest creates a new Backend for test.
func NewBackendForTest(ctx context.Context, config BackendConfig, storeHelper StoreHelper) (*Backend, error) {
	config.adjust()

	logger := log.FromContext(ctx)
	engineMgr, err := newEngineManager(config, storeHelper, logger)
	if err != nil {
		return nil, err
	}
	local := &Backend{
		BackendConfig: config,
		logger:        logger,
		engineMgr:     engineMgr,
	}
	if m, ok := metric.GetCommonMetric(ctx); ok {
		local.metrics = m
	}

	return local, nil
}

// TotalMemoryConsume returns the total memory usage of the local backend.
func (local *Backend) TotalMemoryConsume() int64 {
	return local.engineMgr.totalMemoryConsume()
}

func checkMultiIngestSupport(ctx context.Context, pdCli pd.Client, factory importClientFactory) (bool, error) {
	stores, err := pdCli.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return false, errors.Trace(err)
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
					return false, ctx.Err()
				}
			}
			client, err1 := factory.create(ctx, s.Id)
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
					return false, nil
				}
			}
			log.FromContext(ctx).Warn("check multi ingest support failed", zap.Error(err), zap.String("store", s.Address),
				zap.Int("retry", i))
		}
		if err != nil {
			// if the cluster contains no TiFlash store, we don't need the multi-ingest feature,
			// so in this condition, downgrade the logic instead of return an error.
			if hasTiFlash {
				return false, errors.Trace(err)
			}
			log.FromContext(ctx).Warn("check multi failed all retry, fallback to false", log.ShortError(err))
			return false, nil
		}
	}

	log.FromContext(ctx).Info("multi ingest support")
	return true, nil
}

func (local *Backend) tikvSideCheckFreeSpace(ctx context.Context) {
	if !local.ShouldCheckTiKV {
		return
	}
	err := tikv.ForTiKVVersions(
		ctx,
		local.pdHTTPCli,
		func(version *semver.Version, addrMsg string) error {
			if version.Compare(tikvSideFreeSpaceCheck) < 0 {
				return errors.Errorf(
					"%s has version %s, it does not support server side free space check",
					addrMsg, version,
				)
			}
			return nil
		},
	)
	if err == nil {
		local.logger.Info("TiKV server side free space check is enabled, so lightning will turn it off")
		local.ShouldCheckTiKV = false
	} else {
		local.logger.Info("", zap.Error(err))
	}
}

// Close the local backend.
func (local *Backend) Close() {
	local.engineMgr.close()
	local.importClientFactory.close()

	_ = local.tikvCli.Close()
	local.pdHTTPCli.Close()
	local.pdCli.Close()
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *Backend) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
	return local.engineMgr.flushEngine(ctx, engineID)
}

// FlushAllEngines flush all engines.
func (local *Backend) FlushAllEngines(parentCtx context.Context) (err error) {
	return local.engineMgr.flushAllEngines(parentCtx)
}

// RetryImportDelay returns the delay time before retrying to import a file.
func (*Backend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

// ShouldPostProcess returns true if the backend should post process the data.
func (*Backend) ShouldPostProcess() bool {
	return true
}

// OpenEngine must be called with holding mutex of Engine.
func (local *Backend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	return local.engineMgr.openEngine(ctx, cfg, engineUUID)
}

// CloseEngine closes backend engine by uuid.
func (local *Backend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	return local.engineMgr.closeEngine(ctx, cfg, engineUUID)
}

func (local *Backend) getImportClient(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	return local.importClientFactory.create(ctx, storeID)
}

func splitRangeBySizeProps(fullRange common.Range, sizeProps *sizeProperties, sizeLimit int64, keysLimit int64) []common.Range {
	ranges := make([]common.Range, 0, sizeProps.totalSize/uint64(sizeLimit))
	curSize := uint64(0)
	curKeys := uint64(0)
	curKey := fullRange.Start

	sizeProps.iter(func(p *rangeProperty) bool {
		if bytes.Compare(p.Key, curKey) <= 0 {
			return true
		}
		if bytes.Compare(p.Key, fullRange.End) > 0 {
			return false
		}
		curSize += p.Size
		curKeys += p.Keys
		if int64(curSize) >= sizeLimit || int64(curKeys) >= keysLimit {
			ranges = append(ranges, common.Range{Start: curKey, End: p.Key})
			curKey = p.Key
			curSize = 0
			curKeys = 0
		}
		return true
	})

	if bytes.Compare(curKey, fullRange.End) < 0 {
		// If the remaining range is too small, append it to last range.
		if len(ranges) > 0 && curKeys == 0 {
			ranges[len(ranges)-1].End = fullRange.End
		} else {
			ranges = append(ranges, common.Range{Start: curKey, End: fullRange.End})
		}
	}
	return ranges
}

func getRegionSplitKeys(
	ctx context.Context,
	engine common.Engine,
	sizeLimit int64,
	keysLimit int64,
) ([][]byte, error) {
	startKey, endKey, err := engine.GetKeyRange()
	if err != nil {
		return nil, err
	}
	if startKey == nil {
		return nil, errors.New("could not find first pair")
	}

	engineFileTotalSize, engineFileLength := engine.KVStatistics()

	if engineFileTotalSize <= sizeLimit && engineFileLength <= keysLimit {
		return [][]byte{startKey, endKey}, nil
	}

	logger := log.FromContext(ctx).With(zap.String("engine", engine.ID()))
	keys, err := engine.GetRegionSplitKeys()
	logger.Info("split engine key ranges",
		zap.Int64("totalSize", engineFileTotalSize),
		zap.Int64("totalCount", engineFileLength),
		logutil.Key("startKey", startKey), logutil.Key("endKey", endKey),
		zap.Int("len(keys)", len(keys)), zap.Error(err))
	return keys, err
}

// prepareAndSendJob will read the engine to get estimated key range, then split
// and scatter regions for these range and send region jobs to jobToWorkerCh.
func (local *Backend) prepareAndSendJob(
	ctx context.Context,
	engine common.Engine,
	regionSplitKeys [][]byte,
	regionSplitSize, regionSplitKeyCnt int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	lfTotalSize, lfLength := engine.KVStatistics()
	log.FromContext(ctx).Info("import engine ranges", zap.Int("len(regionSplitKeyCnt)", len(regionSplitKeys)))

	// if all the kv can fit in one region, skip split regions. TiDB will split one region for
	// the table when table is created.
	needSplit := len(regionSplitKeys) > 2 || lfTotalSize > regionSplitSize || lfLength > regionSplitKeyCnt
	// split region by given ranges
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		needSplit = true
	})
	if needSplit {
		var err error
		logger := log.FromContext(ctx).With(zap.String("uuid", engine.ID())).Begin(zap.InfoLevel, "split and scatter ranges")
		backOffTime := 10 * time.Second
		maxbackoffTime := 120 * time.Second
		for i := 0; i < maxRetryTimes; i++ {
			failpoint.Inject("skipSplitAndScatter", func() {
				failpoint.Break()
			})

			err = local.splitAndScatterRegionInBatches(ctx, regionSplitKeys, maxBatchSplitRanges)
			if err == nil || common.IsContextCanceledError(err) {
				break
			}

			log.FromContext(ctx).Warn("split and scatter failed in retry", zap.String("engine ID", engine.ID()),
				log.ShortError(err), zap.Int("retry", i))
			select {
			case <-time.After(backOffTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			backOffTime *= 2
			if backOffTime > maxbackoffTime {
				backOffTime = maxbackoffTime
			}
		}
		logger.End(zap.ErrorLevel, err)
		if err != nil {
			return err
		}
	}

	return local.generateAndSendJob(
		ctx,
		engine,
		regionSplitSize,
		regionSplitKeyCnt,
		jobToWorkerCh,
		jobWg,
	)
}

// generateAndSendJob scans the region in ranges and send region jobs to jobToWorkerCh.
func (local *Backend) generateAndSendJob(
	ctx context.Context,
	engine common.Engine,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)

	dataAndRangeCh := make(chan common.DataAndRanges)
	conn := local.WorkerConcurrency
	if _, ok := engine.(*external.Engine); ok {
		// currently external engine will generate a large IngestData, se we lower the
		// concurrency to pass backpressure to the LoadIngestData goroutine to avoid OOM
		conn = 1
	}
	for i := 0; i < conn; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return nil
				case p, ok := <-dataAndRangeCh:
					if !ok {
						return nil
					}

					failpoint.Inject("beforeGenerateJob", nil)
					failpoint.Inject("sendDummyJob", func(_ failpoint.Value) {
						// this is used to trigger worker failure, used together
						// with WriteToTiKVNotEnoughDiskSpace
						jobToWorkerCh <- &regionJob{}
						time.Sleep(5 * time.Second)
					})
					jobs, err := local.generateJobForRange(egCtx, p.Data, p.SortedRanges, regionSplitSize, regionSplitKeys)
					if err != nil {
						if common.IsContextCanceledError(err) {
							return nil
						}
						return err
					}
					for _, job := range jobs {
						job.ref(jobWg)
						select {
						case <-egCtx.Done():
							// this job is not put into jobToWorkerCh
							job.done(jobWg)
							// if the context is canceled, it means worker has error.
							return nil
						case jobToWorkerCh <- job:
						}
					}
				}
			}
		})
	}

	eg.Go(func() error {
		err := engine.LoadIngestData(egCtx, dataAndRangeCh)
		if err != nil {
			return errors.Trace(err)
		}
		close(dataAndRangeCh)
		return nil
	})

	return eg.Wait()
}

// fakeRegionJobs is used in test, the injected job can be found by (startKey, endKey).
var fakeRegionJobs map[[2]string]struct {
	jobs []*regionJob
	err  error
}

// generateJobForRange will scan the region in `keyRange` and generate region jobs.
// It will retry internally when scan region meet error.
func (local *Backend) generateJobForRange(
	ctx context.Context,
	data common.IngestData,
	sortedJobRanges []common.Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	startOfAllRanges, endOfAllRanges := sortedJobRanges[0].Start, sortedJobRanges[len(sortedJobRanges)-1].End

	failpoint.Inject("fakeRegionJobs", func() {
		if ctx.Err() != nil {
			failpoint.Return(nil, ctx.Err())
		}
		key := [2]string{string(startOfAllRanges), string(endOfAllRanges)}
		injected := fakeRegionJobs[key]
		// overwrite the stage to regionScanned, because some time same sortedJobRanges
		// will be generated more than once.
		for _, job := range injected.jobs {
			job.stage = regionScanned
		}
		failpoint.Return(injected.jobs, injected.err)
	})

	pairStart, pairEnd, err := data.GetFirstAndLastKey(startOfAllRanges, endOfAllRanges)
	if err != nil {
		return nil, err
	}
	if pairStart == nil {
		logFn := log.FromContext(ctx).Info
		if _, ok := data.(*external.MemoryIngestData); ok {
			logFn = log.FromContext(ctx).Warn
		}
		logFn("There is no pairs in range",
			logutil.Key("startOfAllRanges", startOfAllRanges),
			logutil.Key("endOfAllRanges", endOfAllRanges))
		// trigger cleanup
		data.IncRef()
		data.DecRef()
		return nil, nil
	}

	startKey := codec.EncodeBytes([]byte{}, pairStart)
	endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
	regions, err := split.PaginateScanRegion(ctx, local.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		log.FromContext(ctx).Error("scan region failed",
			log.ShortError(err), zap.Int("region_len", len(regions)),
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey))
		return nil, err
	}

	jobs := newRegionJobs(regions, data, sortedJobRanges, regionSplitSize, regionSplitKeys, local.metrics)
	log.FromContext(ctx).Info("generate region jobs",
		zap.Int("len(jobs)", len(jobs)),
		zap.String("startOfAllRanges", hex.EncodeToString(startOfAllRanges)),
		zap.String("endOfAllRanges", hex.EncodeToString(endOfAllRanges)),
		zap.String("startKeyOfFirstRegion", hex.EncodeToString(regions[0].Region.GetStartKey())),
		zap.String("endKeyOfLastRegion", hex.EncodeToString(regions[len(regions)-1].Region.GetEndKey())),
	)
	return jobs, nil
}

// startWorker creates a worker that reads from the job channel and processes.
// startWorker will return nil if it's expected to stop, where the cases are all
// jobs are finished or the context canceled because other components report
// error. It will return not nil error when it actively stops. startWorker must
// call job.done() if it does not put the job into jobOutCh.
func (local *Backend) startWorker(
	ctx context.Context,
	jobInCh, jobOutCh chan *regionJob,
	afterExecuteJob func([]*metapb.Peer),
	jobWg *sync.WaitGroup,
) error {
	metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Set(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case job, ok := <-jobInCh:
			if !ok {
				return nil
			}

			var peers []*metapb.Peer
			// in unit test, we may not have the real peers
			if job.region != nil && job.region.Region != nil {
				peers = job.region.Region.GetPeers()
			}
			failpoint.InjectCall("beforeExecuteRegionJob", ctx)
			metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Inc()
			err := local.executeJob(ctx, job)
			metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Dec()

			if afterExecuteJob != nil {
				afterExecuteJob(peers)
			}
			switch job.stage {
			case regionScanned, wrote, ingested:
				select {
				case <-ctx.Done():
					job.done(jobWg)
					return nil
				case jobOutCh <- job:
				}
			case needRescan:
				jobs, err2 := local.generateJobForRange(
					ctx,
					job.ingestData,
					[]common.Range{job.keyRange},
					job.regionSplitSize,
					job.regionSplitKeys,
				)
				if err2 != nil {
					// Don't need to put the job back to retry, because generateJobForRange
					// has done the retry internally. Here just done for the "needRescan"
					// job and exit directly.
					job.done(jobWg)
					return err2
				}
				// 1 "needRescan" job becomes len(jobs) "regionScanned" jobs.
				newJobCnt := len(jobs) - 1
				for newJobCnt > 0 {
					job.ref(jobWg)
					newJobCnt--
				}
				for _, j := range jobs {
					j.lastRetryableErr = job.lastRetryableErr
					select {
					case <-ctx.Done():
						j.done(jobWg)
						// don't exit here, we mark done for each job and exit in the outer loop
					case jobOutCh <- j:
					}
				}
			}

			if err != nil {
				return err
			}
		}
	}
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

func checkDiskAvail(ctx context.Context, store *pdhttp.StoreInfo) error {
	logger := log.FromContext(ctx)
	capacity, err := units.RAMInBytes(store.Status.Capacity)
	if err != nil {
		logger.Warn("failed to parse capacity",
			zap.String("capacity", store.Status.Capacity), zap.Error(err))
		return nil
	}
	if capacity <= 0 {
		// PD will return a zero value StoreInfo if heartbeat is not received after
		// startup, skip temporarily.
		return nil
	}
	available, err := units.RAMInBytes(store.Status.Available)
	if err != nil {
		logger.Warn("failed to parse available",
			zap.String("available", store.Status.Available), zap.Error(err))
		return nil
	}
	ratio := available * 100 / capacity
	if ratio < 10 {
		storeType := "TiKV"
		if engine.IsTiFlashHTTPResp(&store.Store) {
			storeType = "TiFlash"
		}
		return errors.Errorf("the remaining storage capacity of %s(%s) is less than 10%%; please increase the storage capacity of %s and try again",
			storeType, store.Store.Address, storeType)
	}
	return nil
}

// executeJob handles a regionJob and tries to convert it to ingested stage.
// If non-retryable error occurs, it will return the error.
// If retryable error occurs, it will return nil and caller should check the stage
// of the regionJob to determine what to do with it.
func (local *Backend) executeJob(
	ctx context.Context,
	job *regionJob,
) error {
	failpoint.Inject("WriteToTiKVNotEnoughDiskSpace", func(_ failpoint.Value) {
		failpoint.Return(
			errors.New("the remaining storage capacity of TiKV is less than 10%%; please increase the storage capacity of TiKV and try again"))
	})
	if local.ShouldCheckTiKV {
		for _, peer := range job.region.Region.GetPeers() {
			store, err := local.pdHTTPCli.GetStore(ctx, peer.StoreId)
			if err != nil {
				log.FromContext(ctx).Warn("failed to get StoreInfo from pd http api", zap.Error(err))
				continue
			}
			err = checkDiskAvail(ctx, store)
			if err != nil {
				return err
			}
		}
	}

	for {
		err := local.writeToTiKV(ctx, job)
		if err != nil {
			if !local.isRetryableImportTiKVError(err) {
				return err
			}
			// if it's retryable error, we retry from scanning region
			log.FromContext(ctx).Warn("meet retryable error when writing to TiKV",
				log.ShortError(err), zap.Stringer("job stage", job.stage))
			job.lastRetryableErr = err
			return nil
		}

		err = local.ingest(ctx, job)
		if err != nil {
			if !local.isRetryableImportTiKVError(err) {
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
		job.keyRange.Start = job.writeResult.remainingStartKey
		job.convertStageTo(regionScanned)
	}
}

// ImportEngine imports an engine to TiKV.
func (local *Backend) ImportEngine(
	ctx context.Context,
	engineUUID uuid.UUID,
	regionSplitSize, regionSplitKeys int64,
) error {
	kvRegionSplitSize, kvRegionSplitKeys, err := GetRegionSplitSizeKeys(ctx, local.pdCli, local.tls)
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

	var e common.Engine
	if externalEngine, ok := local.engineMgr.getExternalEngine(engineUUID); ok {
		e = externalEngine
	} else {
		localEngine := local.engineMgr.lockEngine(engineUUID, importMutexStateImport)
		if localEngine == nil {
			// skip if engine not exist. See the comment of `CloseEngine` for more detail.
			return nil
		}
		defer localEngine.unlock()
		localEngine.regionSplitSize = regionSplitSize
		localEngine.regionSplitKeyCnt = regionSplitKeys
		e = localEngine
	}
	lfTotalSize, lfLength := e.KVStatistics()
	if lfTotalSize == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.FromContext(ctx).Info("engine contains no kv, skip import", zap.Stringer("engine", engineUUID))
		return nil
	}

	// split sorted file into range about regionSplitSize per file
	splitKeys, err := getRegionSplitKeys(ctx, e, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}

	if len(splitKeys) > 0 && local.PausePDSchedulerScope == config.PausePDSchedulerScopeTable {
		log.FromContext(ctx).Info("pause pd scheduler of table scope")
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var startKey, endKey []byte
		if len(splitKeys[0]) > 0 {
			startKey = codec.EncodeBytes(nil, splitKeys[0])
		}
		if len(splitKeys[len(splitKeys)-1]) > 0 {
			endKey = codec.EncodeBytes(nil, splitKeys[len(splitKeys)-1])
		}
		done, err := pdutil.PauseSchedulersByKeyRange(subCtx, local.pdHTTPCli, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	if len(splitKeys) > 0 && local.BackendConfig.RaftKV2SwitchModeDuration > 0 {
		log.FromContext(ctx).Info("switch import mode of ranges",
			zap.String("startKey", hex.EncodeToString(splitKeys[0])),
			zap.String("endKey", hex.EncodeToString(splitKeys[len(splitKeys)-1])))
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done, err := local.switchModeBySplitKeys(subCtx, splitKeys)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	log.FromContext(ctx).Info("start import engine",
		zap.Stringer("uuid", engineUUID),
		zap.Int("region ranges", len(splitKeys)),
		zap.Int64("count", lfLength),
		zap.Int64("size", lfTotalSize))

	failpoint.Inject("ReadyForImportEngine", func() {})

	err = local.doImport(ctx, e, splitKeys, regionSplitSize, regionSplitKeys)
	if err == nil {
		importedSize, importedLength := e.ImportedStatistics()
		log.FromContext(ctx).Info("import engine success",
			zap.Stringer("uuid", engineUUID),
			zap.Int64("size", lfTotalSize),
			zap.Int64("kvs", lfLength),
			zap.Int64("importedSize", importedSize),
			zap.Int64("importedCount", importedLength))
	}
	return err
}

// expose these variables to unit test.
var (
	testJobToWorkerCh = make(chan *regionJob)
	testJobWg         *sync.WaitGroup
)

func (local *Backend) doImport(
	ctx context.Context,
	engine common.Engine,
	regionSplitKeys [][]byte,
	regionSplitSize, regionSplitKeyCnt int64,
) error {
	/*
	 [prepareAndSendJob]---jobToWorkerCh->[storeBalancer(optional)]->[workers]
	                     ^                                             |
	                     |                                     jobFromWorkerCh
	                     |                                             |
	                     |                                             v
	               [regionJobRetryer]<-------------[dispatchJobGoroutine]-->done
	*/

	// Above is the happy path workflow of region jobs. A job is generated by
	// prepareAndSendJob and terminated to "done" state by dispatchJobGoroutine. We
	// maintain an invariant that the number of generated jobs (after job.ref())
	// minus the number of "done" jobs (after job.done()) equals to jobWg. So we can
	// use jobWg to wait for all jobs to be finished.
	//
	// To handle the error case, we still maintain the invariant, but the workflow
	// becomes a bit more complex. When an error occurs, the owner components of a
	// job need to convert the job to "done" state, or send all its owned jobs to
	// next components. The exit order is important because if the next component is
	// exited before the owner component, deadlock will happen.
	//
	// All components are spawned by workGroup so the main goroutine can wait all
	// components to exit. Component exit order in happy path is:
	//
	// 1. prepareAndSendJob is finished, its goroutine will wait all jobs are
	// finished by jobWg.Wait(). Then it will exit and close the output channel of
	// workers.
	//
	// 2. one-by-one, when every component see its input channel is closed, it knows
	// the workflow is finished. It will exit and (except for workers) close the
	// output channel which is the input channel of the next component.
	//
	// 3. Now all components are exited, the main goroutine can exit after
	// workGroup.Wait().
	//
	// Component exit order in error case is:
	//
	// 1. The error component exits and causes workGroup's context to be canceled.
	//
	// 2. All other components will exit because of the canceled context. No need to
	// close channels.
	//
	// 3. the main goroutine can see the error and exit after workGroup.Wait().
	var (
		workGroup, workerCtx = util.NewErrorGroupWithRecoverWithCtx(ctx)
		// jobToWorkerCh and jobFromWorkerCh are unbuffered so jobs will not be
		// owned by them.
		jobToWorkerCh   = make(chan *regionJob)
		jobFromWorkerCh = make(chan *regionJob)
		jobWg           sync.WaitGroup
		balancer        *storeBalancer
	)

	// storeBalancer does not have backpressure, it should not be used with external
	// engine to avoid OOM.
	if _, ok := engine.(*Engine); ok {
		balancer = newStoreBalancer(jobToWorkerCh, &jobWg)
		workGroup.Go(func() error {
			return balancer.run(workerCtx)
		})
	}

	failpoint.Inject("injectVariables", func() {
		jobToWorkerCh = testJobToWorkerCh
		testJobWg = &jobWg
	})

	retryer := newRegionJobRetryer(workerCtx, jobToWorkerCh, &jobWg)
	workGroup.Go(func() error {
		retryer.run()
		return nil
	})

	// dispatchJobGoroutine
	workGroup.Go(func() error {
		var (
			job *regionJob
			ok  bool
		)
		for {
			select {
			case <-workerCtx.Done():
				return nil
			case job, ok = <-jobFromWorkerCh:
			}
			if !ok {
				retryer.close()
				return nil
			}
			switch job.stage {
			case regionScanned, wrote:
				job.retryCount++
				if job.retryCount > MaxWriteAndIngestRetryTimes {
					job.done(&jobWg)
					lastErr := job.lastRetryableErr
					intest.Assert(lastErr != nil, "lastRetryableErr should not be nil")
					if lastErr == nil {
						lastErr = errors.New("retry limit exceeded")
						log.FromContext(ctx).Error(
							"lastRetryableErr should not be nil",
							logutil.Key("startKey", job.keyRange.Start),
							logutil.Key("endKey", job.keyRange.End),
							zap.Stringer("stage", job.stage),
							zap.Error(lastErr))
					}
					return lastErr
				}
				// max retry backoff time: 2+4+8+16+30*26=810s
				sleepSecond := math.Pow(2, float64(job.retryCount))
				if sleepSecond > float64(maxRetryBackoffSecond) {
					sleepSecond = float64(maxRetryBackoffSecond)
				}
				job.waitUntil = time.Now().Add(time.Second * time.Duration(sleepSecond))
				log.FromContext(ctx).Info("put job back to jobCh to retry later",
					logutil.Key("startKey", job.keyRange.Start),
					logutil.Key("endKey", job.keyRange.End),
					zap.Stringer("stage", job.stage),
					zap.Int("retryCount", job.retryCount),
					zap.Time("waitUntil", job.waitUntil))
				if !retryer.push(job) {
					// retryer is closed by worker error
					job.done(&jobWg)
				}
			case ingested:
				job.done(&jobWg)
			case needRescan:
				panic("should not reach here")
			}
		}
	})

	failpoint.Inject("skipStartWorker", func() {
		failpoint.Goto("afterStartWorker")
	})

	for i := 0; i < local.WorkerConcurrency; i++ {
		workGroup.Go(func() error {
			toCh := jobToWorkerCh
			var afterExecuteJob func([]*metapb.Peer)
			if balancer != nil {
				toCh = balancer.innerJobToWorkerCh
				afterExecuteJob = balancer.releaseStoreLoad
			}
			return local.startWorker(workerCtx, toCh, jobFromWorkerCh, afterExecuteJob, &jobWg)
		})
	}

	failpoint.Label("afterStartWorker")

	workGroup.Go(func() error {
		err := local.prepareAndSendJob(
			workerCtx,
			engine,
			regionSplitKeys,
			regionSplitSize,
			regionSplitKeyCnt,
			jobToWorkerCh,
			&jobWg,
		)
		if err != nil {
			return err
		}

		jobWg.Wait()
		if balancer != nil {
			intest.AssertFunc(func() bool {
				allZero := true
				balancer.storeLoadMap.Range(func(_, value any) bool {
					if value.(int) != 0 {
						allZero = false
						return false
					}
					return true
				})
				return allZero
			})
		}
		close(jobFromWorkerCh)
		return nil
	})

	err := workGroup.Wait()
	if err != nil && !common.IsContextCanceledError(err) {
		log.FromContext(ctx).Error("do import meets error", zap.Error(err))
	}
	return err
}

// GetImportedKVCount returns the number of imported KV pairs of some engine.
func (local *Backend) GetImportedKVCount(engineUUID uuid.UUID) int64 {
	return local.engineMgr.getImportedKVCount(engineUUID)
}

// GetExternalEngineKVStatistics returns kv statistics of some engine.
func (local *Backend) GetExternalEngineKVStatistics(engineUUID uuid.UUID) (
	totalKVSize int64, totalKVCount int64) {
	return local.engineMgr.getExternalEngineKVStatistics(engineUUID)
}

// ResetEngine reset the engine and reclaim the space.
func (local *Backend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return local.engineMgr.resetEngine(ctx, engineUUID, false)
}

// ResetEngineSkipAllocTS is like ResetEngine but the inner TS of the engine is
// invalid. Caller must use SetTSAfterResetEngine to set a valid TS before import
// the engine.
func (local *Backend) ResetEngineSkipAllocTS(ctx context.Context, engineUUID uuid.UUID) error {
	return local.engineMgr.resetEngine(ctx, engineUUID, true)
}

// SetTSAfterResetEngine allocates a new TS for the engine after it's reset.
// This is typically called after persisting the chosen TS of the engine to make
// sure TS is not changed after task failover.
func (local *Backend) SetTSAfterResetEngine(engineUUID uuid.UUID, ts uint64) error {
	e := local.engineMgr.lockEngine(engineUUID, importMutexStateClose)
	if e == nil {
		return errors.Errorf("engine %s not found in SetTSAfterResetEngine", engineUUID.String())
	}
	defer e.unlock()
	e.engineMeta.TS = ts
	return e.saveEngineMeta()
}

// CleanupEngine cleanup the engine and reclaim the space.
func (local *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return local.engineMgr.cleanupEngine(ctx, engineUUID)
}

// GetDupeController returns a new dupe controller.
func (local *Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *DupeController {
	return &DupeController{
		splitCli:            local.splitCli,
		tikvCli:             local.tikvCli,
		tikvCodec:           local.tikvCodec,
		errorMgr:            errorMgr,
		dupeConcurrency:     dupeConcurrency,
		duplicateDB:         local.engineMgr.getDuplicateDB(),
		keyAdapter:          local.engineMgr.getKeyAdapter(),
		importClientFactory: local.importClientFactory,
		resourceGroupName:   local.ResourceGroupName,
		taskType:            local.TaskType,
	}
}

// UnsafeImportAndReset forces the backend to import the content of an engine
// into the target and then reset the engine to empty. This method will not
// close the engine. Make sure the engine is flushed manually before calling
// this method.
func (local *Backend) UnsafeImportAndReset(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	// DO NOT call be.abstract.CloseEngine()! The engine should still be writable after
	// calling UnsafeImportAndReset().
	logger := log.FromContext(ctx).With(
		zap.String("engineTag", "<import-and-reset>"),
		zap.Stringer("engineUUID", engineUUID),
	)
	closedEngine := backend.NewClosedEngine(local, logger, engineUUID, 0)
	if err := closedEngine.Import(ctx, regionSplitSize, regionSplitKeys); err != nil {
		return err
	}
	return local.engineMgr.resetEngine(ctx, engineUUID, false)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

// LocalWriter returns a new local writer.
func (local *Backend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	return local.engineMgr.localWriter(ctx, cfg, engineUUID)
}

// switchModeBySplitKeys will switch tikv mode for regions in the specific keys
// for multirocksdb. This function will spawn a goroutine to keep switch mode
// periodically until the context is done. The return done channel is used to
// notify the caller that the background goroutine is exited.
func (local *Backend) switchModeBySplitKeys(
	ctx context.Context,
	splitKeys [][]byte,
) (<-chan struct{}, error) {
	switcher := NewTiKVModeSwitcher(local.tls.TLSConfig(), local.pdHTTPCli, log.FromContext(ctx).Logger)
	done := make(chan struct{})

	keyRange := &sst.Range{}
	if len(splitKeys[0]) > 0 {
		keyRange.Start = codec.EncodeBytes(nil, splitKeys[0])
	}
	if len(splitKeys[len(splitKeys)-1]) > 0 {
		keyRange.End = codec.EncodeBytes(nil, splitKeys[len(splitKeys)-1])
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(local.BackendConfig.RaftKV2SwitchModeDuration)
		defer ticker.Stop()
		switcher.ToImportMode(ctx, keyRange)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
				switcher.ToImportMode(ctx, keyRange)
			}
		}
		// Use a new context to avoid the context is canceled by the caller.
		recoverCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		switcher.ToNormalMode(recoverCtx, keyRange)
	}()
	return done, nil
}

func openLocalWriter(cfg *backend.LocalWriterConfig, engine *Engine, tikvCodec tikvclient.Codec, cacheSize int64, kvBuffer *membuf.Buffer) (*Writer, error) {
	// pre-allocate a long enough buffer to avoid a lot of runtime.growslice
	// this can help save about 3% of CPU.
	var preAllocWriteBatch []common.KvPair
	if !cfg.Local.IsKVSorted {
		preAllocWriteBatch = make([]common.KvPair, units.MiB)
		// we want to keep the cacheSize as the whole limit of this local writer, but the
		// main memory usage comes from two member: kvBuffer and writeBatch, so we split
		// ~10% to writeBatch for !IsKVSorted, which means we estimate the average length
		// of KV pairs are 9 times than the size of common.KvPair (9*72B = 648B).
		cacheSize = cacheSize * 9 / 10
	}
	w := &Writer{
		engine:             engine,
		memtableSizeLimit:  cacheSize,
		kvBuffer:           kvBuffer,
		isKVSorted:         cfg.Local.IsKVSorted,
		isWriteBatchSorted: true,
		tikvCodec:          tikvCodec,
		writeBatch:         preAllocWriteBatch,
	}
	engine.localWriters.Store(w, nil)
	return w, nil
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

// EngineFileSizes implements DiskUsage interface.
func (local *Backend) EngineFileSizes() (res []backend.EngineFileSize) {
	return local.engineMgr.engineFileSizes()
}

// GetTS implements StoreHelper interface.
func (local *Backend) GetTS(ctx context.Context) (physical, logical int64, err error) {
	return local.pdCli.GetTS(ctx)
}

// GetTiKVCodec implements StoreHelper interface.
func (local *Backend) GetTiKVCodec() tikvclient.Codec {
	return local.tikvCodec
}

// CloseEngineMgr close the engine manager.
// This function is used for test.
func (local *Backend) CloseEngineMgr() {
	local.engineMgr.close()
}

var getSplitConfFromStoreFunc = getSplitConfFromStore

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

// GetRegionSplitSizeKeys return region split size, region split keys, error
func GetRegionSplitSizeKeys(ctx context.Context, cli pd.Client, tls *common.TLS) (
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
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStoreFunc(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		log.FromContext(ctx).Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}
