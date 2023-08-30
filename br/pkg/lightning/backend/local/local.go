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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/manual"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
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
	dialTimeout             = 5 * time.Minute
	maxRetryTimes           = 5
	defaultRetryBackoffTime = 3 * time.Second
	// maxWriteAndIngestRetryTimes is the max retry times for write and ingest.
	// A large retry times is for tolerating tikv cluster failures.
	maxWriteAndIngestRetryTimes = 30

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
	localMinTiDBVersion = *semver.New("4.0.0")
	localMinTiKVVersion = *semver.New("4.0.0")
	localMinPDVersion   = *semver.New("4.0.0")
	localMaxTiDBVersion = version.NextMajorVersion()
	localMaxTiKVVersion = version.NextMajorVersion()
	localMaxPDVersion   = version.NextMajorVersion()
	tiFlashMinVersion   = *semver.New("4.0.5")

	errorEngineClosed     = errors.New("engine is closed")
	maxRetryBackoffSecond = 30
)

// ImportClientFactory is factory to create new import client for specific store.
type ImportClientFactory interface {
	Create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error)
	Close()
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

// Create creates a new import client for specific store.
func (f *importClientFactoryImpl) Create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

// Close closes the factory.
func (f *importClientFactoryImpl) Close() {
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

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	start []byte
	end   []byte // end is always exclusive except import_sstpb.SSTMeta
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
	tls      *common.TLS
	targetDB *sql.DB
	pdCli    pd.Client
}

// NewTargetInfoGetter creates an TargetInfoGetter with local backend implementation.
func NewTargetInfoGetter(tls *common.TLS, db *sql.DB, pdCli pd.Client) backend.TargetInfoGetter {
	return &targetInfoGetter{
		tls:      tls,
		targetDB: db,
		pdCli:    pdCli,
	}
}

// FetchRemoteTableModels obtains the models of all tables given the schema name.
// It implements the `TargetInfoGetter` interface.
func (g *targetInfoGetter) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return tikv.FetchRemoteTableModelsFromTLS(ctx, g.tls, schemaName)
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
	if err := tikv.CheckPDVersion(ctx, g.tls, g.pdCli.GetLeaderAddr(), localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, g.tls, g.pdCli.GetLeaderAddr(), localMinTiKVVersion, localMaxTiKVVersion); err != nil {
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
	MemTableSize            int
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
}

// NewBackendConfig creates a new BackendConfig.
func NewBackendConfig(cfg *config.Config, maxOpenFiles int, keyspaceName, resourceGroupName, taskType string, raftKV2SwitchModeDuration time.Duration) BackendConfig {
	return BackendConfig{
		PDAddr:                    cfg.TiDB.PdAddr,
		LocalStoreDir:             cfg.TikvImporter.SortedKVDir,
		MaxConnPerStore:           cfg.TikvImporter.RangeConcurrency,
		ConnCompressType:          cfg.TikvImporter.CompressKVPairs,
		WorkerConcurrency:         cfg.TikvImporter.RangeConcurrency * 2,
		KVWriteBatchSize:          int64(cfg.TikvImporter.SendKVSize),
		RegionSplitBatchSize:      cfg.TikvImporter.RegionSplitBatchSize,
		RegionSplitConcurrency:    cfg.TikvImporter.RegionSplitConcurrency,
		CheckpointEnabled:         cfg.Checkpoint.Enable,
		MemTableSize:              int(cfg.TikvImporter.EngineMemCacheSize),
		LocalWriterMemCacheSize:   int64(cfg.TikvImporter.LocalWriterMemCacheSize),
		ShouldCheckTiKV:           cfg.App.CheckRequirements,
		DupeDetectEnabled:         cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone,
		DuplicateDetectOpt:        common.DupDetectOpt{ReportErrOnDup: cfg.TikvImporter.DuplicateResolution == config.DupeResAlgErr},
		StoreWriteBWLimit:         int(cfg.TikvImporter.StoreWriteBWLimit),
		ShouldCheckWriteStall:     cfg.Cron.SwitchMode.Duration == 0,
		MaxOpenFiles:              maxOpenFiles,
		KeyspaceName:              keyspaceName,
		PausePDSchedulerScope:     cfg.TikvImporter.PausePDSchedulerScope,
		ResourceGroupName:         resourceGroupName,
		TaskType:                  taskType,
		RaftKV2SwitchModeDuration: raftKV2SwitchModeDuration,
	}
}

func (c *BackendConfig) adjust() {
	c.MaxOpenFiles = mathutil.Max(c.MaxOpenFiles, openFilesLowerThreshold)
}

// Backend is a local backend.
type Backend struct {
	engines sync.Map // sync version of map[uuid.UUID]*Engine

	pdCtl            *pdutil.PdController
	splitCli         split.SplitClient
	tikvCli          *tikvclient.KVStore
	tls              *common.TLS
	regionSizeGetter TableRegionSizeGetter
	tikvCodec        tikvclient.Codec

	BackendConfig

	supportMultiIngest  bool
	duplicateDB         *pebble.DB
	keyAdapter          common.KeyAdapter
	importClientFactory ImportClientFactory

	bufferPool   *membuf.Pool
	metrics      *metric.Metrics
	writeLimiter StoreWriteLimiter
	logger       log.Logger
}

var _ DiskUsage = (*Backend)(nil)
var _ backend.Backend = (*Backend)(nil)

func openDuplicateDB(storeDir string) (*pebble.DB, error) {
	dbPath := filepath.Join(storeDir, duplicateDBName)
	// TODO: Optimize the opts for better write.
	opts := &pebble.Options{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	return pebble.Open(dbPath, opts)
}

var (
	// RunInTest indicates whether the current process is running in test.
	RunInTest bool
	// LastAlloc is the last ID allocator.
	LastAlloc manual.Allocator
)

// NewBackend creates new connections to tikv.
func NewBackend(
	ctx context.Context,
	tls *common.TLS,
	config BackendConfig,
	regionSizeGetter TableRegionSizeGetter,
) (*Backend, error) {
	config.adjust()
	pdCtl, err := pdutil.NewPdController(ctx, config.PDAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}
	splitCli := split.NewSplitClient(pdCtl.GetPDClient(), tls.TLSConfig(), false)

	shouldCreate := true
	if config.CheckpointEnabled {
		if info, err := os.Stat(config.LocalStoreDir); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err = os.Mkdir(config.LocalStoreDir, 0o700)
		if err != nil {
			return nil, common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(config.LocalStoreDir)
		}
	}

	var duplicateDB *pebble.DB
	if config.DupeDetectEnabled {
		duplicateDB, err = openDuplicateDB(config.LocalStoreDir)
		if err != nil {
			return nil, common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
	}

	// The following copies tikv.NewTxnClient without creating yet another pdClient.
	spkv, err := tikvclient.NewEtcdSafePointKV(strings.Split(config.PDAddr, ","), tls.TLSConfig())
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	var pdCliForTiKV *tikvclient.CodecPDClient
	if config.KeyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCtl.GetPDClient())
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCtl.GetPDClient(), config.KeyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	rpcCli := tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err := tikvclient.NewKVStore("lightning-local-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	importClientFactory := newImportClientFactoryImpl(splitCli, tls, config.MaxConnPerStore, config.ConnCompressType)
	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if config.DupeDetectEnabled {
		keyAdapter = common.DupDetectKeyAdapter{}
	}
	var writeLimiter StoreWriteLimiter
	if config.StoreWriteBWLimit > 0 {
		writeLimiter = newStoreWriteLimiter(config.StoreWriteBWLimit)
	} else {
		writeLimiter = noopStoreWriteLimiter{}
	}
	alloc := manual.Allocator{}
	if RunInTest {
		alloc.RefCnt = new(atomic.Int64)
		LastAlloc = alloc
	}
	local := &Backend{
		engines:          sync.Map{},
		pdCtl:            pdCtl,
		splitCli:         splitCli,
		tikvCli:          tikvCli,
		tls:              tls,
		regionSizeGetter: regionSizeGetter,
		tikvCodec:        tikvCodec,

		BackendConfig: config,

		duplicateDB:         duplicateDB,
		keyAdapter:          keyAdapter,
		importClientFactory: importClientFactory,
		bufferPool:          membuf.NewPool(membuf.WithAllocator(alloc)),
		writeLimiter:        writeLimiter,
		logger:              log.FromContext(ctx),
	}
	if m, ok := metric.FromContext(ctx); ok {
		local.metrics = m
	}
	if err = local.checkMultiIngestSupport(ctx); err != nil {
		return nil, common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}

	return local, nil
}

// TotalMemoryConsume returns the total memory usage of the local backend.
func (local *Backend) TotalMemoryConsume() int64 {
	var memConsume int64
	local.engines.Range(func(k, v interface{}) bool {
		e := v.(*Engine)
		if e != nil {
			memConsume += e.TotalMemorySize()
		}
		return true
	})
	return memConsume + local.bufferPool.TotalSize()
}

func (local *Backend) checkMultiIngestSupport(ctx context.Context) error {
	stores, err := local.pdCtl.GetPDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
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
			client, err1 := local.getImportClient(ctx, s.Id)
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
					local.supportMultiIngest = false
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
			local.supportMultiIngest = false
			return nil
		}
	}

	local.supportMultiIngest = true
	log.FromContext(ctx).Info("multi ingest support")
	return nil
}

// rlock read locks a local file and returns the Engine instance if it exists.
func (local *Backend) rLockEngine(engineID uuid.UUID) *Engine {
	if e, ok := local.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.rLock()
		return engine
	}
	return nil
}

// lock locks a local file and returns the Engine instance if it exists.
func (local *Backend) lockEngine(engineID uuid.UUID, state importMutexState) *Engine {
	if e, ok := local.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.lock(state)
		return engine
	}
	return nil
}

// tryRLockAllEngines tries to read lock all engines, return all `Engine`s that are successfully locked.
func (local *Backend) tryRLockAllEngines() []*Engine {
	var allEngines []*Engine
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		// skip closed engine
		if engine.tryRLock() {
			if !engine.closed.Load() {
				allEngines = append(allEngines, engine)
			} else {
				engine.rUnlock()
			}
		}
		return true
	})
	return allEngines
}

// lockAllEnginesUnless tries to lock all engines, unless those which are already locked in the
// state given by ignoreStateMask. Returns the list of locked engines.
func (local *Backend) lockAllEnginesUnless(newState, ignoreStateMask importMutexState) []*Engine {
	var allEngines []*Engine
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		if engine.lockUnless(newState, ignoreStateMask) {
			allEngines = append(allEngines, engine)
		}
		return true
	})
	return allEngines
}

// Close the local backend.
func (local *Backend) Close() {
	allEngines := local.lockAllEnginesUnless(importMutexStateClose, 0)
	local.engines = sync.Map{}

	for _, engine := range allEngines {
		_ = engine.Close()
		engine.unlock()
	}

	local.importClientFactory.Close()
	local.bufferPool.Destroy()

	if local.duplicateDB != nil {
		// Check if there are duplicates that are not collected.
		iter := local.duplicateDB.NewIter(&pebble.IterOptions{})
		hasDuplicates := iter.First()
		allIsWell := true
		if err := iter.Error(); err != nil {
			local.logger.Warn("iterate duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		if err := iter.Close(); err != nil {
			local.logger.Warn("close duplicate db iter failed", zap.Error(err))
			allIsWell = false
		}
		if err := local.duplicateDB.Close(); err != nil {
			local.logger.Warn("close duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		// If checkpoint is disabled, or we don't detect any duplicate, then this duplicate
		// db dir will be useless, so we clean up this dir.
		if allIsWell && (!local.CheckpointEnabled || !hasDuplicates) {
			if err := os.RemoveAll(filepath.Join(local.LocalStoreDir, duplicateDBName)); err != nil {
				local.logger.Warn("remove duplicate db file failed", zap.Error(err))
			}
		}
		local.duplicateDB = nil
	}

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !local.CheckpointEnabled || common.IsEmptyDir(local.LocalStoreDir) {
		err := os.RemoveAll(local.LocalStoreDir)
		if err != nil {
			local.logger.Warn("remove local db file failed", zap.Error(err))
		}
	}
	_ = local.tikvCli.Close()
	local.pdCtl.Close()
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *Backend) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
	engine := local.rLockEngine(engineID)

	// the engine cannot be deleted after while we've acquired the lock identified by UUID.
	if engine == nil {
		return errors.Errorf("engine '%s' not found", engineID)
	}
	defer engine.rUnlock()
	if engine.closed.Load() {
		return nil
	}
	return engine.flushEngineWithoutLock(ctx)
}

// FlushAllEngines flush all engines.
func (local *Backend) FlushAllEngines(parentCtx context.Context) (err error) {
	allEngines := local.tryRLockAllEngines()
	defer func() {
		for _, engine := range allEngines {
			engine.rUnlock()
		}
	}()

	eg, ctx := errgroup.WithContext(parentCtx)
	for _, engine := range allEngines {
		e := engine
		eg.Go(func() error {
			return e.flushEngineWithoutLock(ctx)
		})
	}
	return eg.Wait()
}

// RetryImportDelay returns the delay time before retrying to import a file.
func (*Backend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

// ShouldPostProcess returns true if the backend should post process the data.
func (*Backend) ShouldPostProcess() bool {
	return true
}

func (local *Backend) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize: local.MemTableSize,
		// the default threshold value may cause write stall.
		MemTableStopWritesThreshold: 8,
		MaxConcurrentCompactions:    16,
		// set threshold to half of the max open files to avoid trigger compaction
		L0CompactionThreshold: math.MaxInt32,
		L0StopWritesThreshold: math.MaxInt32,
		LBaseMaxBytes:         16 * units.TiB,
		MaxOpenFiles:          local.MaxOpenFiles,
		DisableWAL:            true,
		ReadOnly:              readOnly,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	// set level target file size to avoid pebble auto triggering compaction that split ingest SST files into small SST.
	opt.Levels = []pebble.LevelOptions{
		{
			TargetFileSize: 16 * units.GiB,
		},
	}

	dbPath := filepath.Join(local.LocalStoreDir, engineUUID.String())
	db, err := pebble.Open(dbPath, opt)
	return db, errors.Trace(err)
}

// OpenEngine must be called with holding mutex of Engine.
func (local *Backend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	db, err := local.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}

	sstDir := engineSSTDir(local.LocalStoreDir, engineUUID)
	if !cfg.KeepSortDir {
		if err := os.RemoveAll(sstDir); err != nil {
			return errors.Trace(err)
		}
	}
	if !common.IsDirExists(sstDir) {
		if err := os.Mkdir(sstDir, 0o750); err != nil {
			return errors.Trace(err)
		}
	}
	engineCtx, cancel := context.WithCancel(ctx)

	e, _ := local.engines.LoadOrStore(engineUUID, &Engine{
		UUID:               engineUUID,
		sstDir:             sstDir,
		sstMetasChan:       make(chan metaOrFlush, 64),
		ctx:                engineCtx,
		cancel:             cancel,
		config:             cfg.Local,
		tableInfo:          cfg.TableInfo,
		duplicateDetection: local.DupeDetectEnabled,
		dupDetectOpt:       local.DuplicateDetectOpt,
		duplicateDB:        local.duplicateDB,
		keyAdapter:         local.keyAdapter,
		logger:             log.FromContext(ctx),
	})
	engine := e.(*Engine)
	engine.lock(importMutexStateOpen)
	defer engine.unlock()
	engine.db.Store(db)
	engine.sstIngester = dbSSTIngester{e: engine}
	if err = engine.loadEngineMeta(); err != nil {
		return errors.Trace(err)
	}
	if err = local.allocateTSIfNotExists(ctx, engine); err != nil {
		return errors.Trace(err)
	}
	engine.wg.Add(1)
	go engine.ingestSSTLoop()
	return nil
}

func (local *Backend) allocateTSIfNotExists(ctx context.Context, engine *Engine) error {
	if engine.TS > 0 {
		return nil
	}
	physical, logical, err := local.pdCtl.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)
	engine.TS = ts
	return engine.saveEngineMeta()
}

// CloseEngine closes backend engine by uuid.
func (local *Backend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engineI, ok := local.engines.Load(engineUUID)
	if !ok {
		// recovery mode, we should reopen this engine file
		db, err := local.openEngineDB(engineUUID, true)
		if err != nil {
			return err
		}
		engine := &Engine{
			UUID:               engineUUID,
			sstMetasChan:       make(chan metaOrFlush),
			tableInfo:          cfg.TableInfo,
			keyAdapter:         local.keyAdapter,
			duplicateDetection: local.DupeDetectEnabled,
			dupDetectOpt:       local.DuplicateDetectOpt,
			duplicateDB:        local.duplicateDB,
			logger:             log.FromContext(ctx),
		}
		engine.db.Store(db)
		engine.sstIngester = dbSSTIngester{e: engine}
		if err = engine.loadEngineMeta(); err != nil {
			return err
		}
		local.engines.Store(engineUUID, engine)
		return nil
	}

	engine := engineI.(*Engine)
	engine.rLock()
	if engine.closed.Load() {
		engine.rUnlock()
		return nil
	}

	err := engine.flushEngineWithoutLock(ctx)
	engine.rUnlock()

	// use mutex to make sure we won't close sstMetasChan while other routines
	// trying to do flush.
	engine.lock(importMutexStateClose)
	engine.closed.Store(true)
	close(engine.sstMetasChan)
	engine.unlock()
	if err != nil {
		return errors.Trace(err)
	}
	engine.wg.Wait()
	return engine.ingestErr.Get()
}

func (local *Backend) getImportClient(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	return local.importClientFactory.Create(ctx, storeID)
}

func splitRangeBySizeProps(fullRange Range, sizeProps *sizeProperties, sizeLimit int64, keysLimit int64) []Range {
	ranges := make([]Range, 0, sizeProps.totalSize/uint64(sizeLimit))
	curSize := uint64(0)
	curKeys := uint64(0)
	curKey := fullRange.start

	sizeProps.iter(func(p *rangeProperty) bool {
		if bytes.Compare(p.Key, curKey) <= 0 {
			return true
		}
		if bytes.Compare(p.Key, fullRange.end) > 0 {
			return false
		}
		curSize += p.Size
		curKeys += p.Keys
		if int64(curSize) >= sizeLimit || int64(curKeys) >= keysLimit {
			ranges = append(ranges, Range{start: curKey, end: p.Key})
			curKey = p.Key
			curSize = 0
			curKeys = 0
		}
		return true
	})

	if bytes.Compare(curKey, fullRange.end) < 0 {
		// If the remaining range is too small, append it to last range.
		if len(ranges) > 0 && curKeys == 0 {
			ranges[len(ranges)-1].end = fullRange.end
		} else {
			ranges = append(ranges, Range{start: curKey, end: fullRange.end})
		}
	}
	return ranges
}

func (local *Backend) readAndSplitIntoRange(
	ctx context.Context,
	engine *Engine,
	sizeLimit int64,
	keysLimit int64,
) ([]Range, error) {
	firstKey, lastKey, err := engine.GetFirstAndLastKey(nil, nil)
	if err != nil {
		return nil, err
	}
	if firstKey == nil {
		return nil, errors.New("could not find first pair")
	}

	endKey := nextKey(lastKey)

	engineFileTotalSize := engine.TotalSize.Load()
	engineFileLength := engine.Length.Load()

	if engineFileTotalSize <= sizeLimit && engineFileLength <= keysLimit {
		ranges := []Range{{start: firstKey, end: endKey}}
		return ranges, nil
	}

	logger := log.FromContext(ctx).With(zap.Stringer("engine", engine.UUID))
	sizeProps, err := getSizePropertiesFn(logger, engine.getDB(), local.keyAdapter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := splitRangeBySizeProps(Range{start: firstKey, end: endKey}, sizeProps,
		sizeLimit, keysLimit)

	logger.Info("split engine key ranges",
		zap.Int64("totalSize", engineFileTotalSize), zap.Int64("totalCount", engineFileLength),
		logutil.Key("firstKey", firstKey), logutil.Key("lastKey", lastKey),
		zap.Int("ranges", len(ranges)))

	return ranges, nil
}

// prepareAndSendJob will read the engine to get estimated key range,
// then split and scatter regions for these range and send region jobs to jobToWorkerCh.
// NOTE when ctx is Done, this function will NOT return error even if it hasn't sent
// all the jobs to jobToWorkerCh. This is because the "first error" can only be
// found by checking the work group LATER, we don't want to return an error to
// seize the "first" error.
func (local *Backend) prepareAndSendJob(
	ctx context.Context,
	engine *Engine,
	initialSplitRanges []Range,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	lfTotalSize := engine.TotalSize.Load()
	lfLength := engine.Length.Load()
	log.FromContext(ctx).Info("import engine ranges", zap.Int("count", len(initialSplitRanges)))
	if len(initialSplitRanges) == 0 {
		return nil
	}

	// if all the kv can fit in one region, skip split regions. TiDB will split one region for
	// the table when table is created.
	needSplit := len(initialSplitRanges) > 1 || lfTotalSize > regionSplitSize || lfLength > regionSplitKeys
	var err error
	// split region by given ranges
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		needSplit = true
	})
	logger := log.FromContext(ctx).With(zap.Stringer("uuid", engine.UUID)).Begin(zap.InfoLevel, "split and scatter ranges")
	for i := 0; i < maxRetryTimes; i++ {
		failpoint.Inject("skipSplitAndScatter", func() {
			failpoint.Break()
		})

		err = local.SplitAndScatterRegionInBatches(ctx, initialSplitRanges, needSplit, maxBatchSplitRanges)
		if err == nil || common.IsContextCanceledError(err) {
			break
		}

		log.FromContext(ctx).Warn("split and scatter failed in retry", zap.Stringer("uuid", engine.UUID),
			log.ShortError(err), zap.Int("retry", i))
	}
	logger.End(zap.ErrorLevel, err)
	if err != nil {
		return err
	}

	return local.generateAndSendJob(
		ctx,
		engine,
		initialSplitRanges,
		regionSplitSize,
		regionSplitKeys,
		jobToWorkerCh,
		jobWg,
	)
}

// generateAndSendJob scans the region in ranges and send region jobs to jobToWorkerCh.
func (local *Backend) generateAndSendJob(
	ctx context.Context,
	engine common.Engine,
	jobRanges []Range,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	logger := log.FromContext(ctx)
	// TODO(lance6716): external engine should also support it
	localEngine, ok := engine.(*Engine)

	// when use dynamic region feature, the region may be very big, we need
	// to split to smaller ranges to increase the concurrency.
	if regionSplitSize > 2*int64(config.SplitRegionSize) && ok {
		sizeProps, err := getSizePropertiesFn(logger, localEngine.getDB(), local.keyAdapter)
		if err != nil {
			return errors.Trace(err)
		}

		jobRanges = splitRangeBySizeProps(
			Range{start: jobRanges[0].start, end: jobRanges[len(jobRanges)-1].end},
			sizeProps,
			int64(config.SplitRegionSize),
			int64(config.SplitRegionKeys))
	}
	logger.Debug("the ranges length write to tikv", zap.Int("length", len(jobRanges)))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(local.WorkerConcurrency)
	for _, jobRange := range jobRanges {
		r := jobRange
		data, err := engine.LoadIngestData(ctx, r.start, r.end)
		if err != nil {
			cancel()
			err2 := eg.Wait()
			if err2 != nil && !common.IsContextCanceledError(err2) {
				logger.Warn("meet error when canceling", log.ShortError(err2))
			}
			return errors.Trace(err)
		}
		eg.Go(func() error {
			if egCtx.Err() != nil {
				return nil
			}

			failpoint.Inject("beforeGenerateJob", nil)
			jobs, err := local.generateJobForRange(egCtx, data, r, regionSplitSize, regionSplitKeys)
			if err != nil {
				if common.IsContextCanceledError(err) {
					return nil
				}
				return err
			}
			for _, job := range jobs {
				jobWg.Add(1)
				select {
				case <-egCtx.Done():
					// this job is not put into jobToWorkerCh
					jobWg.Done()
					// if the context is canceled, it means worker has error, the first error can be
					// found by worker's error group LATER. if this function returns an error it will
					// seize the "first error".
					return nil
				case jobToWorkerCh <- job:
				}
			}
			return nil
		})
	}
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
	keyRange Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	failpoint.Inject("fakeRegionJobs", func() {
		if ctx.Err() != nil {
			failpoint.Return(nil, ctx.Err())
		}
		key := [2]string{string(keyRange.start), string(keyRange.end)}
		injected := fakeRegionJobs[key]
		// overwrite the stage to regionScanned, because some time same keyRange
		// will be generated more than once.
		for _, job := range injected.jobs {
			job.stage = regionScanned
		}
		failpoint.Return(injected.jobs, injected.err)
	})

	start, end := keyRange.start, keyRange.end
	pairStart, pairEnd, err := data.GetFirstAndLastKey(start, end)
	if err != nil {
		return nil, err
	}
	if pairStart == nil {
		log.FromContext(ctx).Info("There is no pairs in range",
			logutil.Key("start", start),
			logutil.Key("end", end))
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

	jobs := make([]*regionJob, 0, len(regions))
	for _, region := range regions {
		log.FromContext(ctx).Debug("get region",
			zap.Binary("startKey", startKey),
			zap.Binary("endKey", endKey),
			zap.Uint64("id", region.Region.GetId()),
			zap.Stringer("epoch", region.Region.GetRegionEpoch()),
			zap.Binary("start", region.Region.GetStartKey()),
			zap.Binary("end", region.Region.GetEndKey()),
			zap.Reflect("peers", region.Region.GetPeers()))

		jobs = append(jobs, &regionJob{
			keyRange:        intersectRange(region.Region, Range{start: start, end: end}),
			region:          region,
			stage:           regionScanned,
			ingestData:      data,
			regionSplitSize: regionSplitSize,
			regionSplitKeys: regionSplitKeys,
			metrics:         local.metrics,
		})
	}
	return jobs, nil
}

// startWorker creates a worker that reads from the job channel and processes.
// startWorker will return nil if it's expected to stop, where the only case is
// the context canceled. It will return not nil error when it actively stops.
// startWorker must Done the jobWg if it does not put the job into jobOutCh.
func (local *Backend) startWorker(
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

			err := local.executeJob(ctx, job)
			switch job.stage {
			case regionScanned, wrote, ingested:
				jobOutCh <- job
			case needRescan:
				jobs, err2 := local.generateJobForRange(
					ctx,
					job.ingestData,
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
				for _, j := range jobs {
					j.lastRetryableErr = job.lastRetryableErr
					jobOutCh <- j
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
			var (
				store *pdtypes.StoreInfo
				err   error
			)
			for i := 0; i < maxRetryTimes; i++ {
				store, err = local.pdCtl.GetStoreInfo(ctx, peer.StoreId)
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
		err := local.writeToTiKV(ctx, job)
		if err != nil {
			if !local.isRetryableImportTiKVError(err) {
				return err
			}
			// if it's retryable error, we retry from scanning region
			log.FromContext(ctx).Warn("meet retryable error when writing to TiKV",
				log.ShortError(err), zap.Stringer("job stage", job.stage))
			job.convertStageTo(needRescan)
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
		job.keyRange.start = job.writeResult.remainingStartKey
		job.convertStageTo(regionScanned)
	}
}

// ImportEngine imports an engine to TiKV.
func (local *Backend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	lf := local.lockEngine(engineUUID, importMutexStateImport)
	if lf == nil {
		// skip if engine not exist. See the comment of `CloseEngine` for more detail.
		return nil
	}
	defer lf.unlock()

	lfTotalSize := lf.TotalSize.Load()
	lfLength := lf.Length.Load()
	if lfTotalSize == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.FromContext(ctx).Info("engine contains no kv, skip import", zap.Stringer("engine", engineUUID))
		return nil
	}
	kvRegionSplitSize, kvRegionSplitKeys, err := getRegionSplitSizeKeys(ctx, local.pdCtl.GetPDClient(), local.tls)
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

	// split sorted file into range about regionSplitSize per file
	regionRanges, err := local.readAndSplitIntoRange(ctx, lf, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}

	if len(regionRanges) > 0 && local.PausePDSchedulerScope == config.PausePDSchedulerScopeTable {
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
		done, err := local.pdCtl.PauseSchedulersByKeyRange(subCtx, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	if len(regionRanges) > 0 && local.BackendConfig.RaftKV2SwitchModeDuration > 0 {
		log.FromContext(ctx).Info("switch import mode of ranges",
			zap.String("startKey", hex.EncodeToString(regionRanges[0].start)),
			zap.String("endKey", hex.EncodeToString(regionRanges[len(regionRanges)-1].end)))
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done, err := local.SwitchModeByKeyRanges(subCtx, regionRanges)
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
		zap.Int("region ranges", len(regionRanges)),
		zap.Int64("count", lfLength),
		zap.Int64("size", lfTotalSize))

	failpoint.Inject("ReadyForImportEngine", func() {})

	err = local.doImport(ctx, lf, regionRanges, regionSplitSize, regionSplitKeys)
	if err == nil {
		log.FromContext(ctx).Info("import engine success",
			zap.Stringer("uuid", engineUUID),
			zap.Int64("size", lfTotalSize),
			zap.Int64("kvs", lfLength),
			zap.Int64("importedSize", lf.importedKVSize.Load()),
			zap.Int64("importedCount", lf.importedKVCount.Load()))
	}
	return err
}

func (local *Backend) doImport(ctx context.Context, engine *Engine, regionRanges []Range, regionSplitSize, regionSplitKeys int64) error {
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

	for i := 0; i < local.WorkerConcurrency; i++ {
		workGroup.Go(func() error {
			return local.startWorker(workerCtx, jobToWorkerCh, jobFromWorkerCh, &jobWg)
		})
	}

	err := local.prepareAndSendJob(
		workerCtx,
		engine,
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

// GetImportedKVCount returns the number of imported KV pairs of some engine.
func (local *Backend) GetImportedKVCount(engineUUID uuid.UUID) int64 {
	v, ok := local.engines.Load(engineUUID)
	if !ok {
		// we get it after import, but before clean up, so this should not happen
		// todo: return error
		return 0
	}
	e := v.(*Engine)
	return e.importedKVCount.Load()
}

// ResetEngine reset the engine and reclaim the space.
func (local *Backend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// the only way to reset the engine + reclaim the space is to delete and reopen it 🤷
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	if localEngine == nil {
		log.FromContext(ctx).Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()
	if err := localEngine.Close(); err != nil {
		return err
	}
	if err := localEngine.Cleanup(local.LocalStoreDir); err != nil {
		return err
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err == nil {
		localEngine.db.Store(db)
		localEngine.engineMeta = engineMeta{}
		if !common.IsDirExists(localEngine.sstDir) {
			if err := os.Mkdir(localEngine.sstDir, 0o750); err != nil {
				return errors.Trace(err)
			}
		}
		if err = local.allocateTSIfNotExists(ctx, localEngine); err != nil {
			return errors.Trace(err)
		}
	}
	localEngine.pendingFileSize.Store(0)

	return err
}

// CleanupEngine cleanup the engine and reclaim the space.
func (local *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	// release this engine after import success
	if localEngine == nil {
		log.FromContext(ctx).Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()

	// since closing the engine causes all subsequent operations on it panic,
	// we make sure to delete it from the engine map before calling Close().
	// (note that Close() returning error does _not_ mean the pebble DB
	// remains open/usable.)
	local.engines.Delete(engineUUID)
	err := localEngine.Close()
	if err != nil {
		return err
	}
	err = localEngine.Cleanup(local.LocalStoreDir)
	if err != nil {
		return err
	}
	localEngine.TotalSize.Store(0)
	localEngine.Length.Store(0)
	return nil
}

// GetDupeController returns a new dupe controller.
func (local *Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *DupeController {
	return &DupeController{
		splitCli:            local.splitCli,
		tikvCli:             local.tikvCli,
		tikvCodec:           local.tikvCodec,
		errorMgr:            errorMgr,
		dupeConcurrency:     dupeConcurrency,
		duplicateDB:         local.duplicateDB,
		keyAdapter:          local.keyAdapter,
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
	return local.ResetEngine(ctx, engineUUID)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

// LocalWriter returns a new local writer.
func (local *Backend) LocalWriter(_ context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engine := e.(*Engine)
	return openLocalWriter(cfg, engine, local.tikvCodec, local.LocalWriterMemCacheSize, local.bufferPool.NewBuffer())
}

// SwitchModeByKeyRanges will switch tikv mode for regions in the specific key range for multirocksdb.
// This function will spawn a goroutine to keep switch mode periodically until the context is done.
// The return done channel is used to notify the caller that the background goroutine is exited.
func (local *Backend) SwitchModeByKeyRanges(ctx context.Context, ranges []Range) (<-chan struct{}, error) {
	switcher := NewTiKVModeSwitcher(local.tls, local.pdCtl.GetPDClient(), log.FromContext(ctx).Logger)
	done := make(chan struct{})

	keyRanges := make([]*sst.Range, 0, len(ranges))
	for _, r := range ranges {
		startKey := r.start
		if len(r.start) > 0 {
			startKey = codec.EncodeBytes(nil, r.start)
		}
		endKey := r.end
		if len(r.end) > 0 {
			endKey = codec.EncodeBytes(nil, r.end)
		}
		keyRanges = append(keyRanges, &sst.Range{
			Start: startKey,
			End:   endKey,
		})
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(local.BackendConfig.RaftKV2SwitchModeDuration)
		defer ticker.Stop()
		switcher.ToImportMode(ctx, keyRanges...)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
				switcher.ToImportMode(ctx, keyRanges...)
			}
		}
		// Use a new context to avoid the context is canceled by the caller.
		recoverCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		switcher.ToNormalMode(recoverCtx, keyRanges...)
	}()
	return done, nil
}

func openLocalWriter(cfg *backend.LocalWriterConfig, engine *Engine, tikvCodec tikvclient.Codec, cacheSize int64, kvBuffer *membuf.Buffer) (*Writer, error) {
	w := &Writer{
		engine:             engine,
		memtableSizeLimit:  cacheSize,
		kvBuffer:           kvBuffer,
		isKVSorted:         cfg.IsKVSorted,
		isWriteBatchSorted: true,
		tikvCodec:          tikvCodec,
	}
	// pre-allocate a long enough buffer to avoid a lot of runtime.growslice
	// this can help save about 3% of CPU.
	if !w.isKVSorted {
		w.writeBatch = make([]common.KvPair, units.MiB)
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
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		res = append(res, engine.getEngineFileSize())
		return true
	})
	return
}

// GetPDClient returns the PD client.
func (local *Backend) GetPDClient() pd.Client {
	return local.pdCtl.GetPDClient()
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

// return region split size, region split keys, error
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
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStoreFunc(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		log.FromContext(ctx).Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}
