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
	"fmt"
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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/manual"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/mathutil"
	tikverror "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
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
	maxRetryBackoffSecond       = 30
	maxRetryBackoffTime         = 30 * time.Second

	gRPCKeepAliveTime    = 10 * time.Minute
	gRPCKeepAliveTimeout = 5 * time.Minute
	gRPCBackOffMaxDelay  = 10 * time.Minute
	writeStallSleepTime  = 10 * time.Second

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

	errorEngineClosed = errors.New("engine is closed")
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
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
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

func (f *importClientFactoryImpl) Create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

func (f *importClientFactoryImpl) Close() {
	f.conns.Close()
}

type loggingConn struct {
	net.Conn
}

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
func NewEncodingBuilder(ctx context.Context) backend.EncodingBuilder {
	result := new(encodingBuilder)
	if m, ok := metric.FromContext(ctx); ok {
		result.metrics = m
	}
	return result
}

// NewEncoder creates a KV encoder.
// It implements the `backend.EncodingBuilder` interface.
func (b *encodingBuilder) NewEncoder(ctx context.Context, tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return kv.NewTableKVEncoder(tbl, options, b.metrics, log.FromContext(ctx))
}

// MakeEmptyRows creates an empty KV rows.
// It implements the `backend.EncodingBuilder` interface.
func (b *encodingBuilder) MakeEmptyRows() kv.Rows {
	return kv.MakeRowsFromKvPairs(nil)
}

type targetInfoGetter struct {
	tls          *common.TLS
	targetDBGlue glue.Glue
	pdAddr       string
}

// NewTargetInfoGetter creates an TargetInfoGetter with local backend implementation.
func NewTargetInfoGetter(tls *common.TLS, g glue.Glue, pdAddr string) backend.TargetInfoGetter {
	return &targetInfoGetter{
		tls:          tls,
		targetDBGlue: g,
		pdAddr:       pdAddr,
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
	db, _ := g.targetDBGlue.GetDB()
	versionStr, err := version.FetchVersion(ctx, db)
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkTiDBVersion(ctx, versionStr, localMinTiDBVersion, localMaxTiDBVersion); err != nil {
		return err
	}
	if err := tikv.CheckPDVersion(ctx, g.tls, g.pdAddr, localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, g.tls, g.pdAddr, localMinTiKVVersion, localMaxTiKVVersion); err != nil {
		return err
	}

	serverInfo := version.ParseServerInfo(versionStr)
	return checkTiFlashVersion(ctx, g.targetDBGlue, checkCtx, *serverInfo.ServerVersion)
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
func checkTiFlashVersion(ctx context.Context, g glue.Glue, checkCtx *backend.CheckCtx, tidbVersion semver.Version) error {
	if tidbVersion.Compare(tiFlashMinVersion) >= 0 {
		return nil
	}

	res, err := g.GetSQLExecutor().QueryStringsWithLog(ctx, tiFlashReplicaQuery, "fetch tiflash replica info", log.FromContext(ctx))
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

type local struct {
	engines sync.Map // sync version of map[uuid.UUID]*Engine

	pdCtl     *pdutil.PdController
	splitCli  split.SplitClient
	tikvCli   *tikvclient.KVStore
	tls       *common.TLS
	pdAddr    string
	g         glue.Glue
	tikvCodec tikvclient.Codec

	localStoreDir string

	workerConcurrency int
	kvWriteBatchSize  int
	checkpointEnabled bool

	dupeConcurrency int
	maxOpenFiles    int

	engineMemCacheSize      int
	localWriterMemCacheSize int64
	supportMultiIngest      bool

	shouldCheckTiKV     bool
	duplicateDetection  bool
	duplicateDetectOpt  dupDetectOpt
	duplicateDB         *pebble.DB
	keyAdapter          KeyAdapter
	errorMgr            *errormanager.ErrorManager
	importClientFactory ImportClientFactory

	bufferPool   *membuf.Pool
	metrics      *metric.Metrics
	writeLimiter StoreWriteLimiter
	logger       log.Logger

	encBuilder       backend.EncodingBuilder
	targetInfoGetter backend.TargetInfoGetter

	// When TiKV is in normal mode, ingesting too many SSTs will cause TiKV write stall.
	// To avoid this, we should check write stall before ingesting SSTs. Note that, we
	// must check both leader node and followers in client side, because followers will
	// not check write stall as long as ingest command is accepted by leader.
	shouldCheckWriteStall bool
}

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

// NewLocalBackend creates new connections to tikv.
func NewLocalBackend(
	ctx context.Context,
	tls *common.TLS,
	cfg *config.Config,
	g glue.Glue,
	maxOpenFiles int,
	errorMgr *errormanager.ErrorManager,
	keyspaceName string,
) (backend.Backend, error) {
	localFile := cfg.TikvImporter.SortedKVDir
	rangeConcurrency := cfg.TikvImporter.RangeConcurrency

	pdCtl, err := pdutil.NewPdController(ctx, cfg.TiDB.PdAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return backend.MakeBackend(nil), common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}
	splitCli := split.NewSplitClient(pdCtl.GetPDClient(), tls.TLSConfig(), false)

	shouldCreate := true
	if cfg.Checkpoint.Enable {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return backend.MakeBackend(nil), err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err = os.Mkdir(localFile, 0o700)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(localFile)
		}
	}

	var duplicateDB *pebble.DB
	if cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone {
		duplicateDB, err = openDuplicateDB(localFile)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
	}

	// The following copies tikv.NewTxnClient without creating yet another pdClient.
	spkv, err := tikvclient.NewEtcdSafePointKV(strings.Split(cfg.TiDB.PdAddr, ","), tls.TLSConfig())
	if err != nil {
		return backend.MakeBackend(nil), common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	var pdCliForTiKV *tikvclient.CodecPDClient
	if keyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCtl.GetPDClient())
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCtl.GetPDClient(), keyspaceName)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	rpcCli := tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err := tikvclient.NewKVStore("lightning-local-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return backend.MakeBackend(nil), common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	importClientFactory := newImportClientFactoryImpl(splitCli, tls, rangeConcurrency, cfg.TikvImporter.CompressKVPairs)
	duplicateDetection := cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone
	keyAdapter := KeyAdapter(noopKeyAdapter{})
	if duplicateDetection {
		keyAdapter = dupDetectKeyAdapter{}
	}
	var writeLimiter StoreWriteLimiter
	if cfg.TikvImporter.StoreWriteBWLimit > 0 {
		writeLimiter = newStoreWriteLimiter(int(cfg.TikvImporter.StoreWriteBWLimit))
	} else {
		writeLimiter = noopStoreWriteLimiter{}
	}
	alloc := manual.Allocator{}
	if RunInTest {
		alloc.RefCnt = new(atomic.Int64)
		LastAlloc = alloc
	}
	local := &local{
		engines:   sync.Map{},
		pdCtl:     pdCtl,
		splitCli:  splitCli,
		tikvCli:   tikvCli,
		tls:       tls,
		pdAddr:    cfg.TiDB.PdAddr,
		g:         g,
		tikvCodec: tikvCodec,

		localStoreDir:     localFile,
		workerConcurrency: rangeConcurrency * 2,
		dupeConcurrency:   rangeConcurrency * 2,
		kvWriteBatchSize:  cfg.TikvImporter.SendKVPairs,
		checkpointEnabled: cfg.Checkpoint.Enable,
		maxOpenFiles:      mathutil.Max(maxOpenFiles, openFilesLowerThreshold),

		engineMemCacheSize:      int(cfg.TikvImporter.EngineMemCacheSize),
		localWriterMemCacheSize: int64(cfg.TikvImporter.LocalWriterMemCacheSize),
		duplicateDetection:      duplicateDetection,
		duplicateDetectOpt:      dupDetectOpt{duplicateDetection && cfg.TikvImporter.DuplicateResolution == config.DupeResAlgErr},
		shouldCheckTiKV:         cfg.App.CheckRequirements,
		duplicateDB:             duplicateDB,
		keyAdapter:              keyAdapter,
		errorMgr:                errorMgr,
		importClientFactory:     importClientFactory,
		bufferPool:              membuf.NewPool(membuf.WithAllocator(alloc)),
		writeLimiter:            writeLimiter,
		logger:                  log.FromContext(ctx),
		encBuilder:              NewEncodingBuilder(ctx),
		targetInfoGetter:        NewTargetInfoGetter(tls, g, cfg.TiDB.PdAddr),
		shouldCheckWriteStall:   cfg.Cron.SwitchMode.Duration == 0,
	}
	if m, ok := metric.FromContext(ctx); ok {
		local.metrics = m
	}
	if err = local.checkMultiIngestSupport(ctx); err != nil {
		return backend.MakeBackend(nil), common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}

	return backend.MakeBackend(local), nil
}

func (local *local) TotalMemoryConsume() int64 {
	var memConsume int64 = 0
	local.engines.Range(func(k, v interface{}) bool {
		e := v.(*Engine)
		if e != nil {
			memConsume += e.TotalMemorySize()
		}
		return true
	})
	return memConsume + local.bufferPool.TotalSize()
}

func (local *local) checkMultiIngestSupport(ctx context.Context) error {
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
func (local *local) rLockEngine(engineId uuid.UUID) *Engine {
	if e, ok := local.engines.Load(engineId); ok {
		engine := e.(*Engine)
		engine.rLock()
		return engine
	}
	return nil
}

// lock locks a local file and returns the Engine instance if it exists.
func (local *local) lockEngine(engineID uuid.UUID, state importMutexState) *Engine {
	if e, ok := local.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.lock(state)
		return engine
	}
	return nil
}

// tryRLockAllEngines tries to read lock all engines, return all `Engine`s that are successfully locked.
func (local *local) tryRLockAllEngines() []*Engine {
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
func (local *local) lockAllEnginesUnless(newState, ignoreStateMask importMutexState) []*Engine {
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
func (local *local) Close() {
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
		if allIsWell && (!local.checkpointEnabled || !hasDuplicates) {
			if err := os.RemoveAll(filepath.Join(local.localStoreDir, duplicateDBName)); err != nil {
				local.logger.Warn("remove duplicate db file failed", zap.Error(err))
			}
		}
		local.duplicateDB = nil
	}

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !local.checkpointEnabled || common.IsEmptyDir(local.localStoreDir) {
		err := os.RemoveAll(local.localStoreDir)
		if err != nil {
			local.logger.Warn("remove local db file failed", zap.Error(err))
		}
	}
	_ = local.tikvCli.Close()
	local.pdCtl.Close()
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *local) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
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

func (local *local) FlushAllEngines(parentCtx context.Context) (err error) {
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

func (local *local) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (local *local) ShouldPostProcess() bool {
	return true
}

func (local *local) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize: local.engineMemCacheSize,
		// the default threshold value may cause write stall.
		MemTableStopWritesThreshold: 8,
		MaxConcurrentCompactions:    16,
		// set threshold to half of the max open files to avoid trigger compaction
		L0CompactionThreshold: math.MaxInt32,
		L0StopWritesThreshold: math.MaxInt32,
		LBaseMaxBytes:         16 * units.TiB,
		MaxOpenFiles:          local.maxOpenFiles,
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

	dbPath := filepath.Join(local.localStoreDir, engineUUID.String())
	db, err := pebble.Open(dbPath, opt)
	return db, errors.Trace(err)
}

// OpenEngine must be called with holding mutex of Engine.
func (local *local) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	engineCfg := backend.LocalEngineConfig{}
	if cfg.Local != nil {
		engineCfg = *cfg.Local
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}

	sstDir := engineSSTDir(local.localStoreDir, engineUUID)
	if err := os.RemoveAll(sstDir); err != nil {
		return errors.Trace(err)
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
		config:             engineCfg,
		tableInfo:          cfg.TableInfo,
		duplicateDetection: local.duplicateDetection,
		dupDetectOpt:       local.duplicateDetectOpt,
		duplicateDB:        local.duplicateDB,
		errorMgr:           local.errorMgr,
		keyAdapter:         local.keyAdapter,
		logger:             log.FromContext(ctx),
	})
	engine := e.(*Engine)
	engine.db = db
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

func (local *local) allocateTSIfNotExists(ctx context.Context, engine *Engine) error {
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
func (local *local) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
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
			db:                 db,
			sstMetasChan:       make(chan metaOrFlush),
			tableInfo:          cfg.TableInfo,
			keyAdapter:         local.keyAdapter,
			duplicateDetection: local.duplicateDetection,
			dupDetectOpt:       local.duplicateDetectOpt,
			duplicateDB:        local.duplicateDB,
			errorMgr:           local.errorMgr,
			logger:             log.FromContext(ctx),
		}
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

func (local *local) getImportClient(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
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

func (local *local) readAndSplitIntoRange(ctx context.Context, engine *Engine, regionSplitSize int64, regionSplitKeys int64) ([]Range, error) {
	firstKey, lastKey, err := engine.getFirstAndLastKey(nil, nil)
	if err != nil {
		return nil, err
	}
	if firstKey == nil {
		return nil, errors.New("could not find first pair")
	}

	endKey := nextKey(lastKey)

	engineFileTotalSize := engine.TotalSize.Load()
	engineFileLength := engine.Length.Load()

	// <= 96MB no need to split into range
	if engineFileTotalSize <= regionSplitSize && engineFileLength <= regionSplitKeys {
		ranges := []Range{{start: firstKey, end: endKey}}
		return ranges, nil
	}

	logger := log.FromContext(ctx).With(zap.Stringer("engine", engine.UUID))
	sizeProps, err := getSizeProperties(logger, engine.db, local.keyAdapter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := splitRangeBySizeProps(Range{start: firstKey, end: endKey}, sizeProps,
		regionSplitSize, regionSplitKeys)

	logger.Info("split engine key ranges",
		zap.Int64("totalSize", engineFileTotalSize), zap.Int64("totalCount", engineFileLength),
		logutil.Key("firstKey", firstKey), logutil.Key("lastKey", lastKey),
		zap.Int("ranges", len(ranges)))

	return ranges, nil
}

// prepareAndGenerateUnfinishedJob will read the engine to get unfinished key range,
// then split and scatter regions for these range and generate region jobs.
func (local *local) prepareAndGenerateUnfinishedJob(
	ctx context.Context,
	engineUUID uuid.UUID,
	lf *Engine,
	initialSplitRanges []Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	lfTotalSize := lf.TotalSize.Load()
	lfLength := lf.Length.Load()
	unfinishedRanges := lf.unfinishedRanges(initialSplitRanges)
	log.FromContext(ctx).Info("import engine unfinished ranges", zap.Int("count", len(unfinishedRanges)))

	if len(unfinishedRanges) > 0 {
		// if all the kv can fit in one region, skip split regions. TiDB will split one region for
		// the table when table is created.
		needSplit := len(unfinishedRanges) > 1 || lfTotalSize > regionSplitSize || lfLength > regionSplitKeys
		var err error
		// split region by given ranges
		failpoint.Inject("failToSplit", func(_ failpoint.Value) {
			needSplit = true
		})
		for i := 0; i < maxRetryTimes; i++ {
			err = local.SplitAndScatterRegionInBatches(ctx, unfinishedRanges, lf.tableInfo, needSplit, regionSplitSize, maxBatchSplitRanges)
			if err == nil || common.IsContextCanceledError(err) {
				break
			}

			log.FromContext(ctx).Warn("split and scatter failed in retry", zap.Stringer("uuid", engineUUID),
				log.ShortError(err), zap.Int("retry", i))
		}
		if err != nil {
			log.FromContext(ctx).Error("split & scatter ranges failed", zap.Stringer("uuid", engineUUID), log.ShortError(err))
			return nil, err
		}
	}

	return local.generateJobInRanges(
		ctx,
		lf,
		unfinishedRanges,
		regionSplitSize,
		regionSplitKeys,
	)
}

// generateJobInRanges scans the region in ranges and generate region jobs.
func (local *local) generateJobInRanges(
	ctx context.Context,
	engine *Engine,
	ranges []Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	log.FromContext(ctx).Debug("the ranges Length write to tikv", zap.Int("Length", len(ranges)))

	ret := make([]*regionJob, 0, len(ranges))

	for _, r := range ranges {
		start, end := r.start, r.end
		pairStart, pairEnd, err := engine.getFirstAndLastKey(start, end)
		if err != nil {
			return nil, err
		}
		if pairStart == nil {
			log.FromContext(ctx).Info("There is no pairs in range",
				logutil.Key("start", start),
				logutil.Key("end", end))
			engine.finishedRanges.add(Range{start: start, end: end})
			continue
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

		for _, region := range regions {
			log.FromContext(ctx).Debug("get region",
				zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey),
				zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()),
				zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()),
				zap.Reflect("peers", region.Region.GetPeers()))

			job := &regionJob{
				keyRange:        intersectRange(region.Region, Range{start: start, end: end}),
				region:          region,
				stage:           regionScanned,
				engine:          engine,
				regionSplitSize: regionSplitSize,
				regionSplitKeys: regionSplitKeys,
				metrics:         local.metrics,
			}

			ret = append(ret, job)
		}
	}
	return ret, nil
}

// startWorker creates a worker that reads from the job channel and processes.
// startWorker will return nil if it's expected to stop, which means the context
// is canceled or channel is closed. It will return not nil error when it actively
// stops.
// this function must send the job back to jobOutCh after read it from jobInCh,
// even if any error happens.
func (local *local) startWorker(
	ctx context.Context,
	jobInCh, jobOutCh chan *regionJob,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case job, ok := <-jobInCh:
			if !ok {
				return nil
			}

			now := time.Now()
			if now.Before(job.waitUntil) {
				duration := job.waitUntil.Sub(now)
				log.FromContext(ctx).Debug("need to wait before processing this job",
					zap.Duration("wait", duration))
				select {
				case <-ctx.Done():
					jobOutCh <- job
					return nil
				case <-time.After(duration):
				}
			}

			err := local.executeJob(ctx, job)
			jobOutCh <- job
			if err != nil {
				return err
			}
		}
	}
}

func (local *local) isRetryableImportTiKVError(err error) bool {
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
// The ingested job will record finished ranges in engine as a checkpoint.
// If non-retryable error occurs, it will return the error.
// If retryable error occurs, it will return nil and caller should check the stage
// of the regionJob to determine what to do with it.
func (local *local) executeJob(
	ctx context.Context,
	job *regionJob,
) error {
	failpoint.Inject("WriteToTiKVNotEnoughDiskSpace", func(_ failpoint.Value) {
		failpoint.Return(
			errors.Errorf("The available disk of TiKV (%s) only left %d, and capacity is %d", "", 0, 0))
	})
	if local.shouldCheckTiKV {
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
						return errors.Errorf("The available disk of TiKV (%s) only left %d, and capacity is %d",
							store.Store.Address, store.Status.Available, store.Status.Capacity)
					}
				}
				break
			}
			if err != nil {
				log.FromContext(ctx).Error("failed to get StoreInfo from pd http api", zap.Error(err))
			}
		}
	}

	err := job.writeToTiKV(ctx,
		local.tikvCodec.GetAPIVersion(),
		local.importClientFactory,
		local.kvWriteBatchSize,
		local.bufferPool,
		local.writeLimiter)
	if err != nil {
		if !local.isRetryableImportTiKVError(err) {
			return err
		}
		log.FromContext(ctx).Warn("meet retryable error when writing to TiKV",
			log.ShortError(err), zap.Stringer("job stage", job.stage))
		job.lastRetryableErr = err
		return nil
	}

	err = job.ingest(
		ctx,
		local.importClientFactory,
		local.splitCli,
		local.supportMultiIngest,
		local.shouldCheckWriteStall,
	)
	if err != nil {
		if !local.isRetryableImportTiKVError(err) {
			return err
		}
		log.FromContext(ctx).Warn("meet retryable error when ingesting",
			log.ShortError(err), zap.Stringer("job stage", job.stage))
		job.lastRetryableErr = err
	}
	return nil
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
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

	// split sorted file into range by 96MB size per file
	ranges, err := local.readAndSplitIntoRange(ctx, lf, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}

	if len(ranges) > 0 && local.pdCtl.CanPauseSchedulerByKeyRange() {
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var startKey, endKey []byte
		if len(ranges[0].start) > 0 {
			startKey = codec.EncodeBytes(nil, ranges[0].start)
		}
		if len(ranges[len(ranges)-1].end) > 0 {
			endKey = codec.EncodeBytes(nil, ranges[len(ranges)-1].end)
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

	log.FromContext(ctx).Info("start import engine", zap.Stringer("uuid", engineUUID),
		zap.Int("ranges", len(ranges)), zap.Int64("count", lfLength), zap.Int64("size", lfTotalSize))

	failpoint.Inject("ReadyForImportEngine", func() {})

	var (
		ctx2, workerCancel   = context.WithCancel(ctx)
		workGroup, workerCtx = errgroup.WithContext(ctx2)
		// jobToWorkerCh is unbuffered so when we finished sending all jobs, we can make sure all jobs have been
		// received by workers.
		jobToWorkerCh   = make(chan *regionJob)
		jobFromWorkerCh = make(chan *regionJob)
		// jobWg is the number of jobs that need to be processed by worker in this round.
		jobWg              sync.WaitGroup
		jobsNeedRetry      []*regionJob
		retryErr           atomic.Error
		retryGoroutineDone = make(chan struct{})
	)

	// handle processed job from worker, it will only exit when jobFromWorkerCh
	// is closed to avoid send to jobFromWorkerCh is blocked.
	defer func() {
		close(jobFromWorkerCh)
		<-retryGoroutineDone
	}()
	go func() {
		defer close(retryGoroutineDone)
		for {
			job, ok := <-jobFromWorkerCh
			if !ok {
				return
			}
			switch job.stage {
			case regionScanned, wrote:
				job.retryCount++
				if job.retryCount > maxWriteAndIngestRetryTimes {
					retryErr.Store(job.lastRetryableErr)
					workerCancel()
					jobWg.Done()
					continue
				}
				// max retry backoff time: 2+4+8+16+30*26=810s
				sleepSecond := math.Pow(2, float64(job.retryCount))
				if sleepSecond > maxRetryBackoffSecond {
					sleepSecond = maxRetryBackoffSecond
				}
				job.waitUntil = time.Now().Add(time.Second * time.Duration(sleepSecond))
				log.FromContext(ctx).Info("put job back to jobCh to retry later",
					logutil.Key("startKey", job.keyRange.start),
					logutil.Key("endKey", job.keyRange.end),
					zap.Stringer("stage", job.stage),
					zap.Int("retryCount", job.retryCount),
					zap.Time("waitUntil", job.waitUntil))
				jobsNeedRetry = append(jobsNeedRetry, job)
			case ingested, needRescan:
			}
			jobWg.Done()
		}
	}()

	for i := 0; i < local.workerConcurrency; i++ {
		workGroup.Go(func() error {
			return local.startWorker(workerCtx, jobToWorkerCh, jobFromWorkerCh)
		})
	}

	var pendingJobs []*regionJob
	for {
		pendingJobs = append(pendingJobs, jobsNeedRetry...)
		jobsNeedRetry = nil
		log.FromContext(ctx).Info("import engine pending jobs", zap.Int("count", len(pendingJobs)))

		newJobs, err := local.prepareAndGenerateUnfinishedJob(
			ctx,
			engineUUID,
			lf,
			ranges,
			regionSplitSize,
			regionSplitKeys,
		)
		if err != nil {
			close(jobToWorkerCh)
			_ = workGroup.Wait()
			return err
		}

		pendingJobs = append(pendingJobs, newJobs...)
		if len(pendingJobs) == 0 {
			break
		}

		jobWg.Add(len(pendingJobs))
		for _, job := range pendingJobs {
			select {
			case <-workerCtx.Done():
				err2 := retryErr.Load()
				if err2 != nil {
					return errors.Trace(err2)
				}
				return errors.Trace(workGroup.Wait())
			case jobToWorkerCh <- job:
			}
		}
		pendingJobs = nil
		// all jobs are received by workers and worker will always call jobWg.Done() after processing
		// no need to check workerCtx.Done() here.
		jobWg.Wait()
		// check if worker has error in this round
		select {
		case <-workerCtx.Done():
			groupErr := workGroup.Wait()
			err2 := retryErr.Load()
			if err2 != nil {
				return errors.Trace(err2)
			}
			return errors.Trace(groupErr)
		default:
		}
	}

	close(jobToWorkerCh)
	log.FromContext(ctx).Info("import engine success", zap.Stringer("uuid", engineUUID),
		zap.Int64("size", lfTotalSize), zap.Int64("kvs", lfLength),
		zap.Int64("importedSize", lf.importedKVSize.Load()), zap.Int64("importedCount", lf.importedKVCount.Load()))
	return workGroup.Wait()
}

func (local *local) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect local duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	atomicHasDupe := atomic.NewBool(false)
	duplicateManager, err := NewDuplicateManager(tbl, tableName, local.splitCli, local.tikvCli, local.tikvCodec,
		local.errorMgr, opts, local.dupeConcurrency, atomicHasDupe, log.FromContext(ctx))
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromDupDB(ctx, local.duplicateDB, local.keyAdapter); err != nil {
		return false, errors.Trace(err)
	}
	return atomicHasDupe.Load(), nil
}

func (local *local) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect remote duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	atomicHasDupe := atomic.NewBool(false)
	duplicateManager, err := NewDuplicateManager(tbl, tableName, local.splitCli, local.tikvCli, local.tikvCodec,
		local.errorMgr, opts, local.dupeConcurrency, atomicHasDupe, log.FromContext(ctx))
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromTiKV(ctx, local.importClientFactory); err != nil {
		return false, errors.Trace(err)
	}
	return atomicHasDupe.Load(), nil
}

func (local *local) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) (err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[resolve-dupe] resolve duplicate rows")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	switch algorithm {
	case config.DupeResAlgRecord, config.DupeResAlgNone:
		logger.Warn("[resolve-dupe] skipping resolution due to selected algorithm. this table will become inconsistent!", zap.Stringer("algorithm", algorithm))
		return nil
	case config.DupeResAlgRemove:
	default:
		panic(fmt.Sprintf("[resolve-dupe] unknown resolution algorithm %v", algorithm))
	}

	// TODO: reuse the *kv.SessionOptions from NewEncoder for picking the correct time zone.
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, &kv.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
	}, log.FromContext(ctx))
	if err != nil {
		return err
	}

	tableIDs := physicalTableIDs(tbl.Meta())
	keyInTable := func(key []byte) bool {
		return slices.Contains(tableIDs, tablecodec.DecodeTableID(key))
	}

	errLimiter := rate.NewLimiter(1, 1)
	pool := utils.NewWorkerPool(uint(local.dupeConcurrency), "resolve duplicate rows")
	err = local.errorMgr.ResolveAllConflictKeys(
		ctx, tableName, pool,
		func(ctx context.Context, handleRows [][2][]byte) error {
			for {
				err := local.deleteDuplicateRows(ctx, logger, handleRows, decoder, keyInTable)
				if err == nil {
					return nil
				}
				if types.ErrBadNumber.Equal(err) {
					logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
					return common.ErrResolveDuplicateRows.Wrap(err).GenWithStackByArgs(tableName)
				}
				if log.IsContextCanceledError(err) {
					return err
				}
				if !tikverror.IsErrWriteConflict(errors.Cause(err)) {
					logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
				}
				if err = errLimiter.Wait(ctx); err != nil {
					return err
				}
			}
		},
	)
	return errors.Trace(err)
}

func (local *local) deleteDuplicateRows(
	ctx context.Context,
	logger *log.Task,
	handleRows [][2][]byte,
	decoder *kv.TableKVDecoder,
	keyInTable func(key []byte) bool,
) (err error) {
	// Starts a Delete transaction.
	txn, err := local.tikvCli.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	deleteKey := func(key []byte) error {
		logger.Debug("[resolve-dupe] will delete key", logutil.Key("key", key))
		return txn.Delete(key)
	}

	// Collect all rows & index keys into the deletion transaction.
	// (if the number of duplicates is small this should fit entirely in memory)
	// (Txn's MemBuf's bufferSizeLimit is currently infinity)
	for _, handleRow := range handleRows {
		// Skip the row key if it's not in the table.
		// This can happen if the table has been recreated or truncated,
		// and the duplicate key is from the old table.
		if !keyInTable(handleRow[0]) {
			continue
		}
		logger.Debug("[resolve-dupe] found row to resolve",
			logutil.Key("handle", handleRow[0]),
			logutil.Key("row", handleRow[1]))

		if err := deleteKey(handleRow[0]); err != nil {
			return err
		}

		handle, err := decoder.DecodeHandleFromRowKey(handleRow[0])
		if err != nil {
			return err
		}

		err = decoder.IterRawIndexKeys(handle, handleRow[1], deleteKey)
		if err != nil {
			return err
		}
	}

	logger.Debug("[resolve-dupe] number of KV pairs to be deleted", zap.Int("count", txn.Len()))
	return nil
}

func (local *local) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
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
	if err := localEngine.Cleanup(local.localStoreDir); err != nil {
		return err
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err == nil {
		localEngine.db = db
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
	localEngine.finishedRanges.reset()

	return err
}

func (local *local) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
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
	err = localEngine.Cleanup(local.localStoreDir)
	if err != nil {
		return err
	}
	localEngine.TotalSize.Store(0)
	localEngine.Length.Store(0)
	return nil
}

func (local *local) CheckRequirements(ctx context.Context, checkCtx *backend.CheckCtx) error {
	return local.targetInfoGetter.CheckRequirements(ctx, checkCtx)
}

func (local *local) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return local.targetInfoGetter.FetchRemoteTableModels(ctx, schemaName)
}

func (local *local) MakeEmptyRows() kv.Rows {
	return local.encBuilder.MakeEmptyRows()
}

func (local *local) NewEncoder(ctx context.Context, tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return local.encBuilder.NewEncoder(ctx, tbl, options)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

func (local *local) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engine := e.(*Engine)
	return openLocalWriter(cfg, engine, local.tikvCodec, local.localWriterMemCacheSize, local.bufferPool.NewBuffer())
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

func (local *local) EngineFileSizes() (res []backend.EngineFileSize) {
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		res = append(res, engine.getEngineFileSize())
		return true
	})
	return
}

var getSplitConfFromStoreFunc = getSplitConfFromStore

// return region split size, region split keys, error
func getSplitConfFromStore(ctx context.Context, host string, tls *common.TLS) (int64, int64, error) {
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
	splitSize, err := units.FromHumanSize(nested.Coprocessor.RegionSplitSize)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return splitSize, nested.Coprocessor.RegionSplitKeys, nil
}

// return region split size, region split keys, error
func getRegionSplitSizeKeys(ctx context.Context, cli pd.Client, tls *common.TLS) (int64, int64, error) {
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
