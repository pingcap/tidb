// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	tidalloc "github.com/pingcap/tidb/br/pkg/restore/prealloc_table_id"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/br/pkg/version"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/redact"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/tikv/client-go/v2/oracle"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// defaultChecksumConcurrency is the default number of the concurrent
// checksum tasks.
const defaultChecksumConcurrency = 64
const defaultDDLConcurrency = 16
const minBatchDdlSize = 1

const (
	strictPlacementPolicyMode = "STRICT"
	ignorePlacementPolicyMode = "IGNORE"

	MetaKVBatchSize = 64 * 1024 * 1024
)

// tables in this map is restored when fullClusterRestore=true
var sysPrivilegeTableMap = map[string]string{
	"user":          "(user = '%s' and host = '%%')",       // since v1.0.0
	"db":            "(user = '%s' and host = '%%')",       // since v1.0.0
	"tables_priv":   "(user = '%s' and host = '%%')",       // since v1.0.0
	"columns_priv":  "(user = '%s' and host = '%%')",       // since v1.0.0
	"default_roles": "(user = '%s' and host = '%%')",       // since v3.0.0
	"role_edges":    "(to_user = '%s' and to_host = '%%')", // since v3.0.0
	"global_priv":   "(user = '%s' and host = '%%')",       // since v3.0.8
	"global_grants": "(user = '%s' and host = '%%')",       // since v5.0.3
}

// Client sends requests to restore files.
type Client struct {
	pdClient      pd.Client
	pdHTTPClient  pdhttp.Client
	toolClient    split.SplitClient
	fileImporter  FileImporter
	rawKVClient   *RawKVBatchClient
	workerPool    *tidbutil.WorkerPool
	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters

	concurrencyPerStore uint
	databases           map[string]*metautil.Database
	ddlJobs             []*model.Job

	// store tables need to rebase info like auto id and random id and so on after create table
	rebasedTablesMap map[UniqueTableName]bool

	backupMeta *backuppb.BackupMeta
	// TODO Remove this field or replace it with a []*DB,
	// since https://github.com/pingcap/br/pull/377 needs more DBs to speed up DDL execution.
	// And for now, we must inject a pool of DBs to `Client.GoCreateTables`, otherwise there would be a race condition.
	// This is dirty: why we need DBs from different sources?
	// By replace it with a []*DB, we can remove the dirty parameter of `Client.GoCreateTable`,
	// along with them in some private functions.
	// Before you do it, you can firstly read discussions at
	// https://github.com/pingcap/br/pull/377#discussion_r446594501,
	// this probably isn't as easy as it seems like (however, not hard, too :D)
	db *DB

	// use db pool to speed up restoration in BR binary mode.
	dbPool          []*DB
	rateLimit       uint64
	isOnline        bool
	granularity     string
	noSchema        bool
	hasSpeedLimited bool

	restoreStores []uint64
	storeCount    int

	cipher             *backuppb.CipherInfo
	switchModeInterval time.Duration
	switchCh           chan struct{}

	// statHandler and dom are used for analyze table after restore.
	// it will backup stats with #dump.DumpStatsToJSON
	// and restore stats with #dump.LoadStatsFromJSONNoUpdate
	statsHandler *handle.Handle
	dom          *domain.Domain

	batchDdlSize uint

	// correspond to --tidb-placement-mode config.
	// STRICT(default) means policy related SQL can be executed in tidb.
	// IGNORE means policy related SQL will be ignored.
	policyMode string

	// policy name -> policy info
	policyMap *sync.Map

	supportPolicy bool

	// currentTS is used for rewrite meta kv when restore stream.
	// Can not use `restoreTS` directly, because schema created in `full backup` maybe is new than `restoreTS`.
	currentTS uint64

	// clusterID is the cluster id from down-stream cluster.
	clusterID uint64

	*logFileManager

	// storage for log restore
	storage storage.ExternalStorage

	// if fullClusterRestore = true:
	// - if there's system tables in the backup(backup data since br 5.1.0), the cluster should be a fresh cluster
	//	without user database or table. and system tables about privileges is restored together with user data.
	// - if there no system tables in the backup(backup data from br < 5.1.0), restore all user data just like
	//	previous version did.
	// if fullClusterRestore = false, restore all user data just like previous version did.
	// fullClusterRestore = true when there is no explicit filter setting, and it's full restore or point command
	// 	with a full backup data.
	// todo: maybe change to an enum
	// this feature is controlled by flag with-sys-table
	fullClusterRestore bool
	// the query to insert rows into table `gc_delete_range`, lack of ts.
	deleteRangeQuery          []*stream.PreDelRangeQuery
	deleteRangeQueryCh        chan *stream.PreDelRangeQuery
	deleteRangeQueryWaitGroup sync.WaitGroup

	// see RestoreCommonConfig.WithSysTable
	withSysTable bool

	// the successfully preallocated table IDs.
	preallocedTableIDs *tidalloc.PreallocIDs

	// the rewrite mode of the downloaded SST files in TiKV.
	rewriteMode RewriteMode

	// checkpoint information for snapshot restore
	checkpointRunner   *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
	checkpointChecksum map[int64]*checkpoint.ChecksumItem

	// checkpoint information for log restore
	useCheckpoint bool
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	pdClient pd.Client,
	pdHTTPCli pdhttp.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
	isRawKv bool,
) *Client {
	return &Client{
		pdClient:           pdClient,
		pdHTTPClient:       pdHTTPCli,
		toolClient:         split.NewSplitClient(pdClient, pdHTTPCli, tlsConf, isRawKv, maxSplitKeysOnce),
		tlsConf:            tlsConf,
		keepaliveConf:      keepaliveConf,
		switchCh:           make(chan struct{}),
		deleteRangeQuery:   make([]*stream.PreDelRangeQuery, 0),
		deleteRangeQueryCh: make(chan *stream.PreDelRangeQuery, 10),
	}
}

// Init create db connection and domain for storage.
func (rc *Client) Init(g glue.Glue, store kv.Storage) error {
	// setDB must happen after set PolicyMode.
	// we will use policyMode to set session variables.
	var err error
	rc.db, rc.supportPolicy, err = NewDB(g, store, rc.policyMode)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dom, err = g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}
	// tikv.Glue will return nil, tidb.Glue will return available domain
	if rc.dom != nil {
		rc.statsHandler = rc.dom.StatsHandle()
	}
	// init backupMeta only for passing unit test
	if rc.backupMeta == nil {
		rc.backupMeta = new(backuppb.BackupMeta)
	}

	// There are different ways to create session between in binary and in SQL.
	//
	// Maybe allow user modify the DDL concurrency isn't necessary,
	// because executing DDL is really I/O bound (or, algorithm bound?),
	// and we cost most of time at waiting DDL jobs be enqueued.
	// So these jobs won't be faster or slower when machine become faster or slower,
	// hence make it a fixed value would be fine.
	rc.dbPool, err = makeDBPool(defaultDDLConcurrency, func() (*DB, error) {
		db, _, err := NewDB(g, store, rc.policyMode)
		return db, err
	})
	if err != nil {
		log.Warn("create session pool failed, we will send DDLs only by created sessions",
			zap.Error(err),
			zap.Int("sessionCount", len(rc.dbPool)),
		)
	}
	return errors.Trace(err)
}

// InitCheckpoint initialize the checkpoint status for the cluster. If the cluster is
// restored for the first time, it will initialize the checkpoint metadata. Otherwrise,
// it will load checkpoint metadata and checkpoint ranges/checksum from the external
// storage.
func (rc *Client) InitCheckpoint(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
	config *pdutil.ClusterConfig,
	checkpointFirstRun bool,
) (map[int64]map[string]struct{}, *pdutil.ClusterConfig, error) {
	var (
		// checkpoint sets distinguished by range key
		checkpointSetWithTableID = make(map[int64]map[string]struct{})

		checkpointClusterConfig *pdutil.ClusterConfig

		err error
	)

	if !checkpointFirstRun {
		// load the checkpoint since this is not the first time to restore
		meta, err := checkpoint.LoadCheckpointMetadataForRestore(ctx, s, taskName)
		if err != nil {
			return checkpointSetWithTableID, nil, errors.Trace(err)
		}

		// The schedulers config is nil, so the restore-schedulers operation is just nil.
		// Then the undo function would use the result undo of `remove schedulers` operation,
		// instead of that in checkpoint meta.
		if meta.SchedulersConfig != nil {
			checkpointClusterConfig = meta.SchedulersConfig
		}

		// t1 is the latest time the checkpoint ranges persisted to the external storage.
		t1, err := checkpoint.WalkCheckpointFileForRestore(ctx, s, rc.cipher, taskName, func(tableID int64, rangeKey checkpoint.RestoreValueType) {
			checkpointSet, exists := checkpointSetWithTableID[tableID]
			if !exists {
				checkpointSet = make(map[string]struct{})
				checkpointSetWithTableID[tableID] = checkpointSet
			}
			checkpointSet[rangeKey.RangeKey] = struct{}{}
		})
		if err != nil {
			return checkpointSetWithTableID, nil, errors.Trace(err)
		}
		// t2 is the latest time the checkpoint checksum persisted to the external storage.
		checkpointChecksum, t2, err := checkpoint.LoadCheckpointChecksumForRestore(ctx, s, taskName)
		if err != nil {
			return checkpointSetWithTableID, nil, errors.Trace(err)
		}
		rc.checkpointChecksum = checkpointChecksum
		// use the later time to adjust the summary elapsed time.
		if t1 > t2 {
			summary.AdjustStartTimeToEarlierTime(t1)
		} else {
			summary.AdjustStartTimeToEarlierTime(t2)
		}
	} else {
		// initialize the checkpoint metadata since it is the first time to restore.
		meta := &checkpoint.CheckpointMetadataForRestore{}
		// a nil config means undo function
		if config != nil {
			meta.SchedulersConfig = &pdutil.ClusterConfig{Schedulers: config.Schedulers, ScheduleCfg: config.ScheduleCfg}
		}
		if err = checkpoint.SaveCheckpointMetadataForRestore(ctx, s, meta, taskName); err != nil {
			return checkpointSetWithTableID, nil, errors.Trace(err)
		}
	}

	rc.checkpointRunner, err = checkpoint.StartCheckpointRunnerForRestore(ctx, s, rc.cipher, taskName)
	return checkpointSetWithTableID, checkpointClusterConfig, errors.Trace(err)
}

func (rc *Client) WaitForFinishCheckpoint(ctx context.Context, flush bool) {
	if rc.checkpointRunner != nil {
		rc.checkpointRunner.WaitForFinish(ctx, flush)
	}
}

func (rc *Client) GetCheckpointRunner() *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType] {
	return rc.checkpointRunner
}

func (rc *Client) StartCheckpointRunnerForLogRestore(ctx context.Context, taskName string) (*checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType], error) {
	runner, err := checkpoint.StartCheckpointRunnerForLogRestore(ctx, rc.storage, rc.cipher, taskName)
	return runner, errors.Trace(err)
}

func (rc *Client) InitCheckpointMetadataForLogRestore(ctx context.Context, taskName string, gcRatio string) (string, error) {
	rc.useCheckpoint = true

	// it shows that the user has modified gc-ratio, if `gcRatio` doesn't equal to "1.1".
	// update the `gcRatio` for checkpoint metadata.
	if gcRatio == utils.DefaultGcRatioVal {
		// if the checkpoint metadata exists in the external storage, the restore is not
		// for the first time.
		exists, err := checkpoint.ExistsRestoreCheckpoint(ctx, rc.storage, taskName)
		if err != nil {
			return "", errors.Trace(err)
		}

		if exists {
			// load the checkpoint since this is not the first time to restore
			meta, err := checkpoint.LoadCheckpointMetadataForRestore(ctx, rc.storage, taskName)
			if err != nil {
				return "", errors.Trace(err)
			}

			log.Info("reuse gc ratio from checkpoint metadata", zap.String("gc-ratio", gcRatio))
			return meta.GcRatio, nil
		}
	}

	// initialize the checkpoint metadata since it is the first time to restore.
	log.Info("save gc ratio into checkpoint metadata", zap.String("gc-ratio", gcRatio))
	if err := checkpoint.SaveCheckpointMetadataForRestore(ctx, rc.storage, &checkpoint.CheckpointMetadataForRestore{
		GcRatio: gcRatio,
	}, taskName); err != nil {
		return gcRatio, errors.Trace(err)
	}

	return gcRatio, nil
}

// AllocTableIDs would pre-allocate the table's origin ID if exists, so that the TiKV doesn't need to rewrite the key in
// the download stage.
func (rc *Client) AllocTableIDs(ctx context.Context, tables []*metautil.Table) error {
	rc.preallocedTableIDs = tidalloc.New(tables)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(ctx, rc.GetDomain().Store(), true, func(_ context.Context, txn kv.Transaction) error {
		return rc.preallocedTableIDs.Alloc(meta.NewMeta(txn))
	})
	if err != nil {
		return err
	}

	log.Info("registering the table IDs", zap.Stringer("ids", rc.preallocedTableIDs))
	for i := range rc.dbPool {
		rc.dbPool[i].registerPreallocatedIDs(rc.preallocedTableIDs)
	}
	if rc.db != nil {
		rc.db.registerPreallocatedIDs(rc.preallocedTableIDs)
	}
	return nil
}

// SetPlacementPolicyMode to policy mode.
func (rc *Client) SetPlacementPolicyMode(withPlacementPolicy string) {
	switch strings.ToUpper(withPlacementPolicy) {
	case strictPlacementPolicyMode:
		rc.policyMode = strictPlacementPolicyMode
	case ignorePlacementPolicyMode:
		rc.policyMode = ignorePlacementPolicyMode
	default:
		rc.policyMode = strictPlacementPolicyMode
	}
	log.Info("set placement policy mode", zap.String("mode", rc.policyMode))
}

// SetRateLimit to set rateLimit.
func (rc *Client) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

func (rc *Client) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

// SetPolicyMap set policyMap.
func (rc *Client) SetPolicyMap(p *sync.Map) {
	rc.policyMap = p
}

// GetClusterID gets the cluster id from down-stream cluster.
func (rc *Client) GetClusterID(ctx context.Context) uint64 {
	if rc.clusterID <= 0 {
		rc.clusterID = rc.GetPDClient().GetClusterID(ctx)
	}
	return rc.clusterID
}

// GetPolicyMap set policyMap.
func (rc *Client) GetPolicyMap() *sync.Map {
	return rc.policyMap
}

// GetSupportPolicy tells whether target tidb support placement policy.
func (rc *Client) GetSupportPolicy() bool {
	return rc.supportPolicy
}

func (rc *Client) GetDomain() *domain.Domain {
	return rc.dom
}

func (rc *Client) GetStoreCount() int {
	return rc.storeCount
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// IsOnline tells if it's a online restore.
func (rc *Client) IsOnline() bool {
	return rc.isOnline
}

// SetSwitchModeInterval set switch mode interval for client.
func (rc *Client) SetSwitchModeInterval(interval time.Duration) {
	rc.switchModeInterval = interval
}

func (rc *Client) SetBatchDdlSize(batchDdlsize uint) {
	rc.batchDdlSize = batchDdlsize
}

func (rc *Client) GetBatchDdlSize() uint {
	return rc.batchDdlSize
}

func (rc *Client) SetRewriteMode(mode RewriteMode) {
	rc.rewriteMode = mode
}

func (rc *Client) GetRewriteMode() RewriteMode {
	return rc.rewriteMode
}

func (rc *Client) closeConn() {
	// rc.db can be nil in raw kv mode.
	if rc.db != nil {
		rc.db.Close()
	}
	for _, db := range rc.dbPool {
		db.Close()
	}
}

// Close a client.
func (rc *Client) Close() {
	// close the connection, and it must be succeed when in SQL mode.
	rc.closeConn()

	if rc.rawKVClient != nil {
		rc.rawKVClient.Close()
	}

	if err := rc.fileImporter.Close(); err != nil {
		log.Warn("failed to close file improter")
	}

	log.Info("Restore client closed")
}

func (rc *Client) SetCurrentTS(ts uint64) {
	rc.currentTS = ts
}

func (rc *Client) SetStorage(ctx context.Context, backend *backuppb.StorageBackend, opts *storage.ExternalStorageOptions) error {
	var err error
	rc.storage, err = storage.New(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *Client) InitClients(ctx context.Context, backend *backuppb.StorageBackend, isRawKvMode bool, isTxnKvMode bool) {
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, rc.pdClient, util.SkipTiFlash)
	if err != nil {
		log.Fatal("failed to get stores", zap.Error(err))
	}
	concurrencyPerStore := rc.GetConcurrencyPerStore()
	useTokenBucket := false
	if rc.granularity == string(CoarseGrained) {
		// coarse-grained make split & scatter pipeline fast enough
		// so we can use a new token bucket way to speed up download.
		// ToDo remove it when token bucket is stable enough.
		log.Info("use token bucket to control download and ingest flow")
		useTokenBucket = true
	}

	metaClient := split.NewSplitClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, isRawKvMode, maxSplitKeysOnce)
	importCli := NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)
	rc.fileImporter = NewFileImporter(metaClient, importCli, backend, isRawKvMode, isTxnKvMode, stores, rc.rewriteMode, concurrencyPerStore, useTokenBucket)
}

func (rc *Client) SetRawKVClient(c *RawKVBatchClient) {
	rc.rawKVClient = c
}

func (rc *Client) needLoadSchemas(backupMeta *backuppb.BackupMeta) bool {
	return !(backupMeta.IsRawKv || backupMeta.IsTxnKv)
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient.
func (rc *Client) InitBackupMeta(
	c context.Context,
	backupMeta *backuppb.BackupMeta,
	backend *backuppb.StorageBackend,
	reader *metautil.MetaReader,
	loadStats bool) error {
	if rc.needLoadSchemas(backupMeta) {
		databases, err := metautil.LoadBackupTables(c, reader, loadStats)
		if err != nil {
			return errors.Trace(err)
		}
		rc.databases = databases

		var ddlJobs []*model.Job
		// ddls is the bytes of json.Marshal
		ddls, err := reader.ReadDDLs(c)
		if err != nil {
			return errors.Trace(err)
		}
		if len(ddls) != 0 {
			err = json.Unmarshal(ddls, &ddlJobs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		rc.ddlJobs = ddlJobs
	}
	rc.backupMeta = backupMeta

	rc.InitClients(c, backend, backupMeta.IsRawKv, backupMeta.IsTxnKv)
	log.Info("load backupmeta", zap.Int("databases", len(rc.databases)), zap.Int("jobs", len(rc.ddlJobs)))
	return rc.fileImporter.CheckMultiIngestSupport(c, rc.pdClient)
}

// IsRawKvMode checks whether the backup data is in raw kv format, in which case transactional recover is forbidden.
func (rc *Client) IsRawKvMode() bool {
	return rc.backupMeta.IsRawKv
}

// GetFilesInRawRange gets all files that are in the given range or intersects with the given range.
func (rc *Client) GetFilesInRawRange(startKey []byte, endKey []byte, cf string) ([]*backuppb.File, error) {
	if !rc.IsRawKvMode() {
		return nil, errors.Annotate(berrors.ErrRestoreModeMismatch, "the backup data is not in raw kv mode")
	}

	for _, rawRange := range rc.backupMeta.RawRanges {
		// First check whether the given range is backup-ed. If not, we cannot perform the restore.
		if rawRange.Cf != cf {
			continue
		}

		if (len(rawRange.EndKey) > 0 && bytes.Compare(startKey, rawRange.EndKey) >= 0) ||
			(len(endKey) > 0 && bytes.Compare(rawRange.StartKey, endKey) >= 0) {
			// The restoring range is totally out of the current range. Skip it.
			continue
		}

		if bytes.Compare(startKey, rawRange.StartKey) < 0 ||
			utils.CompareEndKey(endKey, rawRange.EndKey) > 0 {
			// Only partial of the restoring range is in the current backup-ed range. So the given range can't be fully
			// restored.
			return nil, errors.Annotatef(berrors.ErrRestoreRangeMismatch,
				"the given range to restore [%s, %s) is not fully covered by the range that was backed up [%s, %s)",
				redact.Key(startKey), redact.Key(endKey), redact.Key(rawRange.StartKey), redact.Key(rawRange.EndKey),
			)
		}

		// We have found the range that contains the given range. Find all necessary files.
		files := make([]*backuppb.File, 0)

		for _, file := range rc.backupMeta.Files {
			if file.Cf != cf {
				continue
			}

			if len(file.EndKey) > 0 && bytes.Compare(file.EndKey, startKey) < 0 {
				// The file is before the range to be restored.
				continue
			}
			if len(endKey) > 0 && bytes.Compare(endKey, file.StartKey) <= 0 {
				// The file is after the range to be restored.
				// The specified endKey is exclusive, so when it equals to a file's startKey, the file is still skipped.
				continue
			}

			files = append(files, file)
		}

		// There should be at most one backed up range that covers the restoring range.
		return files, nil
	}

	return nil, errors.Annotate(berrors.ErrRestoreRangeMismatch, "no backup data in the range")
}

// SetConcurrency sets the concurrency of dbs tables files.
func (rc *Client) SetConcurrency(c uint) {
	log.Info("download worker pool", zap.Uint("size", c))
	if rc.granularity == string(CoarseGrained) {
		// we believe 32 is large enough for download worker pool.
		// it won't reach the limit if sst files distribute evenly.
		// when restore memory usage is still too high, we should reduce concurrencyPerStore
		// to sarifice some speed to reduce memory usage.
		count := uint(rc.storeCount) * rc.concurrencyPerStore * 32
		log.Info("download coarse worker pool", zap.Uint("size", count))
		rc.workerPool = tidbutil.NewWorkerPool(count, "file")
		return
	}
	rc.workerPool = tidbutil.NewWorkerPool(c, "file")
}

// SetConcurrencyPerStore sets the concurrency of download files for each store.
func (rc *Client) SetConcurrencyPerStore(c uint) {
	log.Info("per-store download worker pool", zap.Uint("size", c))
	rc.concurrencyPerStore = c
}

func (rc *Client) GetConcurrencyPerStore() uint {
	return rc.concurrencyPerStore
}

// EnableOnline sets the mode of restore to online.
func (rc *Client) EnableOnline() {
	rc.isOnline = true
}

// SetGranularity sets the ganularity of restore pipeline.
func (rc *Client) SetGranularity(g string) {
	rc.granularity = g
}

// GetGranularity sets the ganularity of restore pipeline.
func (rc *Client) GetGranularity() string {
	return rc.granularity
}

// GetTLSConfig returns the tls config.
func (rc *Client) GetTLSConfig() *tls.Config {
	return rc.tlsConf
}

// GetTS gets a new timestamp from PD.
func (rc *Client) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := rc.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// GetTSWithRetry gets a new timestamp with retry from PD.
func (rc *Client) GetTSWithRetry(ctx context.Context) (uint64, error) {
	var (
		startTS  uint64
		getTSErr error
		retry    uint
	)

	err := utils.WithRetry(ctx, func() error {
		startTS, getTSErr = rc.GetTS(ctx)
		failpoint.Inject("get-ts-error", func(val failpoint.Value) {
			if val.(bool) && retry < 3 {
				getTSErr = errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
			}
		})

		retry++
		if getTSErr != nil {
			log.Warn("failed to get TS, retry it", zap.Uint("retry time", retry), logutil.ShortError(getTSErr))
		}
		return getTSErr
	}, utils.NewPDReqBackoffer())

	if err != nil {
		log.Error("failed to get TS", zap.Error(err))
	}
	return startTS, errors.Trace(err)
}

// ResetTS resets the timestamp of PD to a bigger value.
func (rc *Client) ResetTS(ctx context.Context, pdCtrl *pdutil.PdController) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	return utils.WithRetry(ctx, func() error {
		return pdCtrl.ResetTS(ctx, restoreTS)
	}, utils.NewPDReqBackoffer())
}

// GetPlacementRules return the current placement rules.
func (rc *Client) GetPlacementRules(ctx context.Context, pdAddrs []string) ([]pdtypes.Rule, error) {
	var placementRules []pdtypes.Rule
	i := 0
	errRetry := utils.WithRetry(ctx, func() error {
		var err error
		idx := i % len(pdAddrs)
		i++
		placementRules, err = pdutil.GetPlacementRules(ctx, pdAddrs[idx], rc.tlsConf)
		return errors.Trace(err)
	}, utils.NewPDReqBackoffer())
	return placementRules, errors.Trace(errRetry)
}

// GetDatabases returns all databases.
func (rc *Client) GetDatabases() []*metautil.Database {
	dbs := make([]*metautil.Database, 0, len(rc.databases))
	for _, db := range rc.databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// GetDatabase returns a database by name.
func (rc *Client) GetDatabase(name string) *metautil.Database {
	return rc.databases[name]
}

// HasBackedUpSysDB whether we have backed up system tables
// br backs system tables up since 5.1.0
func (rc *Client) HasBackedUpSysDB() bool {
	sysDBs := []string{"mysql", "sys"}
	for _, db := range sysDBs {
		temporaryDB := utils.TemporaryDBName(db)
		_, backedUp := rc.databases[temporaryDB.O]
		if backedUp {
			return true
		}
	}
	return false
}

// GetPlacementPolicies returns policies.
func (rc *Client) GetPlacementPolicies() (*sync.Map, error) {
	policies := &sync.Map{}
	for _, p := range rc.backupMeta.Policies {
		policyInfo := &model.PolicyInfo{}
		err := json.Unmarshal(p.Info, policyInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies.Store(policyInfo.Name.L, policyInfo)
	}
	return policies, nil
}

// GetDDLJobs returns ddl jobs.
func (rc *Client) GetDDLJobs() []*model.Job {
	return rc.ddlJobs
}

// GetTableSchema returns the schema of a table from TiDB.
func (rc *Client) GetTableSchema(
	dom *domain.Domain,
	dbName model.CIStr,
	tableName model.CIStr,
) (*model.TableInfo, error) {
	info := dom.InfoSchema()
	table, err := info.TableByName(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

// CreatePolicies creates all policies in full restore.
func (rc *Client) CreatePolicies(ctx context.Context, policyMap *sync.Map) error {
	var err error
	policyMap.Range(func(key, value any) bool {
		e := rc.db.CreatePlacementPolicy(ctx, value.(*model.PolicyInfo))
		if e != nil {
			err = e
			return false
		}
		return true
	})
	return err
}

// GetDBSchema gets the schema of a db from TiDB cluster
func (rc *Client) GetDBSchema(dom *domain.Domain, dbName model.CIStr) (*model.DBInfo, bool) {
	info := dom.InfoSchema()
	return info.SchemaByName(dbName)
}

// CreateDatabases creates databases. If the client has the db pool, it would create it.
func (rc *Client) CreateDatabases(ctx context.Context, dbs []*metautil.Database) error {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create database")
		return nil
	}

	if len(rc.dbPool) == 0 {
		log.Info("create databases sequentially")
		for _, db := range dbs {
			err := rc.createDatabaseWithDBConn(ctx, db.Info, rc.db)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	log.Info("create databases in db pool", zap.Int("pool size", len(rc.dbPool)))
	eg, ectx := errgroup.WithContext(ctx)
	workers := tidbutil.NewWorkerPool(uint(len(rc.dbPool)), "DB DDL workers")
	for _, db_ := range dbs {
		db := db_
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			conn := rc.dbPool[id%uint64(len(rc.dbPool))]
			return rc.createDatabaseWithDBConn(ectx, db.Info, conn)
		})
	}
	return eg.Wait()
}

func (rc *Client) createDatabaseWithDBConn(ctx context.Context, db *model.DBInfo, conn *DB) error {
	log.Info("create database", zap.Stringer("name", db.Name))

	if !rc.supportPolicy {
		log.Info("set placementPolicyRef to nil when target tidb not support policy",
			zap.Stringer("database", db.Name))
		db.PlacementPolicyRef = nil
	}

	if db.PlacementPolicyRef != nil {
		if err := conn.ensurePlacementPolicy(ctx, db.PlacementPolicyRef.Name, rc.policyMap); err != nil {
			return errors.Trace(err)
		}
	}

	return conn.CreateDatabase(ctx, db)
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *Client) CreateTables(
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
) (*RewriteRules, []*model.TableInfo, error) {
	rewriteRules := &RewriteRules{
		Data: make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	errCh := make(chan error, 1)
	tbMapping := map[string]int{}
	for i, t := range tables {
		tbMapping[t.Info.Name.String()] = i
	}
	dataCh := rc.GoCreateTables(context.TODO(), dom, tables, newTS, errCh)
	for et := range dataCh {
		rules := et.RewriteRule
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, et.Table)
	}
	// Let's ensure that it won't break the original order.
	slices.SortFunc(newTables, func(i, j *model.TableInfo) int {
		return cmp.Compare(tbMapping[i.Name.String()], tbMapping[j.Name.String()])
	})

	select {
	case err, ok := <-errCh:
		if ok {
			return nil, nil, errors.Trace(err)
		}
	default:
	}
	return rewriteRules, newTables, nil
}
func (rc *Client) createTables(
	ctx context.Context,
	db *DB,
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
) ([]CreatedTable, error) {
	log.Info("client to create tables")
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID")
	} else {
		err := db.CreateTables(ctx, tables, rc.GetRebasedTables(), rc.GetSupportPolicy(), rc.GetPolicyMap())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	cts := make([]CreatedTable, 0, len(tables))
	for _, table := range tables {
		newTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
			return nil, errors.Annotatef(berrors.ErrRestoreModeMismatch,
				"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
				transferBoolToValue(table.Info.IsCommonHandle),
				table.Info.IsCommonHandle,
				newTableInfo.IsCommonHandle)
		}
		rules := GetRewriteRules(newTableInfo, table.Info, newTS, true)
		ct := CreatedTable{
			RewriteRule: rules,
			Table:       newTableInfo,
			OldTable:    table,
		}
		log.Debug("new created tables", zap.Any("table", ct))
		cts = append(cts, ct)
	}
	return cts, nil
}

func (rc *Client) createTable(
	ctx context.Context,
	db *DB,
	dom *domain.Domain,
	table *metautil.Table,
	newTS uint64,
) (CreatedTable, error) {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID", zap.Stringer("table", table.Info.Name))
	} else {
		err := db.CreateTable(ctx, table, rc.GetRebasedTables(), rc.GetSupportPolicy(), rc.GetPolicyMap())
		if err != nil {
			return CreatedTable{}, errors.Trace(err)
		}
	}
	newTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
	if err != nil {
		return CreatedTable{}, errors.Trace(err)
	}
	if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
		return CreatedTable{}, errors.Annotatef(berrors.ErrRestoreModeMismatch,
			"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
			transferBoolToValue(table.Info.IsCommonHandle),
			table.Info.IsCommonHandle,
			newTableInfo.IsCommonHandle)
	}
	rules := GetRewriteRules(newTableInfo, table.Info, newTS, true)
	et := CreatedTable{
		RewriteRule: rules,
		Table:       newTableInfo,
		OldTable:    table,
	}
	return et, nil
}

// GoCreateTables create tables, and generate their information.
// this function will use workers as the same number of sessionPool,
// leave sessionPool nil to send DDLs sequential.
func (rc *Client) GoCreateTables(
	ctx context.Context,
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
	errCh chan<- error,
) <-chan CreatedTable {
	// Could we have a smaller size of tables?
	log.Info("start create tables")

	rc.GenerateRebasedTables(tables)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.GoCreateTables", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	outCh := make(chan CreatedTable, len(tables))
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)

	var err error

	if rc.batchDdlSize > minBatchDdlSize && len(rc.dbPool) > 0 {
		err = rc.createTablesInWorkerPool(ctx, dom, tables, newTS, outCh)
		if err == nil {
			defer log.Debug("all tables are created")
			close(outCh)
			return outCh
		} else if !utils.FallBack2CreateTable(err) {
			errCh <- err
			close(outCh)
			return outCh
		}
		// fall back to old create table (sequential create table)
		log.Info("fall back to the sequential create table")
	}

	createOneTable := func(c context.Context, db *DB, t *metautil.Table) error {
		select {
		case <-c.Done():
			return c.Err()
		default:
		}
		rt, err := rc.createTable(c, db, dom, t, newTS)
		if err != nil {
			log.Error("create table failed",
				zap.Error(err),
				zap.Stringer("db", t.DB.Name),
				zap.Stringer("table", t.Info.Name))
			return errors.Trace(err)
		}
		log.Debug("table created and send to next",
			zap.Int("output chan size", len(outCh)),
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		outCh <- rt
		rater.Inc()
		rater.L().Info("table created",
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		return nil
	}
	go func() {
		defer close(outCh)
		defer log.Debug("all tables are created")
		var err error
		if len(rc.dbPool) > 0 {
			err = rc.createTablesWithDBPool(ctx, createOneTable, tables)
		} else {
			err = rc.createTablesWithSoleDB(ctx, createOneTable, tables)
		}
		if err != nil {
			errCh <- err
		}
	}()

	return outCh
}

func (rc *Client) createTablesWithSoleDB(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *metautil.Table) error,
	tables []*metautil.Table) error {
	for _, t := range tables {
		if err := createOneTable(ctx, rc.db, t); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (rc *Client) createTablesWithDBPool(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *metautil.Table) error,
	tables []*metautil.Table) error {
	eg, ectx := errgroup.WithContext(ctx)
	workers := tidbutil.NewWorkerPool(uint(len(rc.dbPool)), "DDL workers")
	for _, t := range tables {
		table := t
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			db := rc.dbPool[id%uint64(len(rc.dbPool))]
			return createOneTable(ectx, db, table)
		})
	}
	return eg.Wait()
}

func (rc *Client) createTablesInWorkerPool(ctx context.Context, dom *domain.Domain, tables []*metautil.Table, newTS uint64, outCh chan<- CreatedTable) error {
	eg, ectx := errgroup.WithContext(ctx)
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)
	workers := tidbutil.NewWorkerPool(uint(len(rc.dbPool)), "Create Tables Worker")
	numOfTables := len(tables)

	for lastSent := 0; lastSent < numOfTables; lastSent += int(rc.batchDdlSize) {
		end := min(lastSent+int(rc.batchDdlSize), len(tables))
		log.Info("create tables", zap.Int("table start", lastSent), zap.Int("table end", end))

		tableSlice := tables[lastSent:end]
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			db := rc.dbPool[id%uint64(len(rc.dbPool))]
			cts, err := rc.createTables(ectx, db, dom, tableSlice, newTS) // ddl job for [lastSent:i)
			failpoint.Inject("restore-createtables-error", func(val failpoint.Value) {
				if val.(bool) {
					err = errors.New("sample error without extra message")
				}
			})
			if err != nil {
				log.Error("create tables fail", zap.Error(err))
				return err
			}
			for _, ct := range cts {
				log.Debug("table created and send to next",
					zap.Int("output chan size", len(outCh)),
					zap.Stringer("table", ct.OldTable.Info.Name),
					zap.Stringer("database", ct.OldTable.DB.Name))
				outCh <- ct
				rater.Inc()
				rater.L().Info("table created",
					zap.Stringer("table", ct.OldTable.Info.Name),
					zap.Stringer("database", ct.OldTable.DB.Name))
			}
			return err
		})
	}
	return eg.Wait()
}

// NeedCheckFreshCluster is every time. except restore from a checkpoint or user has not set filter argument.
func (rc *Client) NeedCheckFreshCluster(ExplicitFilter bool, firstRun bool) bool {
	return rc.IsFull() && !ExplicitFilter && firstRun
}

// CheckTargetClusterFresh check whether the target cluster is fresh or not
// if there's no user dbs or tables, we take it as a fresh cluster, although
// user may have created some users or made other changes.
func (rc *Client) CheckTargetClusterFresh(ctx context.Context) error {
	log.Info("checking whether target cluster is fresh")
	userDBs := GetExistedUserDBs(rc.dom)
	if len(userDBs) == 0 {
		return nil
	}

	const maxPrintCount = 10
	userTableOrDBNames := make([]string, 0, maxPrintCount+1)
	addName := func(name string) bool {
		if len(userTableOrDBNames) == maxPrintCount {
			userTableOrDBNames = append(userTableOrDBNames, "...")
			return false
		}
		userTableOrDBNames = append(userTableOrDBNames, name)
		return true
	}
outer:
	for _, db := range userDBs {
		if !addName(db.Name.L) {
			break outer
		}
		for _, tbl := range db.Tables {
			if !addName(tbl.Name.L) {
				break outer
			}
		}
	}
	log.Error("not fresh cluster", zap.Strings("user tables", userTableOrDBNames))
	return errors.Annotate(berrors.ErrRestoreNotFreshCluster, "user db/tables: "+strings.Join(userTableOrDBNames, ", "))
}

func (rc *Client) CheckSysTableCompatibility(dom *domain.Domain, tables []*metautil.Table) error {
	log.Info("checking target cluster system table compatibility with backed up data")
	privilegeTablesInBackup := make([]*metautil.Table, 0)
	for _, table := range tables {
		decodedSysDBName, ok := utils.GetSysDBCIStrName(table.DB.Name)
		if ok && decodedSysDBName.L == mysql.SystemDB && sysPrivilegeTableMap[table.Info.Name.L] != "" {
			privilegeTablesInBackup = append(privilegeTablesInBackup, table)
		}
	}
	sysDB := model.NewCIStr(mysql.SystemDB)
	for _, table := range privilegeTablesInBackup {
		ti, err := rc.GetTableSchema(dom, sysDB, table.Info.Name)
		if err != nil {
			log.Error("missing table on target cluster", zap.Stringer("table", table.Info.Name))
			return errors.Annotate(berrors.ErrRestoreIncompatibleSys, "missed system table: "+table.Info.Name.O)
		}
		backupTi := table.Info
		// skip checking the number of columns in mysql.user table,
		// because higher versions of TiDB may add new columns.
		if len(ti.Columns) != len(backupTi.Columns) && backupTi.Name.L != sysUserTableName {
			log.Error("column count mismatch",
				zap.Stringer("table", table.Info.Name),
				zap.Int("col in cluster", len(ti.Columns)),
				zap.Int("col in backup", len(backupTi.Columns)))
			return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
				"column count mismatch, table: %s, col in cluster: %d, col in backup: %d",
				table.Info.Name.O, len(ti.Columns), len(backupTi.Columns))
		}
		backupColMap := make(map[string]*model.ColumnInfo)
		for i := range backupTi.Columns {
			col := backupTi.Columns[i]
			backupColMap[col.Name.L] = col
		}
		// order can be different but type must compatible
		for i := range ti.Columns {
			col := ti.Columns[i]
			backupCol := backupColMap[col.Name.L]
			if backupCol == nil {
				// skip when the backed up mysql.user table is missing columns.
				if backupTi.Name.L == sysUserTableName {
					log.Warn("missing column in backup data",
						zap.Stringer("table", table.Info.Name),
						zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
					continue
				}
				log.Error("missing column in backup data",
					zap.Stringer("table", table.Info.Name),
					zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"missing column in backup data, table: %s, col: %s %s",
					table.Info.Name.O,
					col.Name, col.FieldType.String())
			}
			if !utils.IsTypeCompatible(backupCol.FieldType, col.FieldType) {
				log.Error("incompatible column",
					zap.Stringer("table", table.Info.Name),
					zap.String("col in cluster", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())),
					zap.String("col in backup", fmt.Sprintf("%s %s", backupCol.Name, backupCol.FieldType.String())))
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column, table: %s, col in cluster: %s %s, col in backup: %s %s",
					table.Info.Name.O,
					col.Name, col.FieldType.String(),
					backupCol.Name, backupCol.FieldType.String())
			}
		}

		if backupTi.Name.L == sysUserTableName {
			// check whether the columns of table in cluster are less than the backup data
			clusterColMap := make(map[string]*model.ColumnInfo)
			for i := range ti.Columns {
				col := ti.Columns[i]
				clusterColMap[col.Name.L] = col
			}
			// order can be different
			for i := range backupTi.Columns {
				col := backupTi.Columns[i]
				clusterCol := clusterColMap[col.Name.L]
				if clusterCol == nil {
					log.Error("missing column in cluster data",
						zap.Stringer("table", table.Info.Name),
						zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
					return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
						"missing column in cluster data, table: %s, col: %s %s",
						table.Info.Name.O,
						col.Name, col.FieldType.String())
				}
			}
		}
	}
	return nil
}

// ExecDDLs executes the queries of the ddl jobs.
func (rc *Client) ExecDDLs(ctx context.Context, ddlJobs []*model.Job) error {
	// Sort the ddl jobs by schema version in ascending order.
	slices.SortFunc(ddlJobs, func(i, j *model.Job) int {
		return cmp.Compare(i.BinlogInfo.SchemaVersion, j.BinlogInfo.SchemaVersion)
	})

	for _, job := range ddlJobs {
		err := rc.db.ExecDDL(ctx, job)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("execute ddl query",
			zap.String("db", job.SchemaName),
			zap.String("query", job.Query),
			zap.Int64("historySchemaVersion", job.BinlogInfo.SchemaVersion))
	}
	return nil
}

// Mock the call of setSpeedLimit function
func MockCallSetSpeedLimit(ctx context.Context, fakeImportClient ImporterClient, rc *Client, concurrency uint) error {
	rc.SetRateLimit(42)
	rc.SetConcurrency(concurrency)
	rc.hasSpeedLimited = false
	rc.fileImporter = NewFileImporter(nil, fakeImportClient, nil, false, false, nil, rc.rewriteMode, 128, false)
	return rc.setSpeedLimit(ctx, rc.rateLimit)
}

func (rc *Client) ResetSpeedLimit(ctx context.Context) error {
	rc.hasSpeedLimited = false
	err := rc.setSpeedLimit(ctx, 0)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *Client) setSpeedLimit(ctx context.Context, rateLimit uint64) error {
	if !rc.hasSpeedLimited {
		stores, err := util.GetAllTiKVStores(ctx, rc.pdClient, util.SkipTiFlash)
		if err != nil {
			return errors.Trace(err)
		}

		eg, ectx := errgroup.WithContext(ctx)
		for _, store := range stores {
			if err := ectx.Err(); err != nil {
				return errors.Trace(err)
			}

			finalStore := store
			rc.workerPool.ApplyOnErrorGroup(eg,
				func() error {
					err := rc.fileImporter.setDownloadSpeedLimit(ectx, finalStore.GetId(), rateLimit)
					if err != nil {
						return errors.Trace(err)
					}
					return nil
				})
		}

		if err := eg.Wait(); err != nil {
			return errors.Trace(err)
		}
		rc.hasSpeedLimited = true
	}
	return nil
}

func getFileRangeKey(f string) string {
	// the backup date file pattern is `{store_id}_{region_id}_{epoch_version}_{key}_{ts}_{cf}.sst`
	// so we need to compare with out the `_{cf}.sst` suffix
	idx := strings.LastIndex(f, "_")
	if idx < 0 {
		panic(fmt.Sprintf("invalid backup data file name: '%s'", f))
	}

	return f[:idx]
}

// isFilesBelongToSameRange check whether two files are belong to the same range with different cf.
func isFilesBelongToSameRange(f1, f2 string) bool {
	return getFileRangeKey(f1) == getFileRangeKey(f2)
}

func drainFilesByRange(files []*backuppb.File) ([]*backuppb.File, []*backuppb.File) {
	if len(files) == 0 {
		return nil, nil
	}
	idx := 1
	for idx < len(files) {
		if !isFilesBelongToSameRange(files[idx-1].Name, files[idx].Name) {
			break
		}
		idx++
	}

	return files[:idx], files[idx:]
}

func getGroupFiles(files []*backuppb.File, supportMulti bool) [][]*backuppb.File {
	if len(files) == 0 {
		return nil
	}
	if !supportMulti {
		newFiles := make([][]*backuppb.File, 0, len(files))
		for _, file := range files {
			newFiles = append(newFiles, []*backuppb.File{file})
		}
		return newFiles
	}
	return [][]*backuppb.File{files}
}

// SplitRanges implements TiKVRestorer.
func (rc *Client) SplitRanges(ctx context.Context,
	ranges []rtree.Range,
	updateCh glue.Progress,
	isRawKv bool) error {
	return SplitRanges(ctx, rc, ranges, updateCh, isRawKv)
}

func (rc *Client) WrapLogFilesIterWithSplitHelper(logIter LogIter, rules map[int64]*RewriteRules, g glue.Glue, store kv.Storage) (LogIter, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	splitSize, splitKeys := utils.GetRegionSplitInfo(execCtx)
	log.Info("get split threshold from tikv config", zap.Uint64("split-size", splitSize), zap.Int64("split-keys", splitKeys))
	client := split.NewSplitClient(rc.GetPDClient(), rc.pdHTTPClient, rc.GetTLSConfig(), false, maxSplitKeysOnce)
	return NewLogFilesIterWithSplitHelper(logIter, rules, client, splitSize, splitKeys), nil
}

func (rc *Client) generateKvFilesSkipMap(ctx context.Context, downstreamIdset map[int64]struct{}, taskName string) (*LogFilesSkipMap, error) {
	skipMap := NewLogFilesSkipMap()
	t, err := checkpoint.WalkCheckpointFileForRestore(ctx, rc.storage, rc.cipher, taskName, func(groupKey checkpoint.LogRestoreKeyType, off checkpoint.LogRestoreValueMarshaled) {
		for tableID, foffs := range off.Foffs {
			// filter out the checkpoint data of dropped table
			if _, exists := downstreamIdset[tableID]; exists {
				for _, foff := range foffs {
					skipMap.Insert(groupKey, off.Goff, foff)
				}
			}
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	summary.AdjustStartTimeToEarlierTime(t)
	return skipMap, nil
}

func (rc *Client) WrapLogFilesIterWithCheckpoint(
	ctx context.Context,
	logIter LogIter,
	downstreamIdset map[int64]struct{},
	taskName string,
	updateStats func(kvCount, size uint64),
	onProgress func(),
) (LogIter, error) {
	skipMap, err := rc.generateKvFilesSkipMap(ctx, downstreamIdset, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter.FilterOut(logIter, func(d *LogDataFileInfo) bool {
		if skipMap.NeedSkip(d.MetaDataGroupName, d.OffsetInMetaGroup, d.OffsetInMergedGroup) {
			onProgress()
			updateStats(uint64(d.NumberOfEntries), d.Length)
			return true
		}
		return false
	}), nil
}

// RestoreSSTFiles tries to restore the files.
func (rc *Client) RestoreSSTFiles(
	ctx context.Context,
	tableIDWithFiles []TableIDWithFiles,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	fileCount := 0
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore files", zap.Duration("take", elapsed), zapTableIDWithFiles(tableIDWithFiles))
			summary.CollectSuccessUnit("files", fileCount, elapsed)
		}
	}()

	log.Debug("start to restore files", zap.Int("files", fileCount))

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreSSTFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	eg, ectx := errgroup.WithContext(ctx)
	err = rc.setSpeedLimit(ctx, rc.rateLimit)
	if err != nil {
		return errors.Trace(err)
	}

	runner := rc.GetCheckpointRunner()

	var rangeFiles []*backuppb.File
	var leftFiles []*backuppb.File
LOOPFORTABLE:
	for _, tableIDWithFile := range tableIDWithFiles {
		tableID := tableIDWithFile.TableID
		files := tableIDWithFile.Files
		rules := tableIDWithFile.RewriteRules
		fileCount += len(files)
		for rangeFiles, leftFiles = drainFilesByRange(files); len(rangeFiles) != 0; rangeFiles, leftFiles = drainFilesByRange(leftFiles) {
			filesReplica := rangeFiles
			if ectx.Err() != nil {
				log.Warn("Restoring encountered error and already stopped, give up remained files.",
					zap.Int("remained", len(leftFiles)),
					logutil.ShortError(ectx.Err()))
				// We will fetch the error from the errgroup then (If there were).
				// Also note if the parent context has been canceled or something,
				// breaking here directly is also a reasonable behavior.
				break LOOPFORTABLE
			}
			restoreFn := func() error {
				filesGroups := getGroupFiles(filesReplica, rc.fileImporter.supportMultiIngest)
				for _, filesGroup := range filesGroups {
					if importErr := func(fs []*backuppb.File) (err error) {
						fileStart := time.Now()
						defer func() {
							if err == nil {
								log.Info("import files done", logutil.Files(filesGroup),
									zap.Duration("take", time.Since(fileStart)))
								updateCh.Inc()
							}
						}()
						return rc.fileImporter.ImportSSTFiles(ectx, fs, rules, rc.cipher, rc.dom.Store().GetCodec().GetAPIVersion())
					}(filesGroup); importErr != nil {
						return errors.Trace(importErr)
					}
				}

				// the data of this range has been import done
				if runner != nil && len(filesReplica) > 0 {
					rangeKey := getFileRangeKey(filesReplica[0].Name)
					// The checkpoint range shows this ranges of kvs has been restored into
					// the table corresponding to the table-id.
					if err := checkpoint.AppendRangesForRestore(ectx, runner, tableID, rangeKey); err != nil {
						return errors.Trace(err)
					}
				}
				return nil
			}
			if rc.granularity == string(CoarseGrained) {
				rc.fileImporter.cond.L.Lock()
				for rc.fileImporter.ShouldBlock() {
					// wait for download worker notified
					rc.fileImporter.cond.Wait()
				}
				rc.fileImporter.cond.L.Unlock()
				rc.workerPool.ApplyOnErrorGroup(eg, restoreFn)
			} else {
				// if we are not use coarse granularity which means
				// we still pipeline split & scatter regions and import sst files
				// just keep the consistency as before.
				rc.workerPool.ApplyOnErrorGroup(eg, restoreFn)
			}
		}
	}

	if err := eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error(
			"restore files failed",
			zap.Error(err),
		)
		return errors.Trace(err)
	}
	// Once the parent context canceled and there is no task running in the errgroup,
	// we may break the for loop without error in the errgroup. (Will this happen?)
	// At that time, return the error in the context here.
	return ctx.Err()
}

func (rc *Client) WaitForFilesRestored(ctx context.Context, files []*backuppb.File, updateCh glue.Progress) error {
	errCh := make(chan error, len(files))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)

	for _, file := range files {
		fileReplica := file
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				defer func() {
					log.Info("import sst files done", logutil.Files(files))
					updateCh.Inc()
				}()
				return rc.fileImporter.ImportSSTFiles(ectx, []*backuppb.File{fileReplica}, EmptyRewriteRule(), rc.cipher, rc.backupMeta.ApiVersion)
			})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *Client) RestoreRaw(
	ctx context.Context, startKey []byte, endKey []byte, files []*backuppb.File, updateCh glue.Progress,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Raw",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.Duration("take", elapsed))
	}()
	err := rc.fileImporter.SetRawRange(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	err = rc.WaitForFilesRestored(ctx, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(
		"finish to restore raw range",
		logutil.Key("startKey", startKey),
		logutil.Key("endKey", endKey),
	)
	return nil
}

// SwitchToImportMode switch tikv cluster to import mode.
func (rc *Client) SwitchToImportMode(ctx context.Context) {
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(rc.switchModeInterval)
		defer tick.Stop()

		// [important!] switch tikv mode into import at the beginning
		log.Info("switch to import mode at beginning")
		err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
		if err != nil {
			log.Warn("switch to import mode failed", zap.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			case <-rc.switchCh:
				log.Info("stop automatic switch to import mode")
				return
			}
		}
	}()
}

// SwitchToNormalMode switch tikv cluster to normal mode.
func (rc *Client) SwitchToNormalMode(ctx context.Context) error {
	close(rc.switchCh)
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (rc *Client) switchTiKVMode(ctx context.Context, mode import_sstpb.SwitchMode) error {
	stores, err := util.GetAllTiKVStores(ctx, rc.pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3

	eg, ectx := errgroup.WithContext(ctx)
	for _, store := range stores {
		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}

		finalStore := store
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				opt := grpc.WithTransportCredentials(insecure.NewCredentials())
				if rc.tlsConf != nil {
					opt = grpc.WithTransportCredentials(credentials.NewTLS(rc.tlsConf))
				}
				gctx, cancel := context.WithTimeout(ectx, time.Second*5)
				connection, err := grpc.DialContext(
					gctx,
					finalStore.GetAddress(),
					opt,
					grpc.WithBlock(),
					grpc.FailOnNonTempDialError(true),
					grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
					// we don't need to set keepalive timeout here, because the connection lives
					// at most 5s. (shorter than minimal value for keepalive time!)
				)
				cancel()
				if err != nil {
					return errors.Trace(err)
				}
				client := import_sstpb.NewImportSSTClient(connection)
				_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
					Mode: mode,
				})
				if err != nil {
					return errors.Trace(err)
				}
				err = connection.Close()
				if err != nil {
					log.Error("close grpc connection failed in switch mode", zap.Error(err))
				}
				return nil
			})
	}

	if err = eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func concurrentHandleTablesCh(
	ctx context.Context,
	inCh <-chan *CreatedTable,
	outCh chan<- *CreatedTable,
	errCh chan<- error,
	workers *tidbutil.WorkerPool,
	processFun func(context.Context, *CreatedTable) error,
	deferFun func()) {
	eg, ectx := errgroup.WithContext(ctx)
	defer func() {
		if err := eg.Wait(); err != nil {
			errCh <- err
		}
		close(outCh)
		deferFun()
	}()

	for {
		select {
		// if we use ectx here, maybe canceled will mask real error.
		case <-ctx.Done():
			errCh <- ctx.Err()
		case tbl, ok := <-inCh:
			if !ok {
				return
			}
			cloneTable := tbl
			worker := workers.ApplyWorker()
			eg.Go(func() error {
				defer workers.RecycleWorker(worker)
				err := processFun(ectx, cloneTable)
				if err != nil {
					return err
				}
				outCh <- cloneTable
				return nil
			})
		}
	}
}

// GoValidateChecksum forks a goroutine to validate checksum after restore.
// it returns a channel fires a struct{} when all things get done.
func (rc *Client) GoValidateChecksum(
	ctx context.Context,
	inCh <-chan *CreatedTable,
	kvClient kv.Client,
	errCh chan<- error,
	updateCh glue.Progress,
	concurrency uint,
) chan *CreatedTable {
	log.Info("Start to validate checksum")
	outCh := DefaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			summary.CollectSuccessUnit("table checksum", 1, elapsed)
		}()
		err := rc.execChecksum(c, tbl, kvClient, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		updateCh.Inc()
		return nil
	}, func() {
		log.Info("all checksum ended")
	})
	return outCh
}

func (rc *Client) execChecksum(
	ctx context.Context,
	tbl *CreatedTable,
	kvClient kv.Client,
	concurrency uint,
) error {
	logger := log.L().With(
		zap.String("db", tbl.OldTable.DB.Name.O),
		zap.String("table", tbl.OldTable.Info.Name.O),
	)

	if tbl.OldTable.NoChecksum() {
		logger.Warn("table has no checksum, skipping checksum")
		return nil
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.execChecksum", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	item, exists := rc.checkpointChecksum[tbl.Table.ID]
	if !exists {
		startTS, err := rc.GetTSWithRetry(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		exe, err := checksum.NewExecutorBuilder(tbl.Table, startTS).
			SetOldTable(tbl.OldTable).
			SetConcurrency(concurrency).
			SetOldKeyspace(tbl.RewriteRule.OldKeyspace).
			SetNewKeyspace(tbl.RewriteRule.NewKeyspace).
			SetExplicitRequestSourceType(kvutil.ExplicitTypeBR).
			Build()
		if err != nil {
			return errors.Trace(err)
		}
		checksumResp, err := exe.Execute(ctx, kvClient, func() {
			// TODO: update progress here.
		})
		if err != nil {
			return errors.Trace(err)
		}
		item = &checkpoint.ChecksumItem{
			TableID:    tbl.Table.ID,
			Crc64xor:   checksumResp.Checksum,
			TotalKvs:   checksumResp.TotalKvs,
			TotalBytes: checksumResp.TotalBytes,
		}
		if rc.checkpointRunner != nil {
			err = rc.checkpointRunner.FlushChecksumItem(ctx, item)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	table := tbl.OldTable
	if item.Crc64xor != table.Crc64Xor ||
		item.TotalKvs != table.TotalKvs ||
		item.TotalBytes != table.TotalBytes {
		logger.Error("failed in validate checksum",
			zap.Uint64("origin tidb crc64", table.Crc64Xor),
			zap.Uint64("calculated crc64", item.Crc64xor),
			zap.Uint64("origin tidb total kvs", table.TotalKvs),
			zap.Uint64("calculated total kvs", item.TotalKvs),
			zap.Uint64("origin tidb total bytes", table.TotalBytes),
			zap.Uint64("calculated total bytes", item.TotalBytes),
		)
		return errors.Annotate(berrors.ErrRestoreChecksumMismatch, "failed to validate checksum")
	}
	logger.Info("success in validate checksum")
	return nil
}

func (rc *Client) GoUpdateMetaAndLoadStats(
	ctx context.Context,
	s storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	inCh <-chan *CreatedTable,
	errCh chan<- error,
	statsConcurrency uint,
	loadStats bool,
) chan *CreatedTable {
	log.Info("Start to update meta then load stats")
	outCh := DefaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(statsConcurrency, "UpdateStats")

	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
		oldTable := tbl.OldTable
		var statsErr error = nil
		if loadStats && oldTable.Stats != nil {
			log.Info("start loads analyze after validate checksum",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			// NOTICE: skip updating cache after load stats from json
			if statsErr = rc.statsHandler.LoadStatsFromJSONNoUpdate(ctx, rc.dom.InfoSchema(), oldTable.Stats, 0); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.Stats), zap.Error(statsErr))
			}
			log.Info("restore stat done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		} else if loadStats && len(oldTable.StatsFileIndexes) > 0 {
			log.Info("start to load statistic data for each partition",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			rewriteIDMap := getTableIDMap(tbl.Table, tbl.OldTable.Info)
			if statsErr = metautil.RestoreStats(ctx, s, cipher, rc.statsHandler, tbl.Table, oldTable.StatsFileIndexes, rewriteIDMap); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.StatsFileIndexes), zap.Error(statsErr))
			}
			log.Info("restore statistic data done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		}

		if statsErr != nil || !loadStats || (oldTable.Stats == nil && len(oldTable.StatsFileIndexes) == 0) {
			// Not need to return err when failed because of update analysis-meta
			log.Info("start update metas", zap.Stringer("table", oldTable.Info.Name), zap.Stringer("db", oldTable.DB.Name))
			// the total kvs contains the index kvs, but the stats meta needs the count of rows
			count := int64(oldTable.TotalKvs / uint64(len(oldTable.Info.Indices)+1))
			if statsErr = rc.statsHandler.SaveMetaToStorage(tbl.Table.ID, count, 0, "br restore"); statsErr != nil {
				log.Error("update stats meta failed", zap.Any("table", tbl.Table), zap.Error(statsErr))
			}
		}
		return nil
	}, func() {
		log.Info("all stats updated")
	})
	return outCh
}

func (rc *Client) GoWaitTiFlashReady(ctx context.Context, inCh <-chan *CreatedTable, updateCh glue.Progress, errCh chan<- error) chan *CreatedTable {
	log.Info("Start to wait tiflash replica sync")
	outCh := DefaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(4, "WaitForTiflashReady")
	// TODO support tiflash store changes
	tikvStats, err := infosync.GetTiFlashStoresStat(context.Background())
	if err != nil {
		errCh <- err
	}
	tiFlashStores := make(map[int64]pdhttp.StoreInfo)
	for _, store := range tikvStats.Stores {
		if engine.IsTiFlashHTTPResp(&store.Store) {
			tiFlashStores[store.Store.ID] = store
		}
	}
	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
		if tbl.Table != nil && tbl.Table.TiFlashReplica == nil {
			log.Info("table has no tiflash replica",
				zap.Stringer("table", tbl.OldTable.Info.Name),
				zap.Stringer("db", tbl.OldTable.DB.Name))
			updateCh.Inc()
			return nil
		}
		if rc.dom == nil {
			// unreachable, current we have initial domain in mgr.
			log.Fatal("unreachable, domain is nil")
		}
		log.Info("table has tiflash replica, start sync..",
			zap.Stringer("table", tbl.OldTable.Info.Name),
			zap.Stringer("db", tbl.OldTable.DB.Name))
		for {
			var progress float64
			if pi := tbl.Table.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
				for _, p := range pi.Definitions {
					progressOfPartition, err := infosync.MustGetTiFlashProgress(p.ID, tbl.Table.TiFlashReplica.Count, &tiFlashStores)
					if err != nil {
						log.Warn("failed to get progress for tiflash partition replica, retry it",
							zap.Int64("tableID", tbl.Table.ID), zap.Int64("partitionID", p.ID), zap.Error(err))
						time.Sleep(time.Second)
						continue
					}
					progress += progressOfPartition
				}
				progress = progress / float64(len(pi.Definitions))
			} else {
				var err error
				progress, err = infosync.MustGetTiFlashProgress(tbl.Table.ID, tbl.Table.TiFlashReplica.Count, &tiFlashStores)
				if err != nil {
					log.Warn("failed to get progress for tiflash replica, retry it",
						zap.Int64("tableID", tbl.Table.ID), zap.Error(err))
					time.Sleep(time.Second)
					continue
				}
			}
			// check until progress is 1
			if progress == 1 {
				log.Info("tiflash replica synced",
					zap.Stringer("table", tbl.OldTable.Info.Name),
					zap.Stringer("db", tbl.OldTable.DB.Name))
				break
			}
			// just wait for next check
			// tiflash check the progress every 2s
			// we can wait 2.5x times
			time.Sleep(5 * time.Second)
		}
		updateCh.Inc()
		return nil
	}, func() {
		log.Info("all tiflash replica synced")
	})
	return outCh
}

// called by failpoint, only used for test
// it would print the checksum result into the log, and
// the auto-test script records them to compare another
// cluster's checksum.
func (rc *Client) FailpointDoChecksumForLogRestore(
	ctx context.Context,
	kvClient kv.Client,
	pdClient pd.Client,
	idrules map[int64]int64,
	rewriteRules map[int64]*RewriteRules,
) (finalErr error) {
	startTS, err := rc.GetTSWithRetry(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// set gc safepoint for checksum
	sp := utils.BRServiceSafePoint{
		BackupTS: startTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}
	cctx, gcSafePointKeeperCancel := context.WithCancel(ctx)
	defer func() {
		log.Info("start to remove gc-safepoint keeper")
		// close the gc safe point keeper at first
		gcSafePointKeeperCancel()
		// set the ttl to 0 to remove the gc-safe-point
		sp.TTL = 0
		if err := utils.UpdateServiceSafePoint(ctx, pdClient, sp); err != nil {
			log.Warn("failed to update service safe point, backup may fail if gc triggered",
				zap.Error(err),
			)
		}
		log.Info("finish removing gc-safepoint keeper")
	}()
	err = utils.StartServiceSafePointKeeper(cctx, pdClient, sp)
	if err != nil {
		return errors.Trace(err)
	}

	eg, ectx := errgroup.WithContext(ctx)
	pool := tidbutil.NewWorkerPool(4, "checksum for log restore")
	infoSchema := rc.GetDomain().InfoSchema()
	// downstream id -> upstream id
	reidRules := make(map[int64]int64)
	for upstreamID, downstreamID := range idrules {
		reidRules[downstreamID] = upstreamID
	}
	for upstreamID, downstreamID := range idrules {
		newTable, ok := infoSchema.TableByID(downstreamID)
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

const (
	restoreLabelKey   = "exclusive"
	restoreLabelValue = "restore"
)

// LoadRestoreStores loads the stores used to restore data.
func (rc *Client) LoadRestoreStores(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.LoadRestoreStores", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, rc.pdClient, util.SkipTiFlash)

	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		rc.storeCount++
		if rc.isOnline {
			for _, l := range s.GetLabels() {
				if l.GetKey() == restoreLabelKey && l.GetValue() == restoreLabelValue {
					rc.restoreStores = append(rc.restoreStores, s.GetId())
					break
				}
			}
		}
	}
	if rc.isOnline {
		log.Info("load restore stores", zap.Uint64s("store-ids", rc.restoreStores))
	}
	return nil
}

// ResetRestoreLabels removes the exclusive labels of the restore stores.
func (rc *Client) ResetRestoreLabels(ctx context.Context) error {
	if !rc.isOnline {
		return nil
	}
	log.Info("start resetting store labels")
	return rc.toolClient.SetStoresLabel(ctx, rc.restoreStores, restoreLabelKey, "")
}

// SetupPlacementRules sets rules for the tables' regions.
func (rc *Client) SetupPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start setting placement rules")
	rule, err := rc.toolClient.GetPlacementRule(ctx, "pd", "default")
	if err != nil {
		return errors.Trace(err)
	}
	rule.Index = 100
	rule.Override = true
	rule.LabelConstraints = append(rule.LabelConstraints, pdhttp.LabelConstraint{
		Key:    restoreLabelKey,
		Op:     "in",
		Values: []string{restoreLabelValue},
	})
	for _, t := range tables {
		rule.ID = rc.getRuleID(t.ID)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1)))
		err = rc.toolClient.SetPlacementRule(ctx, rule)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("finish setting placement rules")
	return nil
}

// WaitPlacementSchedule waits PD to move tables to restore stores.
func (rc *Client) WaitPlacementSchedule(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start waiting placement schedule")
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, progress, err := rc.checkRegions(ctx, tables)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				log.Info("finish waiting placement schedule")
				return nil
			}
			log.Info("placement schedule progress: " + progress)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rc *Client) checkRegions(ctx context.Context, tables []*model.TableInfo) (bool, string, error) {
	for i, t := range tables {
		start := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID))
		end := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1))
		ok, regionProgress, err := rc.checkRange(ctx, start, end)
		if err != nil {
			return false, "", errors.Trace(err)
		}
		if !ok {
			return false, fmt.Sprintf("table %v/%v, %s", i, len(tables), regionProgress), nil
		}
	}
	return true, "", nil
}

func (rc *Client) checkRange(ctx context.Context, start, end []byte) (bool, string, error) {
	regions, err := rc.toolClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	for i, r := range regions {
	NEXT_PEER:
		for _, p := range r.Region.GetPeers() {
			for _, storeID := range rc.restoreStores {
				if p.GetStoreId() == storeID {
					continue NEXT_PEER
				}
			}
			return false, fmt.Sprintf("region %v/%v", i, len(regions)), nil
		}
	}
	return true, "", nil
}

// ResetPlacementRules removes placement rules for tables.
func (rc *Client) ResetPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start resetting placement rules")
	var failedTables []int64
	for _, t := range tables {
		err := rc.toolClient.DeletePlacementRule(ctx, "pd", rc.getRuleID(t.ID))
		if err != nil {
			log.Info("failed to delete placement rule for table", zap.Int64("table-id", t.ID))
			failedTables = append(failedTables, t.ID)
		}
	}
	if len(failedTables) > 0 {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "failed to delete placement rules for tables %v", failedTables)
	}
	return nil
}

func (rc *Client) getRuleID(tableID int64) string {
	return "restore-t" + strconv.FormatInt(tableID, 10)
}

// IsFull returns whether this backup is full.
func (rc *Client) IsFull() bool {
	failpoint.Inject("mock-incr-backup-data", func() {
		failpoint.Return(false)
	})
	return !rc.IsIncremental()
}

// IsIncremental returns whether this backup is incremental.
func (rc *Client) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}

// EnableSkipCreateSQL sets switch of skip create schema and tables.
func (rc *Client) EnableSkipCreateSQL() {
	rc.noSchema = true
}

// IsSkipCreateSQL returns whether we need skip create schema and tables in restore.
func (rc *Client) IsSkipCreateSQL() bool {
	return rc.noSchema
}

// GenerateRebasedTables generate a map[UniqueTableName]bool to represent tables that haven't updated table info.
// there are two situations:
// 1. tables that already exists in the restored cluster.
// 2. tables that are created by executing ddl jobs.
// so, only tables in incremental restoration will be added to the map
func (rc *Client) GenerateRebasedTables(tables []*metautil.Table) {
	if !rc.IsIncremental() {
		// in full restoration, all tables are created by Session.CreateTable, and all tables' info is updated.
		rc.rebasedTablesMap = make(map[UniqueTableName]bool)
		return
	}

	rc.rebasedTablesMap = make(map[UniqueTableName]bool, len(tables))
	for _, table := range tables {
		rc.rebasedTablesMap[UniqueTableName{DB: table.DB.Name.String(), Table: table.Info.Name.String()}] = true
	}
}

// GetRebasedTables returns tables that may need to be rebase auto increment id or auto random id
func (rc *Client) GetRebasedTables() map[UniqueTableName]bool {
	return rc.rebasedTablesMap
}

func (rc *Client) getTiFlashNodeCount(ctx context.Context) (uint64, error) {
	tiFlashStores, err := util.GetAllTiKVStores(ctx, rc.pdClient, util.TiFlashOnly)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint64(len(tiFlashStores)), nil
}

// PreCheckTableTiFlashReplica checks whether TiFlash replica is less than TiFlash node.
func (rc *Client) PreCheckTableTiFlashReplica(
	ctx context.Context,
	tables []*metautil.Table,
	recorder *tiflashrec.TiFlashRecorder,
) error {
	tiFlashStoreCount, err := rc.getTiFlashNodeCount(ctx)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table.Info.TiFlashReplica != nil {
			// we should not set available to true. because we cannot guarantee the raft log lag of tiflash when restore finished.
			// just let tiflash ticker set it by checking lag of all related regions.
			table.Info.TiFlashReplica.Available = false
			table.Info.TiFlashReplica.AvailablePartitionIDs = nil
			if recorder != nil {
				recorder.AddTable(table.Info.ID, *table.Info.TiFlashReplica)
				log.Info("record tiflash replica for table, to reset it by ddl later",
					zap.Stringer("db", table.DB.Name),
					zap.Stringer("table", table.Info.Name),
				)
				table.Info.TiFlashReplica = nil
			} else if table.Info.TiFlashReplica.Count > tiFlashStoreCount {
				// we cannot satisfy TiFlash replica in restore cluster. so we should
				// set TiFlashReplica to unavailable in tableInfo, to avoid TiDB cannot sense TiFlash and make plan to TiFlash
				// see details at https://github.com/pingcap/br/issues/931
				// TODO maybe set table.Info.TiFlashReplica.Count to tiFlashStoreCount, but we need more tests about it.
				log.Warn("table does not satisfy tiflash replica requirements, set tiflash replcia to unavaiable",
					zap.Stringer("db", table.DB.Name),
					zap.Stringer("table", table.Info.Name),
					zap.Uint64("expect tiflash replica", table.Info.TiFlashReplica.Count),
					zap.Uint64("actual tiflash store", tiFlashStoreCount),
				)
				table.Info.TiFlashReplica = nil
			}
		}
	}
	return nil
}

// PreCheckTableClusterIndex checks whether backup tables and existed tables have different cluster index options。
func (rc *Client) PreCheckTableClusterIndex(
	tables []*metautil.Table,
	ddlJobs []*model.Job,
	dom *domain.Domain,
) error {
	for _, table := range tables {
		oldTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
		// table exists in database
		if err == nil {
			if table.Info.IsCommonHandle != oldTableInfo.IsCommonHandle {
				return errors.Annotatef(berrors.ErrRestoreModeMismatch,
					"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
					transferBoolToValue(table.Info.IsCommonHandle),
					table.Info.IsCommonHandle,
					oldTableInfo.IsCommonHandle)
			}
		}
	}
	for _, job := range ddlJobs {
		if job.Type == model.ActionCreateTable {
			tableInfo := job.BinlogInfo.TableInfo
			if tableInfo != nil {
				oldTableInfo, err := rc.GetTableSchema(dom, model.NewCIStr(job.SchemaName), tableInfo.Name)
				// table exists in database
				if err == nil {
					if tableInfo.IsCommonHandle != oldTableInfo.IsCommonHandle {
						return errors.Annotatef(berrors.ErrRestoreModeMismatch,
							"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
							transferBoolToValue(tableInfo.IsCommonHandle),
							tableInfo.IsCommonHandle,
							oldTableInfo.IsCommonHandle)
					}
				}
			}
		}
	}
	return nil
}

func (rc *Client) InstallLogFileManager(ctx context.Context, startTS, restoreTS uint64, metadataDownloadBatchSize uint) error {
	init := LogFileManagerInit{
		StartTS:   startTS,
		RestoreTS: restoreTS,
		Storage:   rc.storage,

		MetadataDownloadBatchSize: metadataDownloadBatchSize,
	}
	var err error
	rc.logFileManager, err = CreateLogFileManager(ctx, init)
	if err != nil {
		return err
	}
	return nil
}

// FixIndex tries to fix a single index.
func (rc *Client) FixIndex(ctx context.Context, schema, table, index string) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("Client.LoadRestoreStores index: %s.%s:%s",
			schema, table, index), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	sql := fmt.Sprintf("ADMIN RECOVER INDEX %s %s;",
		utils.EncloseDBAndTable(schema, table),
		utils.EncloseName(index))
	log.Debug("Executing fix index sql.", zap.String("sql", sql))
	err := rc.db.se.Execute(ctx, sql)
	if err != nil {
		return errors.Annotatef(err, "failed to execute %s", sql)
	}
	return nil
}

// FixIndicesOfTables tries to fix the indices of the tables via `ADMIN RECOVERY INDEX`.
func (rc *Client) FixIndicesOfTables(
	ctx context.Context,
	fullBackupTables map[int64]*metautil.Table,
	onProgress func(),
) error {
	for _, table := range fullBackupTables {
		if name, ok := utils.GetSysDBName(table.DB.Name); utils.IsSysDB(name) && ok {
			// skip system table for now
			onProgress()
			continue
		}

		if err := rc.FixIndicesOfTable(ctx, table.DB.Name.L, table.Info); err != nil {
			return errors.Annotatef(err, "failed to fix index for table %s.%s", table.DB.Name, table.Info.Name)
		}
		onProgress()
	}

	return nil
}

// FixIndicdesOfTable tries to fix the indices of the table via `ADMIN RECOVERY INDEX`.
func (rc *Client) FixIndicesOfTable(ctx context.Context, schema string, table *model.TableInfo) error {
	tableName := table.Name.L
	// NOTE: Maybe we can create multi sessions and restore indices concurrently?
	for _, index := range table.Indices {
		start := time.Now()
		if err := rc.FixIndex(ctx, schema, tableName, index.Name.L); err != nil {
			return errors.Annotatef(err, "failed to fix index %s", index.Name)
		}

		log.Info("Fix index done.", zap.Stringer("take", time.Since(start)),
			zap.String("table", schema+"."+tableName),
			zap.Stringer("index", index.Name))
	}
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
			if f.GetCf() == stream.DefaultCF {
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

func ApplyKVFilesWithSingelMethod(
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

func (rc *Client) RestoreKVFiles(
	ctx context.Context,
	rules map[int64]*RewriteRules,
	idrules map[int64]int64,
	logIter LogIter,
	runner *checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType],
	pitrBatchCount uint32,
	pitrBatchSize uint32,
	updateStats func(kvCount uint64, size uint64),
	onProgress func(cnt int64),
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
			log.Info("Restore KV files", zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", fileCount, elapsed)
		}
	}()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreKVFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var applyWg sync.WaitGroup
	eg, ectx := errgroup.WithContext(ctx)
	applyFunc := func(files []*LogDataFileInfo, kvCount int64, size uint64) {
		if len(files) == 0 {
			return
		}
		// get rewrite rule from table id.
		// because the tableID of files is the same.
		rule, ok := rules[files[0].TableId]
		if !ok {
			// TODO handle new created table
			// For this version we do not handle new created table after full backup.
			// in next version we will perform rewrite and restore meta key to restore new created tables.
			// so we can simply skip the file that doesn't have the rule here.
			onProgress(int64(len(files)))
			summary.CollectInt("FileSkip", len(files))
			log.Debug("skip file due to table id not matched", zap.Int64("table-id", files[0].TableId))
			skipFile += len(files)
		} else {
			applyWg.Add(1)
			downstreamId := idrules[files[0].TableId]
			rc.workerPool.ApplyOnErrorGroup(eg, func() (err error) {
				fileStart := time.Now()
				defer applyWg.Done()
				defer func() {
					onProgress(int64(len(files)))
					updateStats(uint64(kvCount), size)
					summary.CollectInt("File", len(files))

					if err == nil {
						filenames := make([]string, 0, len(files))
						if runner == nil {
							for _, f := range files {
								filenames = append(filenames, f.Path+", ")
							}
						} else {
							for _, f := range files {
								filenames = append(filenames, f.Path+", ")
								if e := checkpoint.AppendRangeForLogRestore(ectx, runner, f.MetaDataGroupName, downstreamId, f.OffsetInMetaGroup, f.OffsetInMergedGroup); e != nil {
									err = errors.Annotate(e, "failed to append checkpoint data")
									break
								}
							}
						}
						log.Info("import files done", zap.Int("batch-count", len(files)), zap.Uint64("batch-size", size),
							zap.Duration("take", time.Since(fileStart)), zap.Strings("files", filenames))
					}
				}()

				return rc.fileImporter.ImportKVFiles(ectx, files, rule, rc.shiftStartTS, rc.startTS, rc.restoreTS, supportBatch)
			})
		}
	}

	rc.workerPool.ApplyOnErrorGroup(eg, func() error {
		if supportBatch {
			err = ApplyKVFilesWithBatchMethod(ectx, logIter, int(pitrBatchCount), uint64(pitrBatchSize), applyFunc, &applyWg)
		} else {
			err = ApplyKVFilesWithSingelMethod(ectx, logIter, applyFunc, &applyWg)
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

func (rc *Client) CleanUpKVFiles(
	ctx context.Context,
) error {
	// Current we only have v1 prefix.
	// In the future, we can add more operation for this interface.
	return rc.fileImporter.ClearFiles(ctx, rc.pdClient, "v1")
}

func (rc *Client) initSchemasMap(
	ctx context.Context,
	clusterID uint64,
	restoreTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	filename := metautil.PitrIDMapsFilename(clusterID, restoreTS)
	exist, err := rc.storage.FileExists(ctx, filename)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to check filename:%s ", filename)
	} else if !exist {
		log.Info("pitr id maps isn't existed", zap.String("file", filename))
		return nil, nil
	}

	metaData, err := rc.storage.ReadFile(ctx, filename)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	return backupMeta.GetDbMaps(), nil
}

func initFullBackupTables(
	ctx context.Context,
	s storage.ExternalStorage,
	tableFilter filter.Filter,
) (map[int64]*metautil.Table, error) {
	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	// read full backup databases to get map[table]table.Info
	reader := metautil.NewMetaReader(backupMeta, s, nil)

	databases, err := metautil.LoadBackupTables(ctx, reader, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make(map[int64]*metautil.Table)
	for _, db := range databases {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}

		if !tableFilter.MatchSchema(dbName) {
			continue
		}

		for _, table := range db.Tables {
			// check this db is empty.
			if table.Info == nil {
				tables[db.Info.ID] = table
				continue
			}
			if !tableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			tables[table.Info.ID] = table
		}
	}

	return tables, nil
}

type FullBackupStorageConfig struct {
	Backend *backuppb.StorageBackend
	Opts    *storage.ExternalStorageOptions
}

type InitSchemaConfig struct {
	// required
	IsNewTask      bool
	HasFullRestore bool
	TableFilter    filter.Filter

	// optional
	TiFlashRecorder   *tiflashrec.TiFlashRecorder
	FullBackupStorage *FullBackupStorageConfig
}

// InitSchemasReplaceForDDL gets schemas information Mapping from old schemas to new schemas.
// It is used to rewrite meta kv-event.
func (rc *Client) InitSchemasReplaceForDDL(
	ctx context.Context,
	cfg *InitSchemaConfig,
) (*stream.SchemasReplace, error) {
	var (
		err    error
		dbMaps []*backuppb.PitrDBMap
		// the id map doesn't need to construct only when it is not the first execution
		needConstructIdMap bool

		dbReplaces = make(map[stream.UpstreamID]*stream.DBReplace)
	)

	// not new task, load schemas map from external storage
	if !cfg.IsNewTask {
		log.Info("try to load pitr id maps")
		needConstructIdMap = false
		dbMaps, err = rc.initSchemasMap(ctx, rc.GetClusterID(ctx), rc.restoreTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// a new task, but without full snapshot restore, tries to load
	// schemas map whose `restore-ts`` is the task's `start-ts`.
	if len(dbMaps) <= 0 && !cfg.HasFullRestore {
		log.Info("try to load pitr id maps of the previous task", zap.Uint64("start-ts", rc.startTS))
		needConstructIdMap = true
		dbMaps, err = rc.initSchemasMap(ctx, rc.GetClusterID(ctx), rc.startTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(dbMaps) <= 0 {
		log.Info("no id maps, build the table replaces from cluster and full backup schemas")
		needConstructIdMap = true
		s, err := storage.New(ctx, cfg.FullBackupStorage.Backend, cfg.FullBackupStorage.Opts)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fullBackupTables, err := initFullBackupTables(ctx, s, cfg.TableFilter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, t := range fullBackupTables {
			dbName, _ := utils.GetSysDBCIStrName(t.DB.Name)
			newDBInfo, exist := rc.GetDBSchema(rc.GetDomain(), dbName)
			if !exist {
				log.Info("db not existed", zap.String("dbname", dbName.String()))
				continue
			}

			dbReplace, exist := dbReplaces[t.DB.ID]
			if !exist {
				dbReplace = stream.NewDBReplace(t.DB.Name.O, newDBInfo.ID)
				dbReplaces[t.DB.ID] = dbReplace
			}

			if t.Info == nil {
				// If the db is empty, skip it.
				continue
			}
			newTableInfo, err := rc.GetTableSchema(rc.GetDomain(), dbName, t.Info.Name)
			if err != nil {
				log.Info("table not existed", zap.String("tablename", dbName.String()+"."+t.Info.Name.String()))
				continue
			}

			dbReplace.TableMap[t.Info.ID] = &stream.TableReplace{
				Name:         newTableInfo.Name.O,
				TableID:      newTableInfo.ID,
				PartitionMap: getPartitionIDMap(newTableInfo, t.Info),
				IndexMap:     getIndexIDMap(newTableInfo, t.Info),
			}
		}
	} else {
		dbReplaces = stream.FromSchemaMaps(dbMaps)
	}

	for oldDBID, dbReplace := range dbReplaces {
		log.Info("replace info", func() []zapcore.Field {
			fields := make([]zapcore.Field, 0, (len(dbReplace.TableMap)+1)*3)
			fields = append(fields,
				zap.String("dbName", dbReplace.Name),
				zap.Int64("oldID", oldDBID),
				zap.Int64("newID", dbReplace.DbID))
			for oldTableID, tableReplace := range dbReplace.TableMap {
				fields = append(fields,
					zap.String("table", tableReplace.Name),
					zap.Int64("oldID", oldTableID),
					zap.Int64("newID", tableReplace.TableID))
			}
			return fields
		}()...)
	}

	rp := stream.NewSchemasReplace(
		dbReplaces, needConstructIdMap, cfg.TiFlashRecorder, rc.currentTS, cfg.TableFilter, rc.GenGlobalID, rc.GenGlobalIDs,
		rc.RecordDeleteRange)
	return rp, nil
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

// RestoreMetaKVFiles tries to restore files about meta kv-event from stream-backup.
func (rc *Client) RestoreMetaKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
) error {
	filesInWriteCF := make([]*backuppb.DataFileInfo, 0, len(files))
	filesInDefaultCF := make([]*backuppb.DataFileInfo, 0, len(files))

	// The k-v events in default CF should be restored firstly. The reason is that:
	// The error of transactions of meta could happen if restore write CF events successfully,
	// but failed to restore default CF events.
	for _, f := range files {
		if f.Cf == stream.WriteCF {
			filesInWriteCF = append(filesInWriteCF, f)
			continue
		}
		if f.Type == backuppb.FileType_Delete {
			// this should happen abnormally.
			// only do some preventive checks here.
			log.Warn("detected delete file of meta key, skip it", zap.Any("file", f))
			continue
		}
		if f.Cf == stream.DefaultCF {
			filesInDefaultCF = append(filesInDefaultCF, f)
		}
	}
	filesInDefaultCF = SortMetaKVFiles(filesInDefaultCF)
	filesInWriteCF = SortMetaKVFiles(filesInWriteCF)

	failpoint.Inject("failed-before-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed before id maps saved"))
	})

	log.Info("start to restore meta files",
		zap.Int("total files", len(files)),
		zap.Int("default files", len(filesInDefaultCF)),
		zap.Int("write files", len(filesInWriteCF)))

	if schemasReplace.NeedConstructIdMap() {
		// Preconstruct the map and save it into external storage.
		if err := rc.PreConstructAndSaveIDMap(
			ctx,
			filesInWriteCF,
			filesInDefaultCF,
			schemasReplace,
		); err != nil {
			return errors.Trace(err)
		}
	}
	failpoint.Inject("failed-after-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed after id maps saved"))
	})

	// run the rewrite and restore meta-kv into TiKV cluster.
	if err := rc.RestoreMetaKVFilesWithBatchMethod(
		ctx,
		filesInDefaultCF,
		filesInWriteCF,
		schemasReplace,
		updateStats,
		progressInc,
		rc.RestoreBatchMetaKVFiles,
	); err != nil {
		return errors.Trace(err)
	}

	// Update global schema version and report all of TiDBs.
	if err := rc.UpdateSchemaVersion(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// PreConstructAndSaveIDMap constructs id mapping and save it.
func (rc *Client) PreConstructAndSaveIDMap(
	ctx context.Context,
	fsInWriteCF, fsInDefaultCF []*backuppb.DataFileInfo,
	sr *stream.SchemasReplace,
) error {
	sr.SetPreConstructMapStatus()

	if err := rc.constructIDMap(ctx, fsInWriteCF, sr); err != nil {
		return errors.Trace(err)
	}
	if err := rc.constructIDMap(ctx, fsInDefaultCF, sr); err != nil {
		return errors.Trace(err)
	}

	if err := rc.SaveIDMap(ctx, sr); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *Client) constructIDMap(
	ctx context.Context,
	fs []*backuppb.DataFileInfo,
	sr *stream.SchemasReplace,
) error {
	for _, f := range fs {
		entries, _, err := rc.ReadAllEntries(ctx, f, math.MaxUint64)
		if err != nil {
			return errors.Trace(err)
		}

		for _, entry := range entries {
			if _, err := sr.RewriteKvEntry(&entry.e, f.GetCf()); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (rc *Client) RestoreMetaKVFilesWithBatchMethod(
	ctx context.Context,
	defaultFiles []*backuppb.DataFileInfo,
	writeFiles []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
	restoreBatch func(
		ctx context.Context,
		files []*backuppb.DataFileInfo,
		schemasReplace *stream.SchemasReplace,
		kvEntries []*KvEntryWithTS,
		filterTS uint64,
		updateStats func(kvCount uint64, size uint64),
		progressInc func(),
		cf string,
	) ([]*KvEntryWithTS, error),
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
	// Set restoreKV to SchemaReplace.
	schemasReplace.SetRestoreKVStatus()

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
				defaultKvEntries, err = restoreBatch(ctx, defaultFiles[defaultIdx:i], schemasReplace, defaultKvEntries, f.MinTs, updateStats, progressInc, stream.DefaultCF)
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
				writeKvEntries, err = restoreBatch(ctx, writeFiles[writeIdx:toWriteIdx], schemasReplace, writeKvEntries, f.MinTs, updateStats, progressInc, stream.WriteCF)
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
	_, err = restoreBatch(ctx, defaultFiles[defaultIdx:], schemasReplace, defaultKvEntries, math.MaxUint64, updateStats, progressInc, stream.DefaultCF)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = restoreBatch(ctx, writeFiles[writeIdx:], schemasReplace, writeKvEntries, math.MaxUint64, updateStats, progressInc, stream.WriteCF)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// the kv entry with ts, the ts is decoded from entry.
type KvEntryWithTS struct {
	e  kv.Entry
	ts uint64
}

func (rc *Client) RestoreBatchMetaKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	kvEntries []*KvEntryWithTS,
	filterTS uint64,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
	cf string,
) ([]*KvEntryWithTS, error) {
	nextKvEntries := make([]*KvEntryWithTS, 0)
	curKvEntries := make([]*KvEntryWithTS, 0)
	if len(files) == 0 && len(kvEntries) == 0 {
		return nextKvEntries, nil
	}

	// filter the kv from kvEntries again.
	for _, kv := range kvEntries {
		if kv.ts < filterTS {
			curKvEntries = append(curKvEntries, kv)
		} else {
			nextKvEntries = append(nextKvEntries, kv)
		}
	}

	// read all of entries from files.
	for _, f := range files {
		es, nextEs, err := rc.ReadAllEntries(ctx, f, filterTS)
		if err != nil {
			return nextKvEntries, errors.Trace(err)
		}

		curKvEntries = append(curKvEntries, es...)
		nextKvEntries = append(nextKvEntries, nextEs...)
	}

	// sort these entries.
	slices.SortFunc(curKvEntries, func(i, j *KvEntryWithTS) int {
		return cmp.Compare(i.ts, j.ts)
	})

	// restore these entries with rawPut() method.
	kvCount, size, err := rc.restoreMetaKvEntries(ctx, schemasReplace, curKvEntries, cf)
	if err != nil {
		return nextKvEntries, errors.Trace(err)
	}

	if schemasReplace.IsRestoreKVStatus() {
		updateStats(kvCount, size)
		for i := 0; i < len(files); i++ {
			progressInc()
		}
	}
	return nextKvEntries, nil
}

func (rc *Client) restoreMetaKvEntries(
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
		log.Debug("before rewrte entry", zap.Uint64("key-ts", entry.ts), zap.Int("key-len", len(entry.e.Key)),
			zap.Int("value-len", len(entry.e.Value)), zap.ByteString("key", entry.e.Key))

		newEntry, err := sr.RewriteKvEntry(&entry.e, columnFamily)
		if err != nil {
			log.Error("rewrite txn entry failed", zap.Int("klen", len(entry.e.Key)),
				logutil.Key("txn-key", entry.e.Key))
			return 0, 0, errors.Trace(err)
		} else if newEntry == nil {
			continue
		}
		log.Debug("after rewrite entry", zap.Int("new-key-len", len(newEntry.Key)),
			zap.Int("new-value-len", len(entry.e.Value)), zap.ByteString("new-key", newEntry.Key))

		failpoint.Inject("failed-to-restore-metakv", func(_ failpoint.Value) {
			failpoint.Return(0, 0, errors.Errorf("failpoint: failed to restore metakv"))
		})
		if err := rc.rawKVClient.Put(ctx, newEntry.Key, newEntry.Value, entry.ts); err != nil {
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

func transferBoolToValue(enable bool) string {
	if enable {
		return "ON"
	}
	return "OFF"
}

// GenGlobalID generates a global id by transaction way.
func (rc *Client) GenGlobalID(ctx context.Context) (int64, error) {
	var id int64
	storage := rc.GetDomain().Store()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMeta(txn)
			id, e = t.GenGlobalID()
			return e
		})

	return id, err
}

// GenGlobalIDs generates several global ids by transaction way.
func (rc *Client) GenGlobalIDs(ctx context.Context, n int) ([]int64, error) {
	ids := make([]int64, 0)
	storage := rc.GetDomain().Store()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMeta(txn)
			ids, e = t.GenGlobalIDs(n)
			return e
		})

	return ids, err
}

// UpdateSchemaVersion updates schema version by transaction way.
func (rc *Client) UpdateSchemaVersion(ctx context.Context) error {
	storage := rc.GetDomain().Store()
	var schemaVersion int64

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	if err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			var e error
			// To trigger full-reload instead of diff-reload, we need to increase the schema version
			// by at least `domain.LoadSchemaDiffVersionGapThreshold`.
			schemaVersion, e = t.GenSchemaVersions(64 + domain.LoadSchemaDiffVersionGapThreshold)
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

const (
	alterTableDropIndexSQL         = "ALTER TABLE %n.%n DROP INDEX %n"
	alterTableAddIndexFormat       = "ALTER TABLE %%n.%%n ADD INDEX %%n(%s)"
	alterTableAddUniqueIndexFormat = "ALTER TABLE %%n.%%n ADD UNIQUE KEY %%n(%s)"
	alterTableAddPrimaryFormat     = "ALTER TABLE %%n.%%n ADD PRIMARY KEY (%s) NONCLUSTERED"
)

func (rc *Client) generateRepairIngestIndexSQLs(
	ctx context.Context,
	ingestRecorder *ingestrec.IngestRecorder,
	allSchema []*model.DBInfo,
	taskName string,
) ([]checkpoint.CheckpointIngestIndexRepairSQL, bool, error) {
	var sqls []checkpoint.CheckpointIngestIndexRepairSQL
	if rc.useCheckpoint {
		exists, err := checkpoint.ExistsCheckpointIngestIndexRepairSQLs(ctx, rc.storage, taskName)
		if err != nil {
			return sqls, false, errors.Trace(err)
		}
		if exists {
			checkpointSQLs, err := checkpoint.LoadCheckpointIngestIndexRepairSQLs(ctx, rc.storage, taskName)
			if err != nil {
				return sqls, false, errors.Trace(err)
			}
			sqls = checkpointSQLs.SQLs
			log.Info("load ingest index repair sqls from checkpoint", zap.String("category", "ingest"), zap.Reflect("sqls", sqls))
			return sqls, true, nil
		}
	}

	ingestRecorder.UpdateIndexInfo(allSchema)
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
		// USING BTREE/HASH/RTREE
		indexTypeStr := info.IndexInfo.Tp.String()
		if len(indexTypeStr) > 0 {
			addSQL.WriteString(" USING ")
			addSQL.WriteString(indexTypeStr)
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
		return sqls, false, errors.Trace(err)
	}

	if rc.useCheckpoint && len(sqls) > 0 {
		if err := checkpoint.SaveCheckpointIngestIndexRepairSQLs(ctx, rc.storage, &checkpoint.CheckpointIngestIndexRepairSQLs{
			SQLs: sqls,
		}, taskName); err != nil {
			return sqls, false, errors.Trace(err)
		}
	}
	return sqls, false, nil
}

// RepairIngestIndex drops the indexes from IngestRecorder and re-add them.
func (rc *Client) RepairIngestIndex(ctx context.Context, ingestRecorder *ingestrec.IngestRecorder, g glue.Glue, storage kv.Storage, taskName string) error {
	dom, err := g.GetDomain(storage)
	if err != nil {
		return errors.Trace(err)
	}
	info := dom.InfoSchema()

	sqls, fromCheckpoint, err := rc.generateRepairIngestIndexSQLs(ctx, ingestRecorder, info.AllSchemas(), taskName)
	if err != nil {
		return errors.Trace(err)
	}

	console := glue.GetConsole(g)
NEXTSQL:
	for _, sql := range sqls {
		progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)

		tableInfo, err := info.TableByName(sql.SchemaName, sql.TableName)
		if err != nil {
			return errors.Trace(err)
		}
		oldIndexIDFound := false
		if fromCheckpoint {
			for _, idx := range tableInfo.Indices() {
				indexInfo := idx.Meta()
				if indexInfo.ID == sql.IndexID {
					// the original index id is not dropped
					oldIndexIDFound = true
					break
				}
				// what if index's state is not public?
				if indexInfo.Name.O == sql.IndexName {
					// find the same name index, but not the same index id,
					// which means the repaired index id is created
					if _, err := fmt.Fprintf(console.Out(), "%s ... %s\n", progressTitle, color.HiGreenString("SKIPPED DUE TO CHECKPOINT MODE")); err != nil {
						return errors.Trace(err)
					}
					continue NEXTSQL
				}
			}
		}

		if err := func(sql checkpoint.CheckpointIngestIndexRepairSQL) error {
			w := console.StartProgressBar(progressTitle, glue.OnlyOneTask)
			defer w.Close()

			// TODO: When the TiDB supports the DROP and CREATE the same name index in one SQL,
			//   the checkpoint for ingest recorder can be removed and directly use the SQL:
			//      ALTER TABLE db.tbl DROP INDEX `i_1`, ADD IDNEX `i_1` ...
			//
			// This SQL is compatible with checkpoint: If one ingest index has been recreated by
			// the SQL, the index's id would be another one. In the next retry execution, BR can
			// not find the ingest index's dropped id so that BR regards it as a dropped index by
			// restored metakv and then skips repairing it.

			// only when first execution or old index id is not dropped
			if !fromCheckpoint || oldIndexIDFound {
				if err := rc.db.se.ExecuteInternal(ctx, alterTableDropIndexSQL, sql.SchemaName.O, sql.TableName.O, sql.IndexName); err != nil {
					return errors.Trace(err)
				}
			}
			failpoint.Inject("failed-before-create-ingest-index", func(v failpoint.Value) {
				if v != nil && v.(bool) {
					failpoint.Return(errors.New("failed before create ingest index"))
				}
			})
			// create the repaired index when first execution or not found it
			if err := rc.db.se.ExecuteInternal(ctx, sql.AddSQL, sql.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			w.Inc()
			if err := w.Wait(ctx); err != nil {
				return errors.Trace(err)
			}
			return nil
		}(sql); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (rc *Client) RecordDeleteRange(sql *stream.PreDelRangeQuery) {
	rc.deleteRangeQueryCh <- sql
}

// use channel to save the delete-range query to make it thread-safety.
func (rc *Client) RunGCRowsLoader(ctx context.Context) {
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
func (rc *Client) InsertGCRows(ctx context.Context) error {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	ts, err := rc.GetTSWithRetry(ctx)
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
			if err := rc.db.se.ExecuteInternal(ctx, sql, paramsList...); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// only for unit test
func (rc *Client) GetGCRows() []*stream.PreDelRangeQuery {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	return rc.deleteRangeQuery
}

// SaveIDMap saves the id mapping information.
func (rc *Client) SaveIDMap(
	ctx context.Context,
	sr *stream.SchemasReplace,
) error {
	idMaps := sr.TidySchemaMaps()
	clusterID := rc.GetClusterID(ctx)
	metaFileName := metautil.PitrIDMapsFilename(clusterID, rc.restoreTS)
	metaWriter := metautil.NewMetaWriter(rc.storage, metautil.MetaFileSize, false, metaFileName, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		// save log startTS to backupmeta file
		m.ClusterId = clusterID
		m.DbMaps = idMaps
	})

	if err := metaWriter.FlushBackupMeta(ctx); err != nil {
		return errors.Trace(err)
	}
	if rc.useCheckpoint {
		var items map[int64]model.TiFlashReplicaInfo
		if sr.TiflashRecorder != nil {
			items = sr.TiflashRecorder.GetItems()
		}
		log.Info("save checkpoint task info with InLogRestoreAndIdMapPersist status")
		if err := checkpoint.SaveCheckpointTaskInfoForLogRestore(ctx, rc.storage, &checkpoint.CheckpointTaskInfoForLogRestore{
			Progress:     checkpoint.InLogRestoreAndIdMapPersist,
			StartTS:      rc.startTS,
			RestoreTS:    rc.restoreTS,
			RewriteTS:    rc.currentTS,
			TiFlashItems: items,
		}, rc.GetClusterID(ctx)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// InitFullClusterRestore init fullClusterRestore and set SkipGrantTable as needed
func (rc *Client) InitFullClusterRestore(explicitFilter bool) {
	rc.fullClusterRestore = !explicitFilter && rc.IsFull()

	log.Info("full cluster restore", zap.Bool("value", rc.fullClusterRestore))
}

func (rc *Client) IsFullClusterRestore() bool {
	return rc.fullClusterRestore
}

func (rc *Client) SetWithSysTable(withSysTable bool) {
	rc.withSysTable = withSysTable
}

func (rc *Client) ResetTiFlashReplicas(ctx context.Context, g glue.Glue, storage kv.Storage) error {
	dom, err := g.GetDomain(storage)
	if err != nil {
		return errors.Trace(err)
	}
	info := dom.InfoSchema()
	allSchemaName := info.AllSchemaNames()
	recorder := tiflashrec.New()

	expectTiFlashStoreCount := uint64(0)
	needTiFlash := false
	for _, s := range allSchemaName {
		for _, t := range info.SchemaTables(s) {
			t := t.Meta()
			if t.TiFlashReplica != nil {
				expectTiFlashStoreCount = max(expectTiFlashStoreCount, t.TiFlashReplica.Count)
				recorder.AddTable(t.ID, *t.TiFlashReplica)
				needTiFlash = true
			}
		}
	}
	if !needTiFlash {
		log.Info("no need to set tiflash replica, since there is no tables enable tiflash replica")
		return nil
	}
	// we wait for ten minutes to wait tiflash starts.
	// since tiflash only starts when set unmark recovery mode finished.
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	err = utils.WithRetry(timeoutCtx, func() error {
		tiFlashStoreCount, err := rc.getTiFlashNodeCount(ctx)
		log.Info("get tiflash store count for resetting TiFlash Replica",
			zap.Uint64("count", tiFlashStoreCount))
		if err != nil {
			return errors.Trace(err)
		}
		if tiFlashStoreCount < expectTiFlashStoreCount {
			log.Info("still waiting for enough tiflash store start",
				zap.Uint64("expect", expectTiFlashStoreCount),
				zap.Uint64("actual", tiFlashStoreCount),
			)
			return errors.New("tiflash store count is less than expected")
		}
		return nil
	}, &waitTiFlashBackoffer{
		Attempts:    30,
		BaseBackoff: 4 * time.Second,
	})
	if err != nil {
		return err
	}

	sqls := recorder.GenerateResetAlterTableDDLs(info)
	log.Info("Generating SQLs for resetting tiflash replica",
		zap.Strings("sqls", sqls))

	return g.UseOneShotSession(storage, false, func(se glue.Session) error {
		for _, sql := range sqls {
			if errExec := se.ExecuteInternal(ctx, sql); errExec != nil {
				logutil.WarnTerm("Failed to restore tiflash replica config, you may execute the sql restore it manually.",
					logutil.ShortError(errExec),
					zap.String("sql", sql),
				)
			}
		}
		return nil
	})
}

// RangeFilterFromIngestRecorder rewrites the table id of items in the ingestRecorder
// TODO: need to implement the range filter out feature
func (rc *Client) RangeFilterFromIngestRecorder(recorder *ingestrec.IngestRecorder, rewriteRules map[int64]*RewriteRules) error {
	err := recorder.RewriteTableID(func(tableID int64) (int64, bool, error) {
		rewriteRule, exists := rewriteRules[tableID]
		if !exists {
			// since the table's files will be skipped restoring, here also skips.
			return 0, true, nil
		}
		newTableID := GetRewriteTableID(tableID, rewriteRule)
		if newTableID == 0 {
			return 0, false, errors.Errorf("newTableID is 0, tableID: %d", tableID)
		}
		return newTableID, false, nil
	})
	return errors.Trace(err)
	/* TODO: we can use range filter to skip restoring the index kv using accelerated indexing feature
	filter := rtree.NewRangeTree()
	err = recorder.Iterate(func(tableID int64, indexID int64, info *ingestrec.IngestIndexInfo) error {
		// range after table ID rewritten
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		rg := rtree.Range{
			StartKey: codec.EncodeBytes([]byte{}, startKey),
			EndKey:   codec.EncodeBytes([]byte{}, endKey),
		}
		filter.InsertRange(rg)
		return nil
	})
	return errors.Trace(err)
	*/
}

// MockClient create a fake client used to test.
func MockClient(dbs map[string]*metautil.Database) *Client {
	return &Client{databases: dbs}
}

func CheckKeyspaceBREnable(ctx context.Context, pdClient pd.Client) error {
	return version.CheckClusterVersion(ctx, pdClient, version.CheckVersionForKeyspaceBR)
}

func CheckNewCollationEnable(
	backupNewCollationEnable string,
	g glue.Glue,
	storage kv.Storage,
	CheckRequirements bool,
) (bool, error) {
	se, err := g.CreateSession(storage)
	if err != nil {
		return false, errors.Trace(err)
	}

	newCollationEnable, err := se.GetGlobalVariable(utils.GetTidbNewCollationEnabled())
	if err != nil {
		return false, errors.Trace(err)
	}
	// collate.newCollationEnabled is set to 1 when the collate package is initialized,
	// so we need to modify this value according to the config of the cluster
	// before using the collate package.
	enabled := newCollationEnable == "True"
	// modify collate.newCollationEnabled according to the config of the cluster
	collate.SetNewCollationEnabledForTest(enabled)
	log.Info(fmt.Sprintf("set %s", utils.TidbNewCollationEnabled), zap.Bool("new_collation_enabled", enabled))

	if backupNewCollationEnable == "" {
		if CheckRequirements {
			return enabled, errors.Annotatef(berrors.ErrUnknown,
				"the value '%s' not found in backupmeta. "+
					"you can use \"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='%s';\" to manually check the config. "+
					"if you ensure the value '%s' in backup cluster is as same as restore cluster, use --check-requirements=false to skip this check",
				utils.TidbNewCollationEnabled, utils.TidbNewCollationEnabled, utils.TidbNewCollationEnabled)
		}
		log.Warn(fmt.Sprintf("the config '%s' is not in backupmeta", utils.TidbNewCollationEnabled))
		return enabled, nil
	}

	if !strings.EqualFold(backupNewCollationEnable, newCollationEnable) {
		return enabled, errors.Annotatef(berrors.ErrUnknown,
			"the config '%s' not match, upstream:%v, downstream: %v",
			utils.TidbNewCollationEnabled, backupNewCollationEnable, newCollationEnable)
	}

	return enabled, nil
}

type waitTiFlashBackoffer struct {
	Attempts    int
	BaseBackoff time.Duration
}

// NextBackoff returns a duration to wait before retrying again
func (b *waitTiFlashBackoffer) NextBackoff(error) time.Duration {
	bo := b.BaseBackoff
	b.Attempts--
	if b.Attempts == 0 {
		return 0
	}
	b.BaseBackoff *= 2
	if b.BaseBackoff > 32*time.Second {
		b.BaseBackoff = 32 * time.Second
	}
	return bo
}

// Attempt returns the remain attempt times
func (b *waitTiFlashBackoffer) Attempt() int {
	return b.Attempts
}
