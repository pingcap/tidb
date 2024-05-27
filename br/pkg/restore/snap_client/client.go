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
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
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
	"github.com/pingcap/tidb/br/pkg/restore"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	tidallocdb "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_db"
	tidalloc "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	internalutils "github.com/pingcap/tidb/br/pkg/restore/internal/utils"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/redact"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/keepalive"
)

const (
	strictPlacementPolicyMode = "STRICT"
	ignorePlacementPolicyMode = "IGNORE"

	defaultDDLConcurrency = 16
	maxSplitKeysOnce      = 10240
)

const minBatchDdlSize = 1

type SnapClient struct {
	// Tool clients used by SnapClient
	fileImporter *SnapFileImporter
	pdClient     pd.Client
	pdHTTPClient pdhttp.Client

	// User configurable parameters
	cipher              *backuppb.CipherInfo
	concurrencyPerStore uint
	keepaliveConf       keepalive.ClientParameters
	rateLimit           uint64
	tlsConf             *tls.Config

	switchCh chan struct{}

	storeCount    int
	supportPolicy bool
	workerPool    *tidbutil.WorkerPool

	noSchema        bool
	hasSpeedLimited bool

	databases map[string]*metautil.Database
	ddlJobs   []*model.Job

	// store tables need to rebase info like auto id and random id and so on after create table
	rebasedTablesMap map[restore.UniqueTableName]bool

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
	db *tidallocdb.DB

	// use db pool to speed up restoration in BR binary mode.
	dbPool []*tidallocdb.DB

	dom *domain.Domain

	// correspond to --tidb-placement-mode config.
	// STRICT(default) means policy related SQL can be executed in tidb.
	// IGNORE means policy related SQL will be ignored.
	policyMode string

	// policy name -> policy info
	policyMap *sync.Map

	batchDdlSize uint

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

	// see RestoreCommonConfig.WithSysTable
	withSysTable bool

	// the rewrite mode of the downloaded SST files in TiKV.
	rewriteMode RewriteMode

	// checkpoint information for snapshot restore
	checkpointRunner   *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
	checkpointChecksum map[int64]*checkpoint.ChecksumItem
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	pdClient pd.Client,
	pdHTTPCli pdhttp.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
) *SnapClient {
	return &SnapClient{
		pdClient:      pdClient,
		pdHTTPClient:  pdHTTPCli,
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
		switchCh:      make(chan struct{}),
	}
}

func (rc *SnapClient) closeConn() {
	// rc.db can be nil in raw kv mode.
	if rc.db != nil {
		rc.db.Close()
	}
	for _, db := range rc.dbPool {
		db.Close()
	}
}

// Close a client.
func (rc *SnapClient) Close() {
	// close the connection, and it must be succeed when in SQL mode.
	rc.closeConn()

	if err := rc.fileImporter.Close(); err != nil {
		log.Warn("failed to close file improter")
	}

	log.Info("Restore client closed")
}

func (rc *SnapClient) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

func (rc *SnapClient) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

// GetClusterID gets the cluster id from down-stream cluster.
func (rc *SnapClient) GetClusterID(ctx context.Context) uint64 {
	return rc.pdClient.GetClusterID(ctx)
}

func (rc *SnapClient) GetDomain() *domain.Domain {
	return rc.dom
}

// GetTLSConfig returns the tls config.
func (rc *SnapClient) GetTLSConfig() *tls.Config {
	return rc.tlsConf
}

// GetSupportPolicy tells whether target tidb support placement policy.
func (rc *SnapClient) GetSupportPolicy() bool {
	return rc.supportPolicy
}

func (rc *SnapClient) updateConcurrency() {
	// we believe 32 is large enough for download worker pool.
	// it won't reach the limit if sst files distribute evenly.
	// when restore memory usage is still too high, we should reduce concurrencyPerStore
	// to sarifice some speed to reduce memory usage.
	count := uint(rc.storeCount) * rc.concurrencyPerStore * 32
	log.Info("download coarse worker pool", zap.Uint("size", count))
	rc.workerPool = tidbutil.NewWorkerPool(count, "file")
}

// SetConcurrencyPerStore sets the concurrency of download files for each store.
func (rc *SnapClient) SetConcurrencyPerStore(c uint) {
	log.Info("per-store download worker pool", zap.Uint("size", c))
	rc.concurrencyPerStore = c
}

func (rc *SnapClient) SetBatchDdlSize(batchDdlsize uint) {
	rc.batchDdlSize = batchDdlsize
}

func (rc *SnapClient) GetBatchDdlSize() uint {
	return rc.batchDdlSize
}

func (rc *SnapClient) SetWithSysTable(withSysTable bool) {
	rc.withSysTable = withSysTable
}

// TODO: remove this check and return RewriteModeKeyspace
func (rc *SnapClient) SetRewriteMode(ctx context.Context) {
	if err := version.CheckClusterVersion(ctx, rc.pdClient, version.CheckVersionForKeyspaceBR); err != nil {
		log.Warn("Keyspace BR is not supported in this cluster, fallback to legacy restore", zap.Error(err))
		rc.rewriteMode = RewriteModeLegacy
	} else {
		rc.rewriteMode = RewriteModeKeyspace
	}
}

func (rc *SnapClient) GetRewriteMode() RewriteMode {
	return rc.rewriteMode
}

// SetPlacementPolicyMode to policy mode.
func (rc *SnapClient) SetPlacementPolicyMode(withPlacementPolicy string) {
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

// AllocTableIDs would pre-allocate the table's origin ID if exists, so that the TiKV doesn't need to rewrite the key in
// the download stage.
func (rc *SnapClient) AllocTableIDs(ctx context.Context, tables []*metautil.Table) error {
	preallocedTableIDs := tidalloc.New(tables)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(ctx, rc.GetDomain().Store(), true, func(_ context.Context, txn kv.Transaction) error {
		return preallocedTableIDs.Alloc(meta.NewMeta(txn))
	})
	if err != nil {
		return err
	}

	log.Info("registering the table IDs", zap.Stringer("ids", preallocedTableIDs))
	for i := range rc.dbPool {
		rc.dbPool[i].RegisterPreallocatedIDs(preallocedTableIDs)
	}
	if rc.db != nil {
		rc.db.RegisterPreallocatedIDs(preallocedTableIDs)
	}
	return nil
}

// InitCheckpoint initialize the checkpoint status for the cluster. If the cluster is
// restored for the first time, it will initialize the checkpoint metadata. Otherwrise,
// it will load checkpoint metadata and checkpoint ranges/checksum from the external
// storage.
func (rc *SnapClient) InitCheckpoint(
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

func (rc *SnapClient) WaitForFinishCheckpoint(ctx context.Context, flush bool) {
	if rc.checkpointRunner != nil {
		rc.checkpointRunner.WaitForFinish(ctx, flush)
	}
}

// makeDBPool makes a session pool with specficated size by sessionFactory.
func makeDBPool(size uint, dbFactory func() (*tidallocdb.DB, error)) ([]*tidallocdb.DB, error) {
	dbPool := make([]*tidallocdb.DB, 0, size)
	for i := uint(0); i < size; i++ {
		db, e := dbFactory()
		if e != nil {
			return dbPool, e
		}
		if db != nil {
			dbPool = append(dbPool, db)
		}
	}
	return dbPool, nil
}

// Init create db connection and domain for storage.
func (rc *SnapClient) Init(g glue.Glue, store kv.Storage) error {
	// setDB must happen after set PolicyMode.
	// we will use policyMode to set session variables.
	var err error
	rc.db, rc.supportPolicy, err = tidallocdb.NewDB(g, store, rc.policyMode)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dom, err = g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
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
	rc.dbPool, err = makeDBPool(defaultDDLConcurrency, func() (*tidallocdb.DB, error) {
		db, _, err := tidallocdb.NewDB(g, store, rc.policyMode)
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

func (rc *SnapClient) initClients(ctx context.Context, backend *backuppb.StorageBackend, isRawKvMode bool, isTxnKvMode bool) error {
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, rc.pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Annotate(err, "failed to get stores")
	}
	rc.storeCount = len(stores)
	rc.updateConcurrency()

	var splitClientOpts []split.ClientOptionalParameter
	if isRawKvMode {
		splitClientOpts = append(splitClientOpts, split.WithRawKV())
	}
	metaClient := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, rc.storeCount+1, splitClientOpts...)
	importCli := importclient.NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)
	rc.fileImporter, err = NewSnapFileImporter(ctx, metaClient, importCli, backend, isRawKvMode, isTxnKvMode, stores, rc.rewriteMode, rc.concurrencyPerStore)
	return errors.Trace(err)
}

func (rc *SnapClient) needLoadSchemas(backupMeta *backuppb.BackupMeta) bool {
	return !(backupMeta.IsRawKv || backupMeta.IsTxnKv)
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient.
func (rc *SnapClient) InitBackupMeta(
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
	log.Info("load backupmeta", zap.Int("databases", len(rc.databases)), zap.Int("jobs", len(rc.ddlJobs)))

	return rc.initClients(c, backend, backupMeta.IsRawKv, backupMeta.IsTxnKv)
}

// IsRawKvMode checks whether the backup data is in raw kv format, in which case transactional recover is forbidden.
func (rc *SnapClient) IsRawKvMode() bool {
	return rc.backupMeta.IsRawKv
}

// GetFilesInRawRange gets all files that are in the given range or intersects with the given range.
func (rc *SnapClient) GetFilesInRawRange(startKey []byte, endKey []byte, cf string) ([]*backuppb.File, error) {
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

// ResetTS resets the timestamp of PD to a bigger value.
func (rc *SnapClient) ResetTS(ctx context.Context, pdCtrl *pdutil.PdController) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	return utils.WithRetry(ctx, func() error {
		return pdCtrl.ResetTS(ctx, restoreTS)
	}, utils.NewPDReqBackoffer())
}

// GetDatabases returns all databases.
func (rc *SnapClient) GetDatabases() []*metautil.Database {
	dbs := make([]*metautil.Database, 0, len(rc.databases))
	for _, db := range rc.databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// HasBackedUpSysDB whether we have backed up system tables
// br backs system tables up since 5.1.0
func (rc *SnapClient) HasBackedUpSysDB() bool {
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
func (rc *SnapClient) GetPlacementPolicies() (*sync.Map, error) {
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
func (rc *SnapClient) GetDDLJobs() []*model.Job {
	return rc.ddlJobs
}

// SetPolicyMap set policyMap.
func (rc *SnapClient) SetPolicyMap(p *sync.Map) {
	rc.policyMap = p
}

// CreatePolicies creates all policies in full restore.
func (rc *SnapClient) CreatePolicies(ctx context.Context, policyMap *sync.Map) error {
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

// CreateDatabases creates databases. If the client has the db pool, it would create it.
func (rc *SnapClient) CreateDatabases(ctx context.Context, dbs []*metautil.Database) error {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create database")
		return nil
	}

	if len(rc.dbPool) == 0 {
		log.Info("create databases sequentially")
		for _, db := range dbs {
			err := rc.db.CreateDatabase(ctx, db.Info, rc.supportPolicy, rc.policyMap)
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
			return conn.CreateDatabase(ectx, db.Info, rc.supportPolicy, rc.policyMap)
		})
	}
	return eg.Wait()
}

// generateRebasedTables generate a map[UniqueTableName]bool to represent tables that haven't updated table info.
// there are two situations:
// 1. tables that already exists in the restored cluster.
// 2. tables that are created by executing ddl jobs.
// so, only tables in incremental restoration will be added to the map
func (rc *SnapClient) generateRebasedTables(tables []*metautil.Table) {
	if !rc.IsIncremental() {
		// in full restoration, all tables are created by Session.CreateTable, and all tables' info is updated.
		rc.rebasedTablesMap = make(map[restore.UniqueTableName]bool)
		return
	}

	rc.rebasedTablesMap = make(map[restore.UniqueTableName]bool, len(tables))
	for _, table := range tables {
		rc.rebasedTablesMap[restore.UniqueTableName{DB: table.DB.Name.String(), Table: table.Info.Name.String()}] = true
	}
}

// getRebasedTables returns tables that may need to be rebase auto increment id or auto random id
func (rc *SnapClient) getRebasedTables() map[restore.UniqueTableName]bool {
	return rc.rebasedTablesMap
}

func (rc *SnapClient) createTables(
	ctx context.Context,
	db *tidallocdb.DB,
	tables []*metautil.Table,
	newTS uint64,
) ([]CreatedTable, error) {
	log.Info("client to create tables")
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID")
	} else {
		err := db.CreateTables(ctx, tables, rc.getRebasedTables(), rc.supportPolicy, rc.policyMap)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	cts := make([]CreatedTable, 0, len(tables))
	for _, table := range tables {
		newTableInfo, err := restore.GetTableSchema(rc.dom, table.DB.Name, table.Info.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
			return nil, errors.Annotatef(berrors.ErrRestoreModeMismatch,
				"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
				restore.TransferBoolToValue(table.Info.IsCommonHandle),
				table.Info.IsCommonHandle,
				newTableInfo.IsCommonHandle)
		}
		rules := restoreutils.GetRewriteRules(newTableInfo, table.Info, newTS, true)
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

func (rc *SnapClient) createTablesInWorkerPool(ctx context.Context, tables []*metautil.Table, newTS uint64, outCh chan<- CreatedTable) error {
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
			cts, err := rc.createTables(ectx, db, tableSlice, newTS) // ddl job for [lastSent:i)
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

func (rc *SnapClient) createTable(
	ctx context.Context,
	db *tidallocdb.DB,
	table *metautil.Table,
	newTS uint64,
) (CreatedTable, error) {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID", zap.Stringer("table", table.Info.Name))
	} else {
		err := db.CreateTable(ctx, table, rc.getRebasedTables(), rc.supportPolicy, rc.policyMap)
		if err != nil {
			return CreatedTable{}, errors.Trace(err)
		}
	}
	newTableInfo, err := restore.GetTableSchema(rc.dom, table.DB.Name, table.Info.Name)
	if err != nil {
		return CreatedTable{}, errors.Trace(err)
	}
	if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
		return CreatedTable{}, errors.Annotatef(berrors.ErrRestoreModeMismatch,
			"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
			restore.TransferBoolToValue(table.Info.IsCommonHandle),
			table.Info.IsCommonHandle,
			newTableInfo.IsCommonHandle)
	}
	rules := restoreutils.GetRewriteRules(newTableInfo, table.Info, newTS, true)
	et := CreatedTable{
		RewriteRule: rules,
		Table:       newTableInfo,
		OldTable:    table,
	}
	return et, nil
}

func (rc *SnapClient) createTablesWithSoleDB(ctx context.Context,
	createOneTable func(ctx context.Context, db *tidallocdb.DB, t *metautil.Table) error,
	tables []*metautil.Table) error {
	for _, t := range tables {
		if err := createOneTable(ctx, rc.db, t); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (rc *SnapClient) createTablesWithDBPool(ctx context.Context,
	createOneTable func(ctx context.Context, db *tidallocdb.DB, t *metautil.Table) error,
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

// InitFullClusterRestore init fullClusterRestore and set SkipGrantTable as needed
func (rc *SnapClient) InitFullClusterRestore(explicitFilter bool) {
	rc.fullClusterRestore = !explicitFilter && rc.IsFull()

	log.Info("full cluster restore", zap.Bool("value", rc.fullClusterRestore))
}

func (rc *SnapClient) IsFullClusterRestore() bool {
	return rc.fullClusterRestore
}

// IsFull returns whether this backup is full.
func (rc *SnapClient) IsFull() bool {
	failpoint.Inject("mock-incr-backup-data", func() {
		failpoint.Return(false)
	})
	return !rc.IsIncremental()
}

// IsIncremental returns whether this backup is incremental.
func (rc *SnapClient) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}

// NeedCheckFreshCluster is every time. except restore from a checkpoint or user has not set filter argument.
func (rc *SnapClient) NeedCheckFreshCluster(ExplicitFilter bool, firstRun bool) bool {
	return rc.IsFull() && !ExplicitFilter && firstRun
}

// EnableSkipCreateSQL sets switch of skip create schema and tables.
func (rc *SnapClient) EnableSkipCreateSQL() {
	rc.noSchema = true
}

// IsSkipCreateSQL returns whether we need skip create schema and tables in restore.
func (rc *SnapClient) IsSkipCreateSQL() bool {
	return rc.noSchema
}

// CheckTargetClusterFresh check whether the target cluster is fresh or not
// if there's no user dbs or tables, we take it as a fresh cluster, although
// user may have created some users or made other changes.
func (rc *SnapClient) CheckTargetClusterFresh(ctx context.Context) error {
	log.Info("checking whether target cluster is fresh")
	userDBs := restore.GetExistedUserDBs(rc.dom)
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

// ExecDDLs executes the queries of the ddl jobs.
func (rc *SnapClient) ExecDDLs(ctx context.Context, ddlJobs []*model.Job) error {
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

func (rc *SnapClient) ResetSpeedLimit(ctx context.Context) error {
	rc.hasSpeedLimited = false
	err := rc.setSpeedLimit(ctx, 0)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *SnapClient) setSpeedLimit(ctx context.Context, rateLimit uint64) error {
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
					err := rc.fileImporter.SetDownloadSpeedLimit(ectx, finalStore.GetId(), rateLimit)
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

// RestoreSSTFiles tries to restore the files.
func (rc *SnapClient) RestoreSSTFiles(
	ctx context.Context,
	tableIDWithFiles []TableIDWithFiles,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	fileCount := 0
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore files", zap.Duration("take", elapsed))
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

	var rangeFiles []*backuppb.File
	var leftFiles []*backuppb.File
LOOPFORTABLE:
	for _, tableIDWithFile := range tableIDWithFiles {
		tableID := tableIDWithFile.TableID
		files := tableIDWithFile.Files
		rules := tableIDWithFile.RewriteRules
		fileCount += len(files)
		for rangeFiles, leftFiles = drainFilesByRange(files); len(rangeFiles) != 0; rangeFiles, leftFiles = drainFilesByRange(leftFiles) {
			if ectx.Err() != nil {
				log.Warn("Restoring encountered error and already stopped, give up remained files.",
					zap.Int("remained", len(leftFiles)),
					logutil.ShortError(ectx.Err()))
				// We will fetch the error from the errgroup then (If there were).
				// Also note if the parent context has been canceled or something,
				// breaking here directly is also a reasonable behavior.
				break LOOPFORTABLE
			}
			filesReplica := rangeFiles
			rc.fileImporter.WaitUntilUnblock()
			rc.workerPool.ApplyOnErrorGroup(eg, func() (restoreErr error) {
				fileStart := time.Now()
				defer func() {
					if restoreErr == nil {
						log.Info("import files done", logutil.Files(filesReplica),
							zap.Duration("take", time.Since(fileStart)))
						updateCh.Inc()
					}
				}()
				if importErr := rc.fileImporter.ImportSSTFiles(ectx, filesReplica, rules, rc.cipher, rc.dom.Store().GetCodec().GetAPIVersion()); importErr != nil {
					return errors.Trace(importErr)
				}

				// the data of this range has been import done
				if rc.checkpointRunner != nil && len(filesReplica) > 0 {
					rangeKey := getFileRangeKey(filesReplica[0].Name)
					// The checkpoint range shows this ranges of kvs has been restored into
					// the table corresponding to the table-id.
					if err := checkpoint.AppendRangesForRestore(ectx, rc.checkpointRunner, tableID, rangeKey); err != nil {
						return errors.Trace(err)
					}
				}
				return nil
			})
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

func (rc *SnapClient) execChecksum(
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
		startTS, err := restore.GetTSWithRetry(ctx, rc.pdClient)
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

func (rc *SnapClient) WaitForFilesRestored(ctx context.Context, files []*backuppb.File, updateCh glue.Progress) error {
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
				return rc.fileImporter.ImportSSTFiles(ectx, []*backuppb.File{fileReplica}, restoreutils.EmptyRewriteRule(), rc.cipher, rc.backupMeta.ApiVersion)
			})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *SnapClient) RestoreRaw(
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

// SplitRanges implements TiKVRestorer. It splits region by
// data range after rewrite.
func (rc *SnapClient) SplitRanges(
	ctx context.Context,
	ranges []rtree.Range,
	updateCh glue.Progress,
	isRawKv bool,
) error {
	splitClientOpts := make([]split.ClientOptionalParameter, 0, 2)
	splitClientOpts = append(splitClientOpts, split.WithOnSplit(func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	}))
	if isRawKv {
		splitClientOpts = append(splitClientOpts, split.WithRawKV())
	}

	splitter := internalutils.NewRegionSplitter(split.NewClient(
		rc.pdClient,
		rc.pdHTTPClient,
		rc.tlsConf,
		maxSplitKeysOnce,
		rc.storeCount+1,
		splitClientOpts...,
	))

	return splitter.ExecuteSplit(ctx, ranges)
}
