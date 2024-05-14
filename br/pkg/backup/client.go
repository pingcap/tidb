// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/conn"
	connutil "github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// ClientMgr manages connections needed by backup.
type ClientMgr interface {
	GetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error)
	ResetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error)
	GetPDClient() pd.Client
	GetLockResolver() *txnlock.LockResolver
	Close()
}

// Checksum is the checksum of some backup files calculated by CollectChecksums.
type Checksum struct {
	Crc64Xor   uint64
	TotalKvs   uint64
	TotalBytes uint64
}

// ProgressUnit represents the unit of progress.
type ProgressUnit string

type StoreBackupPolicy struct {
	One uint64
	All bool
}

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	mgr       ClientMgr
	clusterID uint64

	storage    storage.ExternalStorage
	backend    *backuppb.StorageBackend
	apiVersion kvrpcpb.APIVersion

	cipher           *backuppb.CipherInfo
	checkpointMeta   *checkpoint.CheckpointMetadataForBackup
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.BackupKeyType, checkpoint.BackupValueType]

	gcTTL int64
}

// NewBackupClient returns a new backup client.
func NewBackupClient(ctx context.Context, mgr ClientMgr) *Client {
	log.Info("new backup client")
	pdClient := mgr.GetPDClient()
	clusterID := pdClient.GetClusterID(ctx)
	return &Client{
		clusterID: clusterID,
		mgr:       mgr,

		cipher:           nil,
		checkpointMeta:   nil,
		checkpointRunner: nil,
	}
}

// SetCipher for checkpoint to encrypt sst file's metadata
func (bc *Client) SetCipher(cipher *backuppb.CipherInfo) {
	bc.cipher = cipher
}

// GetCurrentTS gets a new timestamp from PD.
func (bc *Client) GetCurrentTS(ctx context.Context) (uint64, error) {
	p, l, err := bc.mgr.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// GetTS returns the latest timestamp.
func (bc *Client) GetTS(ctx context.Context, duration time.Duration, ts uint64) (uint64, error) {
	var (
		backupTS uint64
		err      error
	)

	if bc.checkpointMeta != nil {
		log.Info("reuse checkpoint BackupTS", zap.Uint64("backup-ts", bc.checkpointMeta.BackupTS))
		return bc.checkpointMeta.BackupTS, nil
	}
	if ts > 0 {
		backupTS = ts
	} else {
		p, l, err := bc.mgr.GetPDClient().GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		backupTS = oracle.ComposeTS(p, l)

		switch {
		case duration < 0:
			return 0, errors.Annotate(berrors.ErrInvalidArgument, "negative timeago is not allowed")
		case duration > 0:
			log.Info("backup time ago", zap.Duration("timeago", duration))

			backupTime := oracle.GetTimeFromTS(backupTS)
			backupAgo := backupTime.Add(-duration)
			if backupTS < oracle.ComposeTS(oracle.GetPhysical(backupAgo), l) {
				return 0, errors.Annotate(berrors.ErrInvalidArgument, "backup ts overflow please choose a smaller timeago")
			}
			backupTS = oracle.ComposeTS(oracle.GetPhysical(backupAgo), l)
		}
	}

	// check backup time do not exceed GCSafePoint
	err = utils.CheckGCSafePoint(ctx, bc.mgr.GetPDClient(), backupTS)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("backup encode timestamp", zap.Uint64("BackupTS", backupTS))
	return backupTS, nil
}

// SetLockFile set write lock file.
func (bc *Client) SetLockFile(ctx context.Context) error {
	return bc.storage.WriteFile(ctx, metautil.LockFile,
		[]byte("DO NOT DELETE\n"+
			"This file exists to remind other backup jobs won't use this path"))
}

// GetSafePointID get the gc-safe-point's service-id from either checkpoint or immediate generation
func (bc *Client) GetSafePointID() string {
	if bc.checkpointMeta != nil {
		log.Info("reuse the checkpoint gc-safepoint service id", zap.String("service-id", bc.checkpointMeta.GCServiceId))
		return bc.checkpointMeta.GCServiceId
	}
	return utils.MakeSafePointID()
}

// SetGCTTL set gcTTL for client.
func (bc *Client) SetGCTTL(ttl int64) {
	if ttl <= 0 {
		ttl = utils.DefaultBRGCSafePointTTL
	}
	bc.gcTTL = ttl
}

// GetGCTTL get gcTTL for this backup.
func (bc *Client) GetGCTTL() int64 {
	return bc.gcTTL
}

// GetStorageBackend gets storage backupend field in client.
func (bc *Client) GetStorageBackend() *backuppb.StorageBackend {
	return bc.backend
}

// GetStorage gets storage for this backup.
func (bc *Client) GetStorage() storage.ExternalStorage {
	return bc.storage
}

// SetStorageAndCheckNotInUse sets ExternalStorage for client and check storage not in used by others.
func (bc *Client) SetStorageAndCheckNotInUse(
	ctx context.Context,
	backend *backuppb.StorageBackend,
	opts *storage.ExternalStorageOptions,
) error {
	err := bc.SetStorage(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}

	// backupmeta already exists
	exist, err := bc.storage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", metautil.MetaFile)
	}
	if exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "backup meta file exists in %v, "+
			"there may be some backup files in the path already, "+
			"please specify a correct backup directory!", bc.storage.URI()+"/"+metautil.MetaFile)
	}
	// use checkpoint mode if checkpoint meta exists
	exist, err = bc.storage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", checkpoint.CheckpointMetaPathForBackup)
	}

	// if there is no checkpoint meta, then checkpoint mode is not used
	// or it is the first execution
	if exist {
		// load the config's hash to keep the config unchanged.
		log.Info("load the checkpoint meta, so the existence of lockfile is allowed.")
		bc.checkpointMeta, err = checkpoint.LoadCheckpointMetadata(ctx, bc.storage)
		if err != nil {
			return errors.Annotatef(err, "error occurred when loading %s file", checkpoint.CheckpointMetaPathForBackup)
		}
	} else {
		err = CheckBackupStorageIsLocked(ctx, bc.storage)
		if err != nil {
			return err
		}
	}

	return nil
}

// CheckCheckpoint check whether the configs are the same
func (bc *Client) CheckCheckpoint(hash []byte) error {
	if bc.checkpointMeta != nil && !bytes.Equal(bc.checkpointMeta.ConfigHash, hash) {
		return errors.Annotatef(berrors.ErrInvalidArgument, "failed to backup to %v, "+
			"because the checkpoint mode is used, "+
			"but the hashs of the configs are not the same. Please check the config",
			bc.storage.URI(),
		)
	}

	// first execution or not using checkpoint mode yet
	// or using the same config can pass the check
	return nil
}

func (bc *Client) GetCheckpointRunner() *checkpoint.CheckpointRunner[checkpoint.BackupKeyType, checkpoint.BackupValueType] {
	return bc.checkpointRunner
}

// StartCheckpointMeta will
// 1. saves the initial status into the external storage;
// 2. load the checkpoint data from external storage
// 3. start checkpoint runner
func (bc *Client) StartCheckpointRunner(
	ctx context.Context,
	cfgHash []byte,
	backupTS uint64,
	ranges []rtree.Range,
	safePointID string,
	progressCallBack func(),
) (err error) {
	if bc.checkpointMeta == nil {
		bc.checkpointMeta = &checkpoint.CheckpointMetadataForBackup{
			GCServiceId: safePointID,
			ConfigHash:  cfgHash,
			BackupTS:    backupTS,
			Ranges:      ranges,
		}

		// sync the checkpoint meta to the external storage at first
		if err := checkpoint.SaveCheckpointMetadata(ctx, bc.storage, bc.checkpointMeta); err != nil {
			return errors.Trace(err)
		}
	} else {
		// otherwise, the checkpoint meta is loaded from the external storage,
		// no need to save it again
		// besides, there are exist checkpoint data need to be loaded before start checkpoint runner
		bc.checkpointMeta.CheckpointDataMap, err = bc.loadCheckpointRanges(ctx, progressCallBack)
		if err != nil {
			return errors.Trace(err)
		}
	}

	bc.checkpointRunner, err = checkpoint.StartCheckpointRunnerForBackup(ctx, bc.storage, bc.cipher, bc.mgr.GetPDClient())
	return errors.Trace(err)
}

func (bc *Client) WaitForFinishCheckpoint(ctx context.Context, flush bool) {
	if bc.checkpointRunner != nil {
		bc.checkpointRunner.WaitForFinish(ctx, flush)
	}
}

// getProgressRange loads the checkpoint(finished) sub-ranges of the current range, and calculate its incompleted sub-ranges.
func (bc *Client) getProgressRange(r rtree.Range) *rtree.ProgressRange {
	// use groupKey to distinguish different ranges
	groupKey := base64.URLEncoding.EncodeToString(r.StartKey)
	if bc.checkpointMeta != nil && len(bc.checkpointMeta.CheckpointDataMap) > 0 {
		rangeTree, exists := bc.checkpointMeta.CheckpointDataMap[groupKey]
		if exists {
			incomplete := rangeTree.GetIncompleteRange(r.StartKey, r.EndKey)
			delete(bc.checkpointMeta.CheckpointDataMap, groupKey)
			return &rtree.ProgressRange{
				Res:        rangeTree,
				Incomplete: incomplete,
				Origin:     r,
				GroupKey:   groupKey,
			}
		}
	}

	// the origin range are not recorded in checkpoint
	// return the default progress range
	return &rtree.ProgressRange{
		Res: rtree.NewRangeTree(),
		Incomplete: []rtree.Range{
			r,
		},
		Origin:   r,
		GroupKey: groupKey,
	}
}

// LoadCheckpointRange loads the checkpoint(finished) sub-ranges of the current range, and calculate its incompleted sub-ranges.
func (bc *Client) loadCheckpointRanges(ctx context.Context, progressCallBack func()) (map[string]rtree.RangeTree, error) {
	rangeDataMap := make(map[string]rtree.RangeTree)

	pastDureTime, err := checkpoint.WalkCheckpointFileForBackup(ctx, bc.storage, bc.cipher, func(groupKey string, rg checkpoint.BackupValueType) {
		rangeTree, exists := rangeDataMap[groupKey]
		if !exists {
			rangeTree = rtree.NewRangeTree()
			rangeDataMap[groupKey] = rangeTree
		}
		rangeTree.Put(rg.StartKey, rg.EndKey, rg.Files)
		progressCallBack()
	})

	// we should adjust start-time of the summary to `pastDureTime` earlier
	log.Info("past cost time", zap.Duration("cost", pastDureTime))
	summary.AdjustStartTimeToEarlierTime(pastDureTime)

	return rangeDataMap, errors.Trace(err)
}

// SetStorage sets ExternalStorage for client.
func (bc *Client) SetStorage(
	ctx context.Context,
	backend *backuppb.StorageBackend,
	opts *storage.ExternalStorageOptions,
) error {
	var err error

	bc.backend = backend
	bc.storage, err = storage.New(ctx, backend, opts)
	return errors.Trace(err)
}

// GetClusterID returns the cluster ID of the tidb cluster to backup.
func (bc *Client) GetClusterID() uint64 {
	return bc.clusterID
}

// GetApiVersion sets api version of the TiKV storage
func (bc *Client) GetApiVersion() kvrpcpb.APIVersion {
	return bc.apiVersion
}

// SetApiVersion sets api version of the TiKV storage
func (bc *Client) SetApiVersion(v kvrpcpb.APIVersion) {
	bc.apiVersion = v
}

// Client.BuildBackupRangeAndSchema calls BuildBackupRangeAndSchema,
// if the checkpoint mode is used, return the ranges from checkpoint meta
func (bc *Client) BuildBackupRangeAndSchema(
	storage kv.Storage,
	tableFilter filter.Filter,
	backupTS uint64,
	isFullBackup bool,
) ([]rtree.Range, *Schemas, []*backuppb.PlacementPolicy, error) {
	if bc.checkpointMeta == nil {
		return BuildBackupRangeAndInitSchema(storage, tableFilter, backupTS, isFullBackup, true)
	}
	_, schemas, policies, err := BuildBackupRangeAndInitSchema(storage, tableFilter, backupTS, isFullBackup, false)
	schemas.SetCheckpointChecksum(bc.checkpointMeta.CheckpointChecksum)
	return bc.checkpointMeta.Ranges, schemas, policies, errors.Trace(err)
}

// CheckBackupStorageIsLocked checks whether backups is locked.
// which means we found other backup progress already write
// some data files into the same backup directory or cloud prefix.
func CheckBackupStorageIsLocked(ctx context.Context, s storage.ExternalStorage) error {
	exist, err := s.FileExists(ctx, metautil.LockFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", metautil.LockFile)
	}
	if exist {
		err = s.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
			// should return error to break the walkDir when found lock file and other .sst files.
			if strings.HasSuffix(path, ".sst") {
				return errors.Annotatef(berrors.ErrInvalidArgument, "backup lock file and sst file exist in %v, "+
					"there are some backup files in the path already, but hasn't checkpoint metadata, "+
					"please specify a correct backup directory!", s.URI()+"/"+metautil.LockFile)
			}
			return nil
		})
		return err
	}
	return nil
}

// BuildBackupRangeAndSchema gets KV range and schema of tables.
// KV ranges are separated by Table IDs.
// Also, KV ranges are separated by Index IDs in the same table.
func BuildBackupRangeAndInitSchema(
	storage kv.Storage,
	tableFilter filter.Filter,
	backupTS uint64,
	isFullBackup bool,
	buildRange bool,
) ([]rtree.Range, *Schemas, []*backuppb.PlacementPolicy, error) {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	var policies []*backuppb.PlacementPolicy
	if isFullBackup {
		// according to https://github.com/pingcap/tidb/issues/32290
		// only full backup will record policies in backupMeta.
		policyList, err := m.ListPolicies()
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		policies = make([]*backuppb.PlacementPolicy, 0, len(policies))
		for _, policyInfo := range policyList {
			p, err := json.Marshal(policyInfo)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			policies = append(policies, &backuppb.PlacementPolicy{Info: p})
		}
	}

	ranges := make([]rtree.Range, 0)
	schemasNum := 0
	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	for _, dbInfo := range dbs {
		// skip system databases
		if !tableFilter.MatchSchema(dbInfo.Name.O) || util.IsMemDB(dbInfo.Name.L) || utils.IsTemplateSysDB(dbInfo.Name) {
			continue
		}

		hasTable := false
		err = m.IterTables(dbInfo.ID, func(tableInfo *model.TableInfo) error {
			if tableInfo.Version > version.CURRENT_BACKUP_SUPPORT_TABLE_INFO_VERSION {
				// normally this shouldn't happen in a production env.
				// because we had a unit test to avoid table info version update silencly.
				// and had version check before run backup.
				return errors.Errorf("backup doesn't not support table %s with version %d, maybe try a new version of br",
					tableInfo.Name.String(),
					tableInfo.Version,
				)
			}
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				return nil
			}

			schemasNum += 1
			hasTable = true
			if buildRange {
				tableRanges, err := distsql.BuildTableRanges(tableInfo)
				if err != nil {
					return errors.Trace(err)
				}
				for _, r := range tableRanges {
					// Add keyspace prefix to BackupRequest
					startKey, endKey := storage.GetCodec().EncodeRange(r.StartKey, r.EndKey)
					ranges = append(ranges, rtree.Range{
						StartKey: startKey,
						EndKey:   endKey,
					})
				}
			}

			return nil
		})

		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if !hasTable {
			log.Info("backup empty database", zap.Stringer("db", dbInfo.Name))
			schemasNum += 1
		}
	}

	if schemasNum == 0 {
		log.Info("nothing to backup")
		return nil, nil, nil, nil
	}
	return ranges, NewBackupSchemas(func(storage kv.Storage, fn func(*model.DBInfo, *model.TableInfo)) error {
		return BuildBackupSchemas(storage, tableFilter, backupTS, isFullBackup, func(dbInfo *model.DBInfo, tableInfo *model.TableInfo) {
			fn(dbInfo, tableInfo)
		})
	}, schemasNum), policies, nil
}

func BuildBackupSchemas(
	storage kv.Storage,
	tableFilter filter.Filter,
	backupTS uint64,
	isFullBackup bool,
	fn func(dbInfo *model.DBInfo, tableInfo *model.TableInfo),
) error {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	dbs, err := m.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, dbInfo := range dbs {
		// skip system databases
		if !tableFilter.MatchSchema(dbInfo.Name.O) || util.IsMemDB(dbInfo.Name.L) || utils.IsTemplateSysDB(dbInfo.Name) {
			continue
		}

		if !isFullBackup {
			// according to https://github.com/pingcap/tidb/issues/32290.
			// ignore placement policy when not in full backup
			dbInfo.PlacementPolicyRef = nil
		}

		hasTable := false
		err = m.IterTables(dbInfo.ID, func(tableInfo *model.TableInfo) error {
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				return nil
			}

			logger := log.L().With(
				zap.String("db", dbInfo.Name.O),
				zap.String("table", tableInfo.Name.O),
			)

			autoIDAccess := m.GetAutoIDAccessors(dbInfo.ID, tableInfo.ID)

			var globalAutoID int64
			switch {
			case tableInfo.IsSequence():
				globalAutoID, err = autoIDAccess.SequenceValue().Get()
			case tableInfo.IsView() || !utils.NeedAutoID(tableInfo):
				// no auto ID for views or table without either rowID nor auto_increment ID.
			default:
				if tableInfo.SepAutoInc() {
					globalAutoID, err = autoIDAccess.IncrementID(tableInfo.Version).Get()
					// For a nonclustered table with auto_increment column, both auto_increment_id and _tidb_rowid are required.
					// See also https://github.com/pingcap/tidb/issues/46093
					if rowID, err1 := autoIDAccess.RowID().Get(); err1 == nil {
						tableInfo.AutoIncIDExtra = rowID + 1
					} else {
						// It is possible that the rowid meta key does not exist (i.e. table have auto_increment_id but no _rowid),
						// so err1 != nil might be expected.
						if globalAutoID == 0 {
							// When both auto_increment_id and _rowid are missing, it must be something wrong.
							return errors.Trace(err1)
						}
						// Print a warning in other scenes, should it be a INFO log?
						log.Warn("get rowid error", zap.Error(err1))
					}
				} else {
					globalAutoID, err = autoIDAccess.RowID().Get()
				}
			}
			if err != nil {
				return errors.Trace(err)
			}
			tableInfo.AutoIncID = globalAutoID + 1
			if !isFullBackup {
				// according to https://github.com/pingcap/tidb/issues/32290.
				// ignore placement policy when not in full backup
				tableInfo.ClearPlacement()
			}

			// Treat cached table as normal table.
			tableInfo.TableCacheStatusType = model.TableCacheStatusDisable

			if tableInfo.ContainsAutoRandomBits() {
				// this table has auto_random id, we need backup and rebase in restoration
				var globalAutoRandID int64
				globalAutoRandID, err = autoIDAccess.RandomID().Get()
				if err != nil {
					return errors.Trace(err)
				}
				tableInfo.AutoRandID = globalAutoRandID + 1
				logger.Debug("change table AutoRandID",
					zap.Int64("AutoRandID", globalAutoRandID))
			}
			logger.Debug("change table AutoIncID",
				zap.Int64("AutoIncID", globalAutoID))

			// remove all non-public indices
			n := 0
			for _, index := range tableInfo.Indices {
				if index.State == model.StatePublic {
					tableInfo.Indices[n] = index
					n++
				}
			}
			tableInfo.Indices = tableInfo.Indices[:n]

			fn(dbInfo, tableInfo)
			hasTable = true

			return nil
		})

		if err != nil {
			return errors.Trace(err)
		}

		if !hasTable {
			log.Info("backup empty database", zap.Stringer("db", dbInfo.Name))
			fn(dbInfo, nil)
		}
	}

	return nil
}

// BuildFullSchema builds a full backup schemas for databases and tables.
func BuildFullSchema(storage kv.Storage, backupTS uint64, fn func(dbInfo *model.DBInfo, tableInfo *model.TableInfo)) error {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	dbs, err := m.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		hasTable := false
		err = m.IterTables(db.ID, func(table *model.TableInfo) error {
			// add table
			fn(db, table)
			hasTable = true
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		// backup this empty db if this schema is empty.
		if !hasTable {
			fn(db, nil)
		}
	}

	return nil
}

func skipUnsupportedDDLJob(job *model.Job) bool {
	switch job.Type {
	// TiDB V5.3.0 supports TableAttributes and TablePartitionAttributes.
	// Backup guarantees data integrity but region placement, which is out of scope of backup
	case model.ActionCreatePlacementPolicy,
		model.ActionAlterPlacementPolicy,
		model.ActionDropPlacementPolicy,
		model.ActionAlterTablePartitionPlacement,
		model.ActionModifySchemaDefaultPlacement,
		model.ActionAlterTablePlacement,
		model.ActionAlterTableAttributes,
		model.ActionAlterTablePartitionAttributes:
		return true
	default:
		return false
	}
}

// WriteBackupDDLJobs sends the ddl jobs are done in (lastBackupTS, backupTS] to metaWriter.
func WriteBackupDDLJobs(metaWriter *metautil.MetaWriter, g glue.Glue, store kv.Storage, lastBackupTS, backupTS uint64, needDomain bool) error {
	snapshot := store.GetSnapshot(kv.NewVersion(backupTS))
	snapMeta := meta.NewSnapshotMeta(snapshot)
	lastSnapshot := store.GetSnapshot(kv.NewVersion(lastBackupTS))
	lastSnapMeta := meta.NewSnapshotMeta(lastSnapshot)
	lastSchemaVersion, err := lastSnapMeta.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return errors.Trace(err)
	}
	backupSchemaVersion, err := snapMeta.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return errors.Trace(err)
	}

	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	newestMeta := meta.NewSnapshotMeta(store.GetSnapshot(kv.NewVersion(version.Ver)))
	allJobs := make([]*model.Job, 0)
	err = g.UseOneShotSession(store, !needDomain, func(se glue.Session) error {
		allJobs, err = ddl.GetAllDDLJobs(se.GetSessionCtx())
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("get all jobs", zap.Int("jobs", len(allJobs)))
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	historyJobs, err := ddl.GetAllHistoryDDLJobs(newestMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("get history jobs", zap.Int("jobs", len(historyJobs)))
	allJobs = append(allJobs, historyJobs...)

	count := 0
	for _, job := range allJobs {
		if skipUnsupportedDDLJob(job) {
			continue
		}

		if (job.State == model.JobStateDone || job.State == model.JobStateSynced) &&
			(job.BinlogInfo != nil && job.BinlogInfo.SchemaVersion > lastSchemaVersion && job.BinlogInfo.SchemaVersion <= backupSchemaVersion) {
			if job.BinlogInfo.DBInfo != nil {
				// ignore all placement policy info during incremental backup for now.
				job.BinlogInfo.DBInfo.PlacementPolicyRef = nil
			}
			if job.BinlogInfo.TableInfo != nil {
				// ignore all placement policy info during incremental backup for now.
				job.BinlogInfo.TableInfo.ClearPlacement()
			}
			jobBytes, err := json.Marshal(job)
			if err != nil {
				return errors.Trace(err)
			}
			err = metaWriter.Send(jobBytes, metautil.AppendDDL)
			if err != nil {
				return errors.Trace(err)
			}
			count++
		}
	}
	log.Debug("get completed jobs", zap.Int("jobs", count))
	return nil
}

func (bc *Client) BuildProgressRangeTree(ranges []rtree.Range) (rtree.ProgressRangeTree, error) {
	// the response from TiKV only contains the region's key, so use the
	// progress range tree to quickly seek the region's corresponding progress range.
	progressRangeTree := rtree.NewProgressRangeTree()
	for _, r := range ranges {
		if err := progressRangeTree.Insert(bc.getProgressRange(r)); err != nil {
			return progressRangeTree, errors.Trace(err)
		}
	}
	return progressRangeTree, nil
}

// BackupRanges make a backup of the given key ranges.
func (bc *Client) BackupRanges(
	ctx context.Context,
	ranges []rtree.Range,
	request backuppb.BackupRequest,
	concurrency uint,
	replicaReadLabel map[string]string,
	metaWriter *metautil.MetaWriter,
	progressCallBack func(),
) error {
	log.Info("Backup Ranges Started", rtree.ZapRanges(ranges))
	init := time.Now()

	defer func() {
		log.Info("Backup Ranges Completed", zap.Duration("take", time.Since(init)))
	}()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.BackupRanges", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	stateChan := make(chan StoreBackupPolicy)
	// TODO implement state change watch goroutine @3pointer
	// go func() {
	// 	// TODO watch changes on cluste state
	// 	cb := storewatch.MakeCallback(storewatch.WithOnReboot(func(s *metapb.Store) {
	// 		stateChan <- StoreBackups{All: true}
	// 	}), storewatch.WithOnDisconnect(func(s *metapb.Store) {
	// 		stateChan <- StoreBackups{All: true}
	// 	}), storewatch.WithOnNewStoreRegistered(func(s *metapb.Store) {
	// 		// only backup for this store
	// 		stateChan <- StoreBackups{One: s.Id}
	// 	}))
	// 	watcher := storewatch.New(bc.mgr.GetPDClient(), cb)
	// 	tick := time.NewTicker(30 * time.Second)
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case <-tick.C:
	// 			err := watcher.Step(ctx)
	// 			if err != nil {
	// 				// ignore it
	// 			}
	// 		}
	// 	}
	// }()

	globalProgressTree, err := bc.BuildProgressRangeTree(ranges)
	if err != nil {
		return errors.Trace(err)
	}

	err = bc.startMainBackupLoop(ctx, &globalProgressTree, replicaReadLabel, request, stateChan, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	return collectRangeFiles(&globalProgressTree, metaWriter)
}

func (bc *Client) getBackupStores(ctx context.Context, replicaReadLabel map[string]string) ([]*metapb.Store, error) {
	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, bc.mgr.GetPDClient(), connutil.SkipTiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var targetStores []*metapb.Store
	targetStoreIds := make(map[uint64]struct{})
	if len(replicaReadLabel) == 0 {
		targetStores = allStores // send backup push down request to all stores
	} else {
		for _, store := range allStores {
			for _, label := range store.Labels {
				if val, ok := replicaReadLabel[label.Key]; ok && val == label.Value {
					targetStores = append(targetStores, store) // send backup push down request to stores that match replica read label
					targetStoreIds[store.GetId()] = struct{}{} // record store id for fine grained backup
				}
			}
		}
	}
	if len(replicaReadLabel) > 0 && len(targetStores) == 0 {
		return nil, errors.Errorf("no store matches replica read label: %v", replicaReadLabel)
	}
	return targetStores, nil
}

func (bc *Client) OnBackupResponse(
	ctx context.Context,
	r *ResponseAndStore,
	errContext *utils.ErrorContext,
	globalProgressTree *rtree.ProgressRangeTree,
) error {
	if r == nil || r.GetResponse() == nil {
		return nil
	}

	resp := r.GetResponse()
	storeID := r.GetStoreID()
	if resp.GetError() == nil {
		pr, err := globalProgressTree.FindContained(resp.StartKey, resp.EndKey)
		if err != nil {
			logutil.CL(ctx).Error("failed to update the backup response",
				zap.Reflect("error", err))
			return err
		}
		if bc.checkpointRunner != nil {
			if err := checkpoint.AppendForBackup(
				ctx,
				bc.checkpointRunner,
				pr.GroupKey,
				resp.StartKey,
				resp.EndKey,
				resp.Files,
			); err != nil {
				// flush checkpoint failed,
				logutil.CL(ctx).Error("failed to flush checkpoint", zap.Error(err))
				return err
			}
		}
		pr.Res.Put(resp.StartKey, resp.EndKey, resp.Files)
		apiVersion := resp.ApiVersion
		bc.SetApiVersion(apiVersion)
	} else {
		errPb := resp.GetError()
		res := errContext.HandleIgnorableError(errPb, storeID)
		switch res.Strategy {
		case utils.GiveUpStrategy:
			errMsg := res.Reason
			if len(errMsg) <= 0 {
				errMsg = errPb.Msg
			}
			// TODO output a precise store address. @3pointer
			return errors.Annotatef(berrors.ErrKVStorage, "error happen in store %v: %s",
				storeID,
				errMsg,
			)
		}
	}
	return nil
}

// infinite loop to backup ranges on all tikv stores
// if one client grpc disconnected. resend backup request to this store.
// if new tikv store joined. send backup request to new store.
// if one tikv store rebooted. consider leader changes, resend backup request to all stores.
// if one tikv store disconnected. consider leader changes, resend backup request to all stores.
func (bc *Client) startMainBackupLoop(
	ctx context.Context,
	globalProgressTree *rtree.ProgressRangeTree,
	replicaReadLabel map[string]string,
	request backuppb.BackupRequest,
	stateChan chan StoreBackupPolicy,
	progressCallBack func(),
) error {
	startStoreBackupAsyncFn := func(
		ctx context.Context,
		round uint64,
		storeID uint64,
		request backuppb.BackupRequest,
		cli backuppb.BackupClient,
		respCh chan *ResponseAndStore,
	) {
		go func() {
			defer func() {
				logutil.CL(ctx).Info("exit store backup goroutine", zap.Uint64("store", storeID))
				close(respCh)
			}()
			err := startStoreBackup(ctx, storeID, request, cli, respCh)
			if err != nil {
				// only 2 kinds of errors will occur here.
				// 1. grpc connection error(already retry inside)
				// 2. context cancelled outside.
				if errors.Cause(err) == context.Canceled {
					logutil.CL(ctx).Info("store backup cancelled",
						zap.Uint64("round", round),
						zap.Uint64("storeID", storeID))
				} else {
					// otherwise retry backup this store
					logutil.CL(ctx).Error("store backup failed",
						zap.Uint64("round", round),
						zap.Uint64("storeID", storeID), zap.Error(err))
					stateChan <- StoreBackupPolicy{One: storeID}
				}
			}
		}()
	}

	collectStoreBackupsAsyncFn := func(
		ctx context.Context,
		round uint64,
		storeBackupChs map[uint64]chan *ResponseAndStore,
		globalCh chan *ResponseAndStore,

	) {
		go func() {
			defer func() {
				logutil.CL(ctx).Info("exit collect backups goroutine", zap.Uint64("round", round))
				close(globalCh)
			}()
			cases := make([]reflect.SelectCase, 0)
			for _, ch := range storeBackupChs {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
			}

			remainingProducers := len(cases)
			logutil.CL(ctx).Info("start wait store backups", zap.Int("remainingProducers", remainingProducers))
			for remainingProducers > 0 {
				chosen, value, ok := reflect.Select(cases)
				if !ok {
					// The chosen channel has been closed, so zero out the channel to disable the case
					cases[chosen].Chan = reflect.ValueOf(nil)
					remainingProducers -= 1
					continue
				}

				select {
				case <-ctx.Done():
					return
				case globalCh <- value.Interface().(*ResponseAndStore):
				}
			}
		}()
	}

	// a flag to indicate the backup round
	// one backup round try to backup all ranges on all tikv stores.
	// ideally, backup should be finished in one round
	// unless the cluster state changed or some kv errors occurred.
	round := uint64(0)
mainLoop:
	for {
		round += 1
		// initialize the error context every round
		errContext := utils.NewErrorContext("MainBackupLoop", 10)

		// a channel to collect all store backup results
		globalBackupResultCh := make(chan *ResponseAndStore)
		// channel slices to receive backup region result from different tikv stores
		storeBackupResultChMap := make(map[uint64]chan *ResponseAndStore)

		// mainCtx used to control mainLoop
		// every round need a new context to control the main backup process
		mainCtx, mainCancel := context.WithCancel(ctx)

		// handleCtx used to control handleLoop
		// every round has another infinite loop to handle all tikv backup responses
		// until backup finished, store state changed or error occurred.
		handleCtx, handleCancel := context.WithCancel(ctx)

		// Compute the left ranges that not backuped yet
		iter := globalProgressTree.Iter()
		inCompleteRanges := iter.GetIncompleteRanges()
		if len(inCompleteRanges) == 0 {
			// all range backuped
			handleCancel()
			mainCancel()
			return nil
		}

		logutil.CL(ctx).Info("backup round start...", zap.Uint64("round", round))

		request.SubRanges = getBackupRanges(inCompleteRanges)

		allStores, err := bc.getBackupStores(mainCtx, replicaReadLabel)
		if err != nil {
			// because we have connectted to pd before.
			// so this error must be retryable, just make infinite retry here
			logutil.CL(mainCtx).Error("failed to get backup stores", zap.Uint64("round", round), zap.Error(err))
			mainCancel()
			continue mainLoop
		}
		for _, store := range allStores {
			if err = utils.CheckStoreLiveness(store); err != nil {
				// skip this store in this round.
				logutil.CL(mainCtx).Warn("store not alive, skip backup it in this round", zap.Uint64("round", round), zap.Error(err))
				continue
			}
			storeID := store.GetId()
			// reset backup client every round, to get a clean grpc connection.
			cli, err := bc.mgr.ResetBackupClient(mainCtx, storeID)
			if err != nil {
				// because the we get store info from pd.
				// there is no customer setting here, so make infinite retry.
				logutil.CL(ctx).Error("failed to reset backup client", zap.Uint64("round", round), zap.Uint64("storeID", storeID), zap.Error(err))
				mainCancel()
				continue mainLoop
			}
			ch := make(chan *ResponseAndStore)
			storeBackupResultChMap[storeID] = ch
			startStoreBackupAsyncFn(mainCtx, round, storeID, request, cli, ch)
		}
		// infinite loop to collect region backup response to global channel
		collectStoreBackupsAsyncFn(handleCtx, round, storeBackupResultChMap, globalBackupResultCh)
	handleLoop:
		for {
			select {
			case <-ctx.Done():
				handleCancel()
				mainCancel()
				return ctx.Err()
			case storeBackupInfo := <-stateChan:
				if storeBackupInfo.All {
					logutil.CL(mainCtx).Info("cluster state changed. restart store backups", zap.Uint64("round", round))
					// stop current connections
					handleCancel()
					mainCancel()
					// start next round backups
					continue mainLoop
				}
				if storeBackupInfo.One != 0 {
					storeID := storeBackupInfo.One
					store, err := bc.mgr.GetPDClient().GetStore(mainCtx, storeID)
					if err != nil {
						// cannot get store, maybe store has scaled-in.
						logutil.CL(mainCtx).Info("cannot get store from pd", zap.Uint64("round", round), zap.Error(err))
						// try next round
						handleCancel()
						mainCancel()
						continue mainLoop
					}
					if err = utils.CheckStoreLiveness(store); err != nil {
						// skip this store in this round.
						logutil.CL(mainCtx).Warn("store not alive, skip backup it in this round", zap.Uint64("round", round), zap.Error(err))
						continue
					}
					// reset backup client. store address could change but store id remained.
					cli, err := bc.mgr.ResetBackupClient(mainCtx, storeID)
					if err != nil {
						logutil.CL(mainCtx).Error("failed to reset backup client", zap.Uint64("round", round), zap.Uint64("storeID", storeID), zap.Error(err))
						handleCancel()
						mainCancel()
						// receive new store info but failed to get backup client.
						// start next round backups to get all tikv stores and reset all client connections.
						continue mainLoop
					}

					// cancel the former collect goroutine
					handleCancel()
					ch := make(chan *ResponseAndStore)

					storeBackupResultChMap[storeID] = ch
					// start backup for this store
					startStoreBackupAsyncFn(mainCtx, round, storeID, request, cli, ch)
					// re-create context for new handler loop
					handleCtx, handleCancel = context.WithCancel(mainCtx)
					// handleCancel makes the former collect goroutine exits
					// so we need to re-create a new channel and restart a new collect goroutine.
					globalBackupResultCh = make(chan *ResponseAndStore)
					// collect all store backup producer channel result to one channel
					collectStoreBackupsAsyncFn(handleCtx, round, storeBackupResultChMap, globalBackupResultCh)
				}
			case respAndStore, ok := <-globalBackupResultCh:
				if !ok {
					// this round backup finished. break and check incomplete ranges in mainLoop.
					break handleLoop
				}
				err = bc.OnBackupResponse(handleCtx, respAndStore, errContext, globalProgressTree)
				if err != nil {
					// if error occurred here, stop the backup process
					// because only 3 kinds of errors will be returned here:
					// 1. permission denied on tikv store.
					// 2. parse backup response error.(shouldn't happen in any case)
					// 3. checkpoint update failed. TODO: should we retry here?
					handleCancel()
					mainCancel()
					return err
				}
				progressCallBack()
			}
		}
	}
}

func collectRangeFiles(progressRangeTree *rtree.ProgressRangeTree, metaWriter *metautil.MetaWriter) error {
	var progressRangeAscendErr error
	progressRangeTree.Ascend(func(progressRange *rtree.ProgressRange) bool {
		var rangeAscendErr error
		progressRange.Res.Ascend(func(i btree.Item) bool {
			r := i.(*rtree.Range)
			for _, f := range r.Files {
				summary.CollectSuccessUnit(summary.TotalKV, 1, f.TotalKvs)
				summary.CollectSuccessUnit(summary.TotalBytes, 1, f.TotalBytes)
			}
			// we need keep the files in order after we support multi_ingest sst.
			// default_sst and write_sst need to be together.
			if err := metaWriter.Send(r.Files, metautil.AppendDataFile); err != nil {
				rangeAscendErr = err
				return false
			}
			return true
		})
		if rangeAscendErr != nil {
			progressRangeAscendErr = rangeAscendErr
			return false
		}

		// Check if there are duplicated files
		checkDupFiles(&progressRange.Res)
		return true
	})

	return errors.Trace(progressRangeAscendErr)
}
