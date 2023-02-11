// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	backupFineGrainedMaxBackoff = 80000
	backupRetryTimes            = 5
	// RangeUnit represents the progress updated counter when a range finished.
	RangeUnit ProgressUnit = "range"
	// RegionUnit represents the progress updated counter when a region finished.
	RegionUnit ProgressUnit = "region"
)

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	mgr       ClientMgr
	clusterID uint64

	storage    storage.ExternalStorage
	backend    *backuppb.StorageBackend
	apiVersion kvrpcpb.APIVersion

	cipher           *backuppb.CipherInfo
	checkpointMeta   *checkpoint.CheckpointMetadata
	checkpointRunner *checkpoint.CheckpointRunner

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

// GetTS gets a new timestamp from PD.
func (bc *Client) GetCurerntTS(ctx context.Context) (uint64, error) {
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
	exist, err = bc.storage.FileExists(ctx, checkpoint.CheckpointMetaPath)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", checkpoint.CheckpointMetaPath)
	}

	// if there is no checkpoint meta, then checkpoint mode is not used
	// or it is the first execution
	if exist {
		// load the config's hash to keep the config unchanged.
		log.Info("load the checkpoint meta, so the existence of lockfile is allowed.")
		bc.checkpointMeta, err = checkpoint.LoadCheckpointMetadata(ctx, bc.storage)
		if err != nil {
			return errors.Annotatef(err, "error occurred when loading %s file", checkpoint.CheckpointMetaPath)
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

func (bc *Client) GetCheckpointRunner() *checkpoint.CheckpointRunner {
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
	progressCallBack func(ProgressUnit),
) (err error) {
	if bc.checkpointMeta == nil {
		bc.checkpointMeta = &checkpoint.CheckpointMetadata{
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

	bc.checkpointRunner, err = checkpoint.StartCheckpointRunner(ctx, bc.storage, bc.cipher, bc.mgr.GetPDClient())
	return errors.Trace(err)
}

func (bc *Client) WaitForFinishCheckpoint(ctx context.Context) {
	if bc.checkpointRunner != nil {
		bc.checkpointRunner.WaitForFinish(ctx)
	}
}

// GetProgressRange loads the checkpoint(finished) sub-ranges of the current range, and calculate its incompleted sub-ranges.
func (bc *Client) GetProgressRange(r rtree.Range) (*rtree.ProgressRange, error) {
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
			}, nil
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
	}, nil
}

// LoadCheckpointRange loads the checkpoint(finished) sub-ranges of the current range, and calculate its incompleted sub-ranges.
func (bc *Client) loadCheckpointRanges(ctx context.Context, progressCallBack func(ProgressUnit)) (map[string]rtree.RangeTree, error) {
	rangeDataMap := make(map[string]rtree.RangeTree)

	pastDureTime, err := checkpoint.WalkCheckpointFile(ctx, bc.storage, bc.cipher, func(groupKey string, rg *rtree.Range) {
		rangeTree, exists := rangeDataMap[groupKey]
		if !exists {
			rangeTree = rtree.NewRangeTree()
			rangeDataMap[groupKey] = rangeTree
		}
		rangeTree.Put(rg.StartKey, rg.EndKey, rg.Files)
		progressCallBack(RegionUnit)
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
		return BuildBackupRangeAndSchema(storage, tableFilter, backupTS, isFullBackup, true)
	}
	_, schemas, policies, err := BuildBackupRangeAndSchema(storage, tableFilter, backupTS, isFullBackup, false)
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

// BuildTableRanges returns the key ranges encompassing the entire table,
// and its partitions if exists.
func BuildTableRanges(tbl *model.TableInfo) ([]kv.KeyRange, error) {
	pis := tbl.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		return appendRanges(tbl, tbl.ID)
	}

	ranges := make([]kv.KeyRange, 0, len(pis.Definitions)*(len(tbl.Indices)+1)+1)
	for _, def := range pis.Definitions {
		rgs, err := appendRanges(tbl, def.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ranges = append(ranges, rgs...)
	}
	return ranges, nil
}

func appendRanges(tbl *model.TableInfo, tblID int64) ([]kv.KeyRange, error) {
	var ranges []*ranger.Range
	if tbl.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	retRanges := make([]kv.KeyRange, 0, 1+len(tbl.Indices))
	kvRanges, err := distsql.TableHandleRangesToKVRanges(nil, []int64{tblID}, tbl.IsCommonHandle, ranges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	retRanges = kvRanges.AppendSelfTo(retRanges)

	for _, index := range tbl.Indices {
		if index.State != model.StatePublic {
			continue
		}
		ranges = ranger.FullRange()
		idxRanges, err := distsql.IndexRangesToKVRanges(nil, tblID, index.ID, ranges, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = idxRanges.AppendSelfTo(retRanges)
	}
	return retRanges, nil
}

// BuildBackupRangeAndSchema gets KV range and schema of tables.
// KV ranges are separated by Table IDs.
// Also, KV ranges are separated by Index IDs in the same table.
func BuildBackupRangeAndSchema(
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
	backupSchemas := NewBackupSchemas()
	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	for _, dbInfo := range dbs {
		// skip system databases
		if !tableFilter.MatchSchema(dbInfo.Name.O) || util.IsMemDB(dbInfo.Name.L) || utils.IsTemplateSysDB(dbInfo.Name) {
			continue
		}

		tables, err := m.ListTables(dbInfo.ID)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if len(tables) == 0 {
			log.Info("backup empty database", zap.Stringer("db", dbInfo.Name))
			backupSchemas.AddSchema(dbInfo, nil)
			continue
		}

		if !isFullBackup {
			// according to https://github.com/pingcap/tidb/issues/32290.
			// ignore placement policy when not in full backup
			dbInfo.PlacementPolicyRef = nil
		}

		for _, tableInfo := range tables {
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				continue
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
				globalAutoID, err = autoIDAccess.RowID().Get()
			}
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			tableInfo.AutoIncID = globalAutoID + 1
			if !isFullBackup {
				// according to https://github.com/pingcap/tidb/issues/32290.
				// ignore placement policy when not in full backup
				tableInfo.ClearPlacement()
			}

			// Treat cached table as normal table.
			tableInfo.TableCacheStatusType = model.TableCacheStatusDisable

			if tableInfo.PKIsHandle && tableInfo.ContainsAutoRandomBits() {
				// this table has auto_random id, we need backup and rebase in restoration
				var globalAutoRandID int64
				globalAutoRandID, err = autoIDAccess.RandomID().Get()
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
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

			backupSchemas.AddSchema(dbInfo, tableInfo)

			if buildRange {
				tableRanges, err := BuildTableRanges(tableInfo)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				for _, r := range tableRanges {
					ranges = append(ranges, rtree.Range{
						StartKey: r.StartKey,
						EndKey:   r.EndKey,
					})
				}
			}
		}
	}

	if backupSchemas.Len() == 0 {
		log.Info("nothing to backup")
		return nil, nil, nil, nil
	}
	return ranges, backupSchemas, policies, nil
}

// BuildFullSchema builds a full backup schemas for databases and tables.
func BuildFullSchema(storage kv.Storage, backupTS uint64) (*Schemas, error) {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	newBackupSchemas := NewBackupSchemas()
	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, db := range dbs {
		tables, err := m.ListTables(db.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// backup this empty db if this schema is empty.
		if len(tables) == 0 {
			newBackupSchemas.AddSchema(db, nil)
		}

		for _, table := range tables {
			// add table
			newBackupSchemas.AddSchema(db, table)
		}
	}

	return newBackupSchemas, nil
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
		allJobs, err = ddl.GetAllDDLJobs(se.GetSessionCtx(), newestMeta)
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

// BackupRanges make a backup of the given key ranges.
func (bc *Client) BackupRanges(
	ctx context.Context,
	ranges []rtree.Range,
	request backuppb.BackupRequest,
	concurrency uint,
	replicaReadLabel map[string]string,
	metaWriter *metautil.MetaWriter,
	progressCallBack func(ProgressUnit),
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

	// we collect all files in a single goroutine to avoid thread safety issues.
	workerPool := utils.NewWorkerPool(concurrency, "Ranges")
	eg, ectx := errgroup.WithContext(ctx)
	for id, r := range ranges {
		id := id
		req := request
		req.StartKey, req.EndKey = r.StartKey, r.EndKey
		pr, err := bc.GetProgressRange(r)
		if err != nil {
			return errors.Trace(err)
		}
		workerPool.ApplyOnErrorGroup(eg, func() error {
			elctx := logutil.ContextWithField(ectx, logutil.RedactAny("range-sn", id))
			err := bc.BackupRange(elctx, req, replicaReadLabel, pr, metaWriter, progressCallBack)
			if err != nil {
				// The error due to context cancel, stack trace is meaningless, the stack shall be suspended (also clear)
				if errors.Cause(err) == context.Canceled {
					return errors.SuspendStack(err)
				}
				return errors.Trace(err)
			}
			return nil
		})
	}

	return eg.Wait()
}

// BackupRange make a backup of the given key range.
// Returns an array of files backed up.
func (bc *Client) BackupRange(
	ctx context.Context,
	request backuppb.BackupRequest,
	replicaReadLabel map[string]string,
	progressRange *rtree.ProgressRange,
	metaWriter *metautil.MetaWriter,
	progressCallBack func(ProgressUnit),
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		logutil.CL(ctx).Info("backup range completed",
			logutil.Key("startKey", progressRange.Origin.StartKey), logutil.Key("endKey", progressRange.Origin.EndKey),
			zap.Duration("take", elapsed))
		key := "range start:" + hex.EncodeToString(progressRange.Origin.StartKey) + " end:" + hex.EncodeToString(progressRange.Origin.EndKey)
		if err != nil {
			summary.CollectFailureUnit(key, err)
		}
	}()
	logutil.CL(ctx).Info("backup range started",
		logutil.Key("startKey", progressRange.Origin.StartKey), logutil.Key("endKey", progressRange.Origin.EndKey),
		zap.Uint64("rateLimit", request.RateLimit),
		zap.Uint32("concurrency", request.Concurrency))

	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, bc.mgr.GetPDClient(), connutil.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
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
		return errors.Errorf("no store matches replica read label: %v", replicaReadLabel)
	}

	logutil.CL(ctx).Info("backup push down started")
	// either the `incomplete` is origin range itself,
	// or the `incomplete` is sub-ranges split by checkpoint of origin range
	if len(progressRange.Incomplete) > 0 {
		// don't make the origin request dirty,
		// since fineGrainedBackup need to use it.
		req := request
		if len(progressRange.Incomplete) > 1 {
			subRanges := make([]*kvrpcpb.KeyRange, 0, len(progressRange.Incomplete))
			for _, r := range progressRange.Incomplete {
				subRanges = append(subRanges, &kvrpcpb.KeyRange{
					StartKey: r.StartKey,
					EndKey:   r.EndKey,
				})
			}
			req.SubRanges = subRanges
		} else {
			// compatible with older version of TiKV
			req.StartKey = progressRange.Incomplete[0].StartKey
			req.EndKey = progressRange.Incomplete[0].EndKey
		}

		push := newPushDown(bc.mgr, len(targetStores))
		err = push.pushBackup(ctx, req, progressRange, targetStores, bc.checkpointRunner, progressCallBack)
		if err != nil {
			return errors.Trace(err)
		}
	}
	logutil.CL(ctx).Info("backup push down completed", zap.Int("small-range-count", progressRange.Res.Len()))

	// Find and backup remaining ranges.
	// TODO: test fine grained backup.
	if err := bc.fineGrainedBackup(ctx, request, targetStoreIds, progressRange, progressCallBack); err != nil {
		return errors.Trace(err)
	}

	// update progress of range unit
	progressCallBack(RangeUnit)

	if request.IsRawKv {
		logutil.CL(ctx).Info("raw ranges backed up",
			logutil.Key("startKey", progressRange.Origin.StartKey),
			logutil.Key("endKey", progressRange.Origin.EndKey),
			zap.String("cf", request.Cf))
	} else {
		logutil.CL(ctx).Info("transactional range backup completed",
			zap.Reflect("StartTS", request.StartVersion),
			zap.Reflect("EndTS", request.EndVersion))
	}

	var ascendErr error
	progressRange.Res.Ascend(func(i btree.Item) bool {
		r := i.(*rtree.Range)
		for _, f := range r.Files {
			summary.CollectSuccessUnit(summary.TotalKV, 1, f.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, 1, f.TotalBytes)
		}
		// we need keep the files in order after we support multi_ingest sst.
		// default_sst and write_sst need to be together.
		if err := metaWriter.Send(r.Files, metautil.AppendDataFile); err != nil {
			ascendErr = err
			return false
		}
		return true
	})
	if ascendErr != nil {
		return errors.Trace(ascendErr)
	}

	// Check if there are duplicated files.
	checkDupFiles(&progressRange.Res)

	return nil
}

func (bc *Client) findTargetPeer(ctx context.Context, key []byte, isRawKv bool, targetStoreIds map[uint64]struct{}) (*metapb.Peer, error) {
	// Keys are saved in encoded format in TiKV, so the key must be encoded
	// in order to find the correct region.
	key = codec.EncodeBytesExt([]byte{}, key, isRawKv)
	for i := 0; i < 5; i++ {
		// better backoff.
		region, err := bc.mgr.GetPDClient().GetRegion(ctx, key)
		if err != nil || region == nil {
			log.Error("find region failed", zap.Error(err), zap.Reflect("region", region))
			time.Sleep(time.Millisecond * time.Duration(100*i))
			continue
		}
		if len(targetStoreIds) == 0 {
			if region.Leader != nil {
				log.Info("find leader",
					zap.Reflect("Leader", region.Leader), logutil.Key("key", key))
				return region.Leader, nil
			}
		} else {
			candidates := make([]*metapb.Peer, 0, len(region.Meta.Peers))
			for _, peer := range region.Meta.Peers {
				if _, ok := targetStoreIds[peer.StoreId]; ok {
					candidates = append(candidates, peer)
				}
			}
			if len(candidates) > 0 {
				peer := candidates[rand.Intn(len(candidates))]
				log.Info("find target peer for backup",
					zap.Reflect("Peer", peer), logutil.Key("key", key))
				return peer, nil
			}
		}

		log.Warn("fail to find a target peer", logutil.Key("key", key))
		time.Sleep(time.Millisecond * time.Duration(1000*i))
		continue
	}
	log.Error("can not find a valid target peer", logutil.Key("key", key))
	if len(targetStoreIds) == 0 {
		return nil, errors.Annotatef(berrors.ErrBackupNoLeader, "can not find a valid leader for key %s", key)
	}
	return nil, errors.Errorf("can not find a valid target peer for key %s", key)
}

func (bc *Client) fineGrainedBackup(
	ctx context.Context,
	req backuppb.BackupRequest,
	targetStoreIds map[uint64]struct{},
	pr *rtree.ProgressRange,
	progressCallBack func(ProgressUnit),
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.fineGrainedBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	failpoint.Inject("hint-fine-grained-backup", func(v failpoint.Value) {
		log.Info("failpoint hint-fine-grained-backup injected, "+
			"process will sleep for 3s and notify the shell.", zap.String("file", v.(string)))
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
			time.Sleep(3 * time.Second)
		}
	})

	bo := tikv.NewBackoffer(ctx, backupFineGrainedMaxBackoff)
	for {
		// Step1, check whether there is any incomplete range
		incomplete := pr.Res.GetIncompleteRange(req.StartKey, req.EndKey)
		if len(incomplete) == 0 {
			return nil
		}
		logutil.CL(ctx).Info("start fine grained backup", zap.Int("incomplete", len(incomplete)))
		// Step2, retry backup on incomplete range
		respCh := make(chan *backuppb.BackupResponse, 4)
		errCh := make(chan error, 4)
		retry := make(chan rtree.Range, 4)

		max := &struct {
			ms int
			mu sync.Mutex
		}{}
		wg := new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			fork, _ := bo.Fork()
			go func(boFork *tikv.Backoffer) {
				defer wg.Done()
				for rg := range retry {
					subReq := req
					subReq.StartKey, subReq.EndKey = rg.StartKey, rg.EndKey
					backoffMs, err := bc.handleFineGrained(ctx, boFork, subReq, targetStoreIds, respCh)
					if err != nil {
						errCh <- err
						return
					}
					if backoffMs != 0 {
						max.mu.Lock()
						if max.ms < backoffMs {
							max.ms = backoffMs
						}
						max.mu.Unlock()
					}
				}
			}(fork)
		}

		// Dispatch rangs and wait
		go func() {
			for _, rg := range incomplete {
				retry <- rg
			}
			close(retry)
			wg.Wait()
			close(respCh)
		}()

	selectLoop:
		for {
			select {
			case err := <-errCh:
				// TODO: should we handle err here?
				return errors.Trace(err)
			case resp, ok := <-respCh:
				if !ok {
					// Finished.
					break selectLoop
				}
				if resp.Error != nil {
					logutil.CL(ctx).Panic("unexpected backup error",
						zap.Reflect("error", resp.Error))
				}
				logutil.CL(ctx).Info("put fine grained range",
					logutil.Key("fine-grained-range-start", resp.StartKey),
					logutil.Key("fine-grained-range-end", resp.EndKey),
				)
				if bc.checkpointRunner != nil {
					if err := bc.checkpointRunner.Append(
						ctx,
						pr.GroupKey,
						resp.StartKey,
						resp.EndKey,
						resp.Files,
					); err != nil {
						return errors.Annotate(err, "failed to flush checkpoint when fineGrainedBackup")
					}
				}
				pr.Res.Put(resp.StartKey, resp.EndKey, resp.Files)
				apiVersion := resp.ApiVersion
				bc.SetApiVersion(apiVersion)

				// Update progress
				progressCallBack(RegionUnit)
			}
		}

		// Step3. Backoff if needed, then repeat.
		max.mu.Lock()
		ms := max.ms
		max.mu.Unlock()
		if ms != 0 {
			log.Info("handle fine grained", zap.Int("backoffMs", ms))
			// TODO: fill a meaningful error.
			err := bo.BackoffWithMaxSleepTxnLockFast(ms, berrors.ErrUnknown)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// OnBackupResponse checks the backup resp, decides whether to retry and generate the error.
func OnBackupResponse(
	storeID uint64,
	bo *tikv.Backoffer,
	backupTS uint64,
	lockResolver *txnlock.LockResolver,
	resp *backuppb.BackupResponse,
) (*backuppb.BackupResponse, int, error) {
	log.Debug("OnBackupResponse", zap.Reflect("resp", resp))
	if resp.Error == nil {
		return resp, 0, nil
	}
	backoffMs := 0
	switch v := resp.Error.Detail.(type) {
	case *backuppb.Error_KvError:
		if lockErr := v.KvError.Locked; lockErr != nil {
			// Try to resolve lock.
			log.Warn("backup occur kv error", zap.Reflect("error", v))
			msBeforeExpired, err1 := lockResolver.ResolveLocks(
				bo, backupTS, []*txnlock.Lock{txnlock.NewLock(lockErr)})
			if err1 != nil {
				return nil, 0, errors.Trace(err1)
			}
			if msBeforeExpired > 0 {
				backoffMs = int(msBeforeExpired)
			}
			return nil, backoffMs, nil
		}
		// Backup should not meet error other than KeyLocked.
		log.Error("unexpect kv error", zap.Reflect("KvError", v.KvError))
		return nil, backoffMs, errors.Annotatef(berrors.ErrKVUnknown, "storeID: %d OnBackupResponse error %v", storeID, v)

	case *backuppb.Error_RegionError:
		regionErr := v.RegionError
		// Ignore following errors.
		if !(regionErr.EpochNotMatch != nil ||
			regionErr.NotLeader != nil ||
			regionErr.RegionNotFound != nil ||
			regionErr.ServerIsBusy != nil ||
			regionErr.StaleCommand != nil ||
			regionErr.StoreNotMatch != nil ||
			regionErr.ReadIndexNotReady != nil ||
			regionErr.ProposalInMergingMode != nil) {
			log.Error("unexpect region error", zap.Reflect("RegionError", regionErr))
			return nil, backoffMs, errors.Annotatef(berrors.ErrKVUnknown, "storeID: %d OnBackupResponse error %v", storeID, v)
		}
		log.Warn("backup occur region error",
			zap.Reflect("RegionError", regionErr),
			zap.Uint64("storeID", storeID))
		// TODO: a better backoff.
		backoffMs = 1000 /* 1s */
		return nil, backoffMs, nil
	case *backuppb.Error_ClusterIdError:
		log.Error("backup occur cluster ID error", zap.Reflect("error", v), zap.Uint64("storeID", storeID))
		return nil, 0, errors.Annotatef(berrors.ErrKVClusterIDMismatch, "%v on storeID: %d", resp.Error, storeID)
	default:
		// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
		if utils.MessageIsRetryableStorageError(resp.GetError().GetMsg()) {
			log.Warn("backup occur storage error", zap.String("error", resp.GetError().GetMsg()))
			// back off 3000ms, for S3 is 99.99% available (i.e. the max outage time would less than 52.56mins per year),
			// this time would be probably enough for s3 to resume.
			return nil, 3000, nil
		}
		log.Error("backup occur unknown error", zap.String("error", resp.Error.GetMsg()), zap.Uint64("storeID", storeID))
		return nil, 0, errors.Annotatef(berrors.ErrKVUnknown, "%v on storeID: %d", resp.Error, storeID)
	}
}

func (bc *Client) handleFineGrained(
	ctx context.Context,
	bo *tikv.Backoffer,
	req backuppb.BackupRequest,
	targetStoreIds map[uint64]struct{},
	respCh chan<- *backuppb.BackupResponse,
) (int, error) {
	targetPeer, pderr := bc.findTargetPeer(ctx, req.StartKey, req.IsRawKv, targetStoreIds)
	if pderr != nil {
		return 0, errors.Trace(pderr)
	}
	storeID := targetPeer.GetStoreId()
	lockResolver := bc.mgr.GetLockResolver()
	client, err := bc.mgr.GetBackupClient(ctx, storeID)
	if err != nil {
		if berrors.Is(err, berrors.ErrFailedToConnect) {
			// When the leader store is died,
			// 20s for the default max duration before the raft election timer fires.
			logutil.CL(ctx).Warn("failed to connect to store, skipping", logutil.ShortError(err), zap.Uint64("storeID", storeID))
			return 20000, nil
		}

		logutil.CL(ctx).Error("fail to connect store", zap.Uint64("StoreID", storeID))
		return 0, errors.Annotatef(err, "failed to connect to store %d", storeID)
	}
	hasProgress := false
	backoffMill := 0
	err = SendBackup(
		ctx, storeID, client, req,
		// Handle responses with the same backoffer.
		func(resp *backuppb.BackupResponse) error {
			response, shouldBackoff, err1 :=
				OnBackupResponse(storeID, bo, req.EndVersion, lockResolver, resp)
			if err1 != nil {
				return err1
			}
			if backoffMill < shouldBackoff {
				backoffMill = shouldBackoff
			}
			if response != nil {
				respCh <- response
			}
			// When meet an error, we need to set hasProgress too, in case of
			// overriding the backoffTime of original error.
			// hasProgress would be false iff there is a early io.EOF from the stream.
			hasProgress = true
			return nil
		},
		func() (backuppb.BackupClient, error) {
			logutil.CL(ctx).Warn("reset the connection in handleFineGrained", zap.Uint64("storeID", storeID))
			return bc.mgr.ResetBackupClient(ctx, storeID)
		})
	if err != nil {
		if berrors.Is(err, berrors.ErrFailedToConnect) {
			// When the leader store is died,
			// 20s for the default max duration before the raft election timer fires.
			logutil.CL(ctx).Warn("failed to connect to store, skipping", logutil.ShortError(err), zap.Uint64("storeID", storeID))
			return 20000, nil
		}
		logutil.CL(ctx).Error("failed to send fine-grained backup", zap.Uint64("storeID", storeID), logutil.ShortError(err))
		return 0, errors.Annotatef(err, "failed to send fine-grained backup [%s, %s)",
			redact.Key(req.StartKey), redact.Key(req.EndKey))
	}

	// If no progress, backoff 10s for debouncing.
	// 10s is the default interval of stores sending a heartbeat to the PD.
	// And is the average new leader election timeout, which would be a reasonable back off time.
	if !hasProgress {
		backoffMill = 10000
	}
	return backoffMill, nil
}

func doSendBackup(
	ctx context.Context,
	client backuppb.BackupClient,
	req backuppb.BackupRequest,
	respFn func(*backuppb.BackupResponse) error,
) error {
	failpoint.Inject("hint-backup-start", func(v failpoint.Value) {
		logutil.CL(ctx).Info("failpoint hint-backup-start injected, " +
			"process will notify the shell.")
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
		}
		time.Sleep(3 * time.Second)
	})
	bCli, err := client.Backup(ctx, &req)
	failpoint.Inject("reset-retryable-error", func(val failpoint.Value) {
		switch val.(string) {
		case "Unavaiable":
			{
				logutil.CL(ctx).Debug("failpoint reset-retryable-error unavailable injected.")
				err = status.Error(codes.Unavailable, "Unavailable error")
			}
		case "Internal":
			{
				logutil.CL(ctx).Debug("failpoint reset-retryable-error internal injected.")
				err = status.Error(codes.Internal, "Internal error")
			}
		}
	})
	failpoint.Inject("reset-not-retryable-error", func(val failpoint.Value) {
		if val.(bool) {
			logutil.CL(ctx).Debug("failpoint reset-not-retryable-error injected.")
			err = status.Error(codes.Unknown, "Your server was haunted hence doesn't work, meow :3")
		}
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = bCli.CloseSend()
	}()

	for {
		resp, err := bCli.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF { // nolint:errorlint
				logutil.CL(ctx).Debug("backup streaming finish",
					logutil.Key("backup-start-key", req.GetStartKey()),
					logutil.Key("backup-end-key", req.GetEndKey()))
				return nil
			}
			return err
		}
		// TODO: handle errors in the resp.
		logutil.CL(ctx).Debug("range backed up",
			logutil.Key("small-range-start-key", resp.GetStartKey()),
			logutil.Key("small-range-end-key", resp.GetEndKey()),
			zap.Int("api-version", int(resp.ApiVersion)))
		err = respFn(resp)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// SendBackup send backup request to the given store.
// Stop receiving response if respFn returns error.
func SendBackup(
	ctx context.Context,
	// the `storeID` seems only used for logging now, maybe we can remove it then?
	storeID uint64,
	client backuppb.BackupClient,
	req backuppb.BackupRequest,
	respFn func(*backuppb.BackupResponse) error,
	resetFn func() (backuppb.BackupClient, error),
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			fmt.Sprintf("Client.SendBackup, storeID = %d, StartKey = %s, EndKey = %s",
				storeID, redact.Key(req.StartKey), redact.Key(req.EndKey)),
			opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var errReset error
	var errBackup error

	for retry := 0; retry < backupRetryTimes; retry++ {
		logutil.CL(ctx).Info("try backup",
			zap.Int("retry time", retry),
		)
		errBackup = doSendBackup(ctx, client, req, respFn)
		if errBackup != nil {
			if isRetryableError(errBackup) {
				time.Sleep(3 * time.Second)
				client, errReset = resetFn()
				if errReset != nil {
					return errors.Annotatef(errReset, "failed to reset backup connection on store:%d "+
						"please check the tikv status", storeID)
				}
				continue
			}
			logutil.CL(ctx).Error("fail to backup", zap.Uint64("StoreID", storeID), zap.Int("retry", retry))
			return berrors.ErrFailedToConnect.Wrap(errBackup).GenWithStack("failed to create backup stream to store %d", storeID)
		}
		// finish backup
		break
	}
	return nil
}

// gRPC communication cancelled with connection closing
const (
	gRPC_Cancel = "the client connection is closing"
)

// isRetryableError represents whether we should retry reset grpc connection.
func isRetryableError(err error) bool {
	// some errors can be retried
	// https://github.com/pingcap/tidb/issues/34350
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded,
		codes.ResourceExhausted, codes.Aborted, codes.Internal:
		{
			log.Warn("backup met some errors, these errors can be retry 5 times", zap.Error(err))
			return true
		}
	}

	// At least, there are two possible cancel() call,
	// one from backup range, another from gRPC, here we retry when gRPC cancel with connection closing
	if status.Code(err) == codes.Canceled {
		if s, ok := status.FromError(err); ok {
			if strings.Contains(s.Message(), gRPC_Cancel) {
				log.Warn("backup met grpc cancel error, this errors can be retry 5 times", zap.Error(err))
				return true
			}
		}
	}
	return false
}
