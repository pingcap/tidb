// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/registry"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tablepkg "github.com/pingcap/tidb/pkg/table"
	"go.uber.org/zap"
)

type abortRestoreModeTable struct {
	schemaID   int64
	tableID    int64
	schemaName string
	tableName  string
}

func markAbortRestoreModeTableSeen(table abortRestoreModeTable, seen map[int64]struct{}) bool {
	if _, ok := seen[table.tableID]; ok {
		return false
	}
	seen[table.tableID] = struct{}{}
	return true
}

func collectAbortSnapshotRestoreModeTables(
	ctx context.Context,
	cmdName string,
	cfg *RestoreConfig,
	mgr *conn.Mgr,
) (iter.Seq[abortRestoreModeTable], error) {
	restoreCfg := *cfg
	snapshotCmdName := cmdName
	if IsStreamRestore(cmdName) {
		if len(cfg.FullBackupStorage) == 0 {
			return func(func(abortRestoreModeTable) bool) {}, nil
		}
		restoreCfg.Config.Storage = cfg.FullBackupStorage
		snapshotCmdName = FullRestoreCmd
	}

	_, storage, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &restoreCfg.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer storage.Close()
	if backupMeta.IsRawKv || backupMeta.IsTxnKv {
		return func(func(abortRestoreModeTable) bool) {}, nil
	}

	_, loadStatsPhysical := isRestoreSysTablesPhysically(&SnapshotRestoreConfig{RestoreConfig: &restoreCfg})
	isIncremental := !(backupMeta.StartVersion == backupMeta.EndVersion || backupMeta.StartVersion == 0)
	if isIncremental || restoreCfg.ExplicitFilter || !isFullRestore(snapshotCmdName) {
		loadStatsPhysical = false
	}

	metaReader := metautil.NewMetaReader(backupMeta, storage, &restoreCfg.CipherInfo)
	databases, err := metautil.LoadBackupTables(ctx, metaReader, restoreCfg.LoadStats)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableMap, _, err := filterRestoreFilesFromDatabases(slices.Collect(maps.Values(databases)), &restoreCfg, loadStatsPhysical)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return func(yield func(abortRestoreModeTable) bool) {
		for _, table := range tableMap {
			if table == nil || table.DB == nil || table.Info == nil {
				continue
			}
			if !yield(abortRestoreModeTable{
				schemaName: table.DB.Name.O,
				tableName:  table.Info.Name.O,
			}) {
				return
			}
		}
	}, nil
}

func newLogClientForAbortIDMap(ctx context.Context, g glue.Glue, cfg *RestoreConfig, mgr *conn.Mgr) (*logclient.LogClient, error) {
	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := logclient.NewLogClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)
	client.SetCheckRequirements(cfg.CheckRequirements)
	if err := client.Init(ctx, g, mgr.GetStorage()); err != nil {
		return nil, errors.Trace(err)
	}

	backend, err := objstore.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		client.Close(ctx)
		return nil, errors.Trace(err)
	}
	opts := getExternalStorageOptions(&cfg.Config, backend)
	if err = client.SetStorage(ctx, backend, &opts); err != nil {
		client.Close(ctx)
		return nil, errors.Trace(err)
	}
	client.SetCrypter(&cfg.CipherInfo)
	client.SetUpstreamClusterID(cfg.UpstreamClusterID)
	client.SetRestoreID(cfg.RestoreID)
	client.SetUseCheckpointEnabled(cfg.UseCheckpoint && cfg.logCheckpointMetaManager != nil)
	return client, nil
}

func collectAbortPITRRestoreModeTables(
	ctx context.Context,
	g glue.Glue,
	cfg *RestoreConfig,
	mgr *conn.Mgr,
) (iter.Seq[abortRestoreModeTable], error) {
	client, err := newLogClientForAbortIDMap(ctx, g, cfg, mgr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer client.Close(ctx)

	dbMaps, err := client.LoadSchemasMap(ctx, cfg.RestoreTS, cfg.logCheckpointMetaManager)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReplaces := stream.FromDBMapProto(dbMaps)
	return func(yield func(abortRestoreModeTable) bool) {
		for _, dbReplace := range dbReplaces {
			if dbReplace.FilteredOut {
				continue
			}
			for _, tableReplace := range dbReplace.TableMap {
				if tableReplace.FilteredOut {
					continue
				}
				if !yield(abortRestoreModeTable{
					schemaID:   dbReplace.DbID,
					tableID:    tableReplace.TableID,
					schemaName: dbReplace.Name,
					tableName:  tableReplace.Name,
				}) {
					return
				}
			}
		}
	}, nil
}

func collectAbortRestoreModeTables(
	ctx context.Context,
	g glue.Glue,
	cmdName string,
	cfg *RestoreConfig,
	mgr *conn.Mgr,
) (iter.Seq[abortRestoreModeTable], error) {
	snapshotTableIter, err := collectAbortSnapshotRestoreModeTables(ctx, cmdName, cfg, mgr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !IsStreamRestore(cmdName) {
		return snapshotTableIter, nil
	}
	pitrTableIter, err := collectAbortPITRRestoreModeTables(ctx, g, cfg, mgr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return func(yield func(abortRestoreModeTable) bool) {
		for table := range snapshotTableIter {
			if !yield(table) {
				return
			}
		}
		for table := range pitrTableIter {
			if !yield(table) {
				return
			}
		}
	}, nil
}

func setAbortRestoreTablesToNormal(
	ctx context.Context,
	g glue.Glue,
	mgr *conn.Mgr,
	tables iter.Seq[abortRestoreModeTable],
) error {
	session, err := g.CreateSession(mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()

	seen := make(map[int64]struct{})
	tableCount := 0
	// Stream restore abort can collect candidates from both the full backup meta
	// and the PiTR ID map. The same restored physical table can appear in both
	// sources, so deduplicate by the current table ID at the final consumer.
	for candidate := range tables {
		var tbl tablepkg.Table
		// Snapshot/full-backup candidates are collected from backup meta by
		// name. Their current restored table ID is only known after resolving
		// the name in the target cluster.
		if candidate.tableID == 0 {
			t, err := mgr.GetDomain().InfoSchema().TableByName(ctx, ast.NewCIStr(candidate.schemaName), ast.NewCIStr(candidate.tableName))
			if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
				log.Info("restore table no longer exists after checkpoint cleanup, skipping",
					zap.String("database", candidate.schemaName),
					zap.String("table", candidate.tableName))
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}
			tbl = t
			candidate.schemaID = t.Meta().DBID
			candidate.tableID = t.Meta().ID
		} else {
			t, ok := mgr.GetDomain().InfoSchema().TableByID(ctx, candidate.tableID)
			if !ok || t.Meta().DBID != candidate.schemaID {
				log.Info("restore table no longer exists after checkpoint cleanup, skipping",
					zap.Int64("schemaID", candidate.schemaID),
					zap.Int64("tableID", candidate.tableID),
					zap.String("database", candidate.schemaName),
					zap.String("table", candidate.tableName))
				continue
			}
			tbl = t
		}
		if tbl.Meta().Mode != model.TableModeRestore {
			log.Warn("table is not in restore mode after checkpoint cleanup, skipping",
				zap.Int64("schemaID", candidate.schemaID),
				zap.Int64("tableID", candidate.tableID),
				zap.String("database", candidate.schemaName),
				zap.String("table", candidate.tableName),
				zap.Any("tableMode", tbl.Meta().Mode))
			continue
		}
		if !markAbortRestoreModeTableSeen(candidate, seen) {
			continue
		}
		tableCount++
		log.Info("altering table mode to normal after abort",
			zap.Int64("schemaID", candidate.schemaID),
			zap.Int64("tableID", candidate.tableID),
			zap.String("database", candidate.schemaName),
			zap.String("table", candidate.tableName))
		if err := session.AlterTableMode(ctx, candidate.schemaID, candidate.tableID, model.TableModeNormal); err != nil {
			return errors.Annotatef(err, "failed to alter table mode for table with schemaID=%d, tableID=%d",
				candidate.schemaID, candidate.tableID)
		}
	}
	if tableCount == 0 {
		log.Info("no restore-mode tables need to be set to normal after abort")
	}
	return nil
}

// RunRestoreAbort aborts a restore task by finding it in the registry and cleaning up
// Similar to resumeOrCreate, it first resolves the restoredTS then finds and deletes the matching paused task
func RunRestoreAbort(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.Adjust()
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// update keyspace to be user specified
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = cfg.KeyspaceName
	})

	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.KeyspaceName, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, true, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	// get upstream cluster ID and startTS from backup storage if not already set
	if cfg.UpstreamClusterID == 0 {
		if IsStreamRestore(cmdName) {
			// For PiTR restore, get cluster ID from log storage
			_, s, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
			if err != nil {
				return errors.Trace(err)
			}
			logInfo, err := getLogInfoFromStorage(ctx, s, cfg.CheckRequirements, cfg.FromReplicationStorage, cfg.ReplicationStatusSubPrefix)
			if err != nil {
				return errors.Trace(err)
			}
			cfg.UpstreamClusterID = logInfo.clusterID

			// For PiTR with full backup, get startTS from full backup meta
			if len(cfg.FullBackupStorage) > 0 && cfg.StartTS == 0 {
				startTS, fullClusterID, err := getFullBackupTS(ctx, cfg)
				if err != nil {
					return errors.Trace(err)
				}
				if logInfo.clusterID > 0 && fullClusterID > 0 && logInfo.clusterID != fullClusterID {
					return errors.Annotatef(berrors.ErrInvalidArgument,
						"cluster ID mismatch: log backup from cluster %d, full backup from cluster %d",
						logInfo.clusterID, fullClusterID)
				}
				cfg.StartTS = startTS
				log.Info("extracted startTS from full backup storage for abort",
					zap.Uint64("start_ts", cfg.StartTS))
			}
		} else {
			// For snapshot restore, get cluster ID from backup meta
			_, _, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
			if err != nil {
				return errors.Trace(err)
			}
			cfg.UpstreamClusterID = backupMeta.ClusterId
		}
		log.Info("extracted upstream cluster ID from backup storage for abort",
			zap.Uint64("upstream_cluster_id", cfg.UpstreamClusterID),
			zap.String("cmd", cmdName))
	}

	// build restore registry
	restoreRegistry, err := registry.NewRestoreRegistry(ctx, g, mgr.GetDomain())
	if err != nil {
		return errors.Trace(err)
	}
	defer restoreRegistry.Close()

	// determine if restoredTS was user-specified
	// if RestoreTS is 0, it means user didn't specify it (similar to resumeOrCreate logic)
	isRestoredTSUserSpecified := cfg.RestoreTS != 0

	// create registration info from config to find matching tasks
	registrationInfo := registry.RegistrationInfo{
		FilterStrings:     cfg.FilterStr,
		StartTS:           cfg.StartTS,
		RestoredTS:        cfg.RestoreTS,
		UpstreamClusterID: cfg.UpstreamClusterID,
		WithSysTable:      cfg.WithSysTable,
		Cmd:               cmdName,
	}

	// find and delete matching paused task atomically
	// this will first resolve the restoredTS (similar to resumeOrCreate) then find and delete the task
	deletedRestoreID, resolvedRestoreTS, err := restoreRegistry.FindAndDeleteMatchingTask(ctx, registrationInfo, isRestoredTSUserSpecified)
	if err != nil {
		return errors.Trace(err)
	}

	if deletedRestoreID == 0 {
		log.Info("no paused restore task found with matching parameters")
		return nil
	}

	log.Info("successfully deleted matching paused restore task", zap.Uint64("restoreId", deletedRestoreID))

	// clean up checkpoint data for the deleted task
	log.Info("cleaning up checkpoint data", zap.Uint64("restoreId", deletedRestoreID))

	// update config with restore ID to clean up checkpoint
	cfg.RestoreID = deletedRestoreID
	cfg.RestoreTS = resolvedRestoreTS

	// initialize all checkpoint managers for cleanup (deletion is noop if checkpoints not exist)
	var checkpointManagerInitErr error
	if len(cfg.CheckpointStorage) > 0 {
		clusterID := mgr.GetPDClient().GetClusterID(ctx)
		log.Info("initializing storage checkpoint meta managers for cleanup",
			zap.Uint64("restoreID", deletedRestoreID),
			zap.Uint64("clusterID", clusterID))
		var err error
		if IsStreamRestore(cmdName) {
			err = cfg.newStorageCheckpointMetaManagerPITR(ctx, clusterID, deletedRestoreID)
		} else {
			err = cfg.newStorageCheckpointMetaManagerSnapshot(ctx, clusterID, deletedRestoreID)
		}
		if err != nil {
			log.Warn("failed to initialize storage checkpoint meta managers for cleanup", zap.Error(err))
			checkpointManagerInitErr = err
		}
	} else {
		log.Info("initializing table checkpoint meta managers for cleanup",
			zap.Uint64("restoreID", deletedRestoreID))
		var err error
		if IsStreamRestore(cmdName) {
			err = cfg.newTableCheckpointMetaManagerPITR(g, mgr.GetDomain(), deletedRestoreID)
		} else {
			err = cfg.newTableCheckpointMetaManagerSnapshot(g, mgr.GetDomain(), deletedRestoreID)
		}
		if err != nil {
			log.Warn("failed to initialize table checkpoint meta managers for cleanup", zap.Error(err))
			checkpointManagerInitErr = err
		}
	}

	// Collect target tables before checkpoint cleanup because PiTR restore table
	// identities can come from the checkpoint-persisted ID map. After cleanup,
	// abort may no longer have a precise source for the incremental restore tables.
	tablesToNormal, collectTablesErr := collectAbortRestoreModeTables(ctx, g, cmdName, cfg, mgr)
	if collectTablesErr != nil {
		log.Warn("failed to collect restore-mode tables before checkpoint cleanup", zap.Error(collectTablesErr))
	}

	// clean up checkpoint data
	cleanUpCheckpoints(ctx, cfg)

	if cfg.UseCheckpoint && checkpointManagerInitErr != nil {
		return errors.Trace(checkpointManagerInitErr)
	}
	if cfg.UseCheckpoint {
		checkpointPersisted, err := hasCheckpointPersisted(ctx, cfg)
		if err != nil {
			return errors.Trace(err)
		}
		if checkpointPersisted {
			return errors.New("checkpoint data still exists after cleanup")
		}
	}

	if collectTablesErr != nil {
		return errors.Trace(collectTablesErr)
	}
	if err := setAbortRestoreTablesToNormal(ctx, g, mgr, tablesToNormal); err != nil {
		return errors.Trace(err)
	}

	log.Info("successfully aborted restore task and cleaned up checkpoint data. "+
		"Use drop statements to clean up the restored data from the cluster if you want to.",
		zap.Uint64("restoreId", deletedRestoreID))
	return nil
}
