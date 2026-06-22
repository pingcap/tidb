// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

func dropTemporarySystemSchemasAfterAbort(ctx context.Context, g glue.Glue, mgr *conn.Mgr, cfg *RestoreConfig) error {
	if !cfg.WithSysTable && !cfg.LoadStats {
		return nil
	}

	session, err := g.CreateSession(mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()

	for _, dbName := range []string{mysql.SystemDB, mysql.SysDB, mysql.WorkloadSchema} {
		tempDBName := utils.TemporaryDBName(dbName)
		log.Info("dropping temporary system database after abort",
			zap.Stringer("database", tempDBName))
		if err := session.Execute(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", utils.EncloseName(tempDBName.L))); err != nil {
			return errors.Annotatef(err, "failed to drop temporary system database %s", tempDBName.O)
		}
	}
	return nil
}
