// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/config"
	"github.com/spf13/pflag"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	flagOnline   = "online"
	flagNoSchema = "no-schema"

	// FlagMergeRegionSizeBytes is the flag name of merge small regions by size
	FlagMergeRegionSizeBytes = "merge-region-size-bytes"
	// FlagMergeRegionKeyCount is the flag name of merge small regions by key count
	FlagMergeRegionKeyCount = "merge-region-key-count"

	defaultRestoreConcurrency = 128
	maxRestoreBatchSizeLimit  = 10240
	defaultDDLConcurrency     = 16
)

// RestoreCommonConfig is the common configuration for all BR restore tasks.
type RestoreCommonConfig struct {
	Online bool `json:"online" toml:"online"`

	// MergeSmallRegionSizeBytes is the threshold of merging small regions (Default 96MB, region split size).
	// MergeSmallRegionKeyCount is the threshold of merging smalle regions (Default 960_000, region split key count).
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	MergeSmallRegionSizeBytes uint64 `json:"merge-region-size-bytes" toml:"merge-region-size-bytes"`
	MergeSmallRegionKeyCount  uint64 `json:"merge-region-key-count" toml:"merge-region-key-count"`
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *RestoreCommonConfig) adjust() {
	if cfg.MergeSmallRegionKeyCount == 0 {
		cfg.MergeSmallRegionKeyCount = restore.DefaultMergeRegionKeyCount
	}
	if cfg.MergeSmallRegionSizeBytes == 0 {
		cfg.MergeSmallRegionSizeBytes = restore.DefaultMergeRegionSizeBytes
	}
}

// DefineRestoreCommonFlags defines common flags for the restore command.
func DefineRestoreCommonFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool(flagOnline, false, "(experimental) Whether online when restore")

	flags.Uint64(FlagMergeRegionSizeBytes, restore.DefaultMergeRegionSizeBytes,
		"the threshold of merging small regions (Default 96MB, region split size)")
	flags.Uint64(FlagMergeRegionKeyCount, restore.DefaultMergeRegionKeyCount,
		"the threshold of merging smalle regions (Default 960_000, region split key count)")
	_ = flags.MarkHidden(FlagMergeRegionSizeBytes)
	_ = flags.MarkHidden(FlagMergeRegionKeyCount)
}

// ParseFromFlags parses the config from the flag set.
func (cfg *RestoreCommonConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionKeyCount, err = flags.GetUint64(FlagMergeRegionKeyCount)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionSizeBytes, err = flags.GetUint64(FlagMergeRegionSizeBytes)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

// RestoreConfig is the configuration specific for restore tasks.
type RestoreConfig struct {
	Config
	RestoreCommonConfig

	NoSchema bool `json:"no-schema" toml:"no-schema"`
}

// DefineRestoreFlags defines common flags for the restore tidb command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool(flagNoSchema, false, "skip creating schemas and tables, reuse existing empty ones")
	// Do not expose this flag
	_ = flags.MarkHidden(flagNoSchema)

	DefineRestoreCommonFlags(flags)
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.NoSchema, err = flags.GetBool(flagNoSchema)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.Config.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.RestoreCommonConfig.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	return nil
}

// adjustRestoreConfig is use for BR(binary) and BR in TiDB.
// When new config was add and not included in parser.
// we should set proper value in this function.
// so that both binary and TiDB will use same default value.
func (cfg *RestoreConfig) adjustRestoreConfig() {
	cfg.Config.adjust()
	cfg.RestoreCommonConfig.adjust()

	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	if cfg.Config.SwitchModeInterval == 0 {
		cfg.Config.SwitchModeInterval = defaultSwitchInterval
	}
}

// CheckRestoreDBAndTable is used to check whether the restore dbs or tables have been backup
func CheckRestoreDBAndTable(client *restore.Client, cfg *RestoreConfig) error {
	if len(cfg.Schemas) == 0 && len(cfg.Tables) == 0 {
		return nil
	}
	schemas := client.GetDatabases()
	schemasMap := make(map[string]struct{})
	tablesMap := make(map[string]struct{})
	for _, db := range schemas {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}
		schemasMap[utils.EncloseName(dbName)] = struct{}{}
		for _, table := range db.Tables {
			tablesMap[utils.EncloseDBAndTable(dbName, table.Info.Name.O)] = struct{}{}
		}
	}
	restoreSchemas := cfg.Schemas
	restoreTables := cfg.Tables
	for schema := range restoreSchemas {
		if _, ok := schemasMap[schema]; !ok {
			return errors.Annotatef(berrors.ErrUndefinedRestoreDbOrTable,
				"[database: %v] has not been backup, please ensure you has input a correct database name", schema)
		}
	}
	for table := range restoreTables {
		if _, ok := tablesMap[table]; !ok {
			return errors.Annotatef(berrors.ErrUndefinedRestoreDbOrTable,
				"[table: %v] has not been backup, please ensure you has input a correct table name", table)
		}
	}
	return nil
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.adjustRestoreConfig()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestore", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Restore needs domain to do DDL.
	needDomain := true
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client, err := restore.NewRestoreClient(g, mgr.GetPDClient(), mgr.GetStorage(), mgr.GetTLSConfig(), keepaliveCfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer client.Close()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
		SkipCheckPath:   cfg.SkipCheckPath,
	}
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	client.SetRateLimit(cfg.RateLimit)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}
	if cfg.NoSchema {
		client.EnableSkipCreateSQL()
	}
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	backupVersion := version.NormalizeBackupVersion(backupMeta.ClusterVersion)
	if cfg.CheckRequirements && backupVersion != nil {
		if versionErr := version.CheckClusterVersion(ctx, mgr.GetPDClient(), version.CheckVersionForBackup(backupVersion)); versionErr != nil {
			return errors.Trace(versionErr)
		}
	}
	reader := metautil.NewMetaReader(backupMeta, s)
	if err = client.InitBackupMeta(c, backupMeta, u, s, reader); err != nil {
		return errors.Trace(err)
	}

	if client.IsRawKvMode() {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do transactional restore from raw kv data")
	}
	if err = CheckRestoreDBAndTable(client, cfg); err != nil {
		return err
	}
	files, tables, dbs := filterRestoreFiles(client, cfg)
	if len(dbs) == 0 && len(tables) != 0 {
		return errors.Annotate(berrors.ErrRestoreInvalidBackup, "contain tables but no databases")
	}
	archiveSize := reader.ArchiveSize(ctx, files)
	g.Record(summary.RestoreDataSize, archiveSize)
	//restore from tidb will fetch a general Size issue https://github.com/pingcap/tidb/issues/27247
	g.Record("Size", archiveSize)
	restoreTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}
	g.Record("BackupTS", restoreTS)

	// restore checksum will check safe point with its start ts, see details at
	// https://github.com/pingcap/tidb/blob/180c02127105bed73712050594da6ead4d70a85f/store/tikv/kv.go#L186-L190
	// so, we should keep the safe point unchangeable. to avoid GC life time is shorter than transaction duration.
	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	var newTS uint64
	if client.IsIncremental() {
		newTS = restoreTS
	}
	ddlJobs := restore.FilterDDLJobs(client.GetDDLJobs(), tables)

	err = client.PreCheckTableTiFlashReplica(ctx, tables)
	if err != nil {
		return errors.Trace(err)
	}

	err = client.PreCheckTableClusterIndex(tables, ddlJobs, mgr.GetDomain())
	if err != nil {
		return errors.Trace(err)
	}

	// pre-set TiDB config for restore
	restoreDBConfig := enableTiDBConfig()
	defer restoreDBConfig()

	// execute DDL first
	err = client.ExecDDLs(ctx, ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}

	// nothing to restore, maybe only ddl changes in incremental restore
	if len(dbs) == 0 && len(tables) == 0 {
		log.Info("nothing to restore, all databases and tables are filtered out")
		// even nothing to restore, we show a success message since there is no failure.
		summary.SetSuccessStatus(true)
		return nil
	}

	for _, db := range dbs {
		err = client.CreateDatabase(ctx, db.Info)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// We make bigger errCh so we won't block on multi-part failed.
	errCh := make(chan error, 32)
	// Maybe allow user modify the DDL concurrency isn't necessary,
	// because executing DDL is really I/O bound (or, algorithm bound?),
	// and we cost most of time at waiting DDL jobs be enqueued.
	// So these jobs won't be faster or slower when machine become faster or slower,
	// hence make it a fixed value would be fine.
	var dbPool []*restore.DB
	if g.OwnsStorage() {
		// Only in binary we can use multi-thread sessions to create tables.
		// so use OwnStorage() to tell whether we are use binary or SQL.
		dbPool, err = restore.MakeDBPool(defaultDDLConcurrency, func() (*restore.DB, error) {
			return restore.NewDB(g, mgr.GetStorage())
		})
	}
	if err != nil {
		log.Warn("create session pool failed, we will send DDLs only by created sessions",
			zap.Error(err),
			zap.Int("sessionCount", len(dbPool)),
		)
	}
	tableStream := client.GoCreateTables(ctx, mgr.GetDomain(), tables, newTS, dbPool, errCh)
	if len(files) == 0 {
		log.Info("no files, empty databases and tables are restored")
		summary.SetSuccessStatus(true)
		// don't return immediately, wait all pipeline done.
	}

	tableFileMap := restore.MapTableToFiles(files)
	log.Debug("mapped table to files", zap.Any("result map", tableFileMap))

	rangeStream := restore.GoValidateFileRanges(
		ctx, tableStream, tableFileMap, cfg.MergeSmallRegionKeyCount, cfg.MergeSmallRegionKeyCount, errCh)

	rangeSize := restore.EstimateRangeSize(files)
	summary.CollectInt("restore ranges", rangeSize)
	log.Info("range and file prepared", zap.Int("file count", len(files)), zap.Int("range count", rangeSize))

	restoreSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restorePostWork(ctx, client, restoreSchedulers)

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(ctx, cfg.PD); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	// Restore sst files in batch.
	batchSize := utils.ClampInt(int(cfg.Concurrency), defaultRestoreConcurrency, maxRestoreBatchSizeLimit)
	failpoint.Inject("small-batch-size", func(v failpoint.Value) {
		log.Info("failpoint small batch size is on", zap.Int("size", v.(int)))
		batchSize = v.(int)
	})

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx,
		cmdName,
		// Split/Scatter + Download/Ingest + Checksum
		int64(rangeSize+len(files)+len(tables)),
		!cfg.LogProgress)
	defer updateCh.Close()
	sender, err := restore.NewTiKVSender(ctx, client, updateCh)
	if err != nil {
		return errors.Trace(err)
	}
	manager := restore.NewBRContextManager(client)
	batcher, afterRestoreStream := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(batchSize)
	batcher.EnableAutoCommit(ctx, time.Second)
	go restoreTableStream(ctx, rangeStream, batcher, errCh)

	var finish <-chan struct{}
	// Checksum
	if cfg.Checksum {
		finish = client.GoValidateChecksum(
			ctx, afterRestoreStream, mgr.GetStorage().GetClient(), errCh, updateCh, cfg.ChecksumConcurrency)
	} else {
		// when user skip checksum, just collect tables, and drop them.
		finish = dropToBlackhole(ctx, afterRestoreStream, errCh, updateCh)
	}

	select {
	case err = <-errCh:
		err = multierr.Append(err, multierr.Combine(restore.Exhaust(errCh)...))
	case <-finish:
	}

	// If any error happened, return now.
	if err != nil {
		return errors.Trace(err)
	}

	// The cost of rename user table / replace into system table wouldn't be so high.
	// So leave it out of the pipeline for easier implementation.
	client.RestoreSystemSchemas(ctx, cfg.TableFilter)

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// dropToBlackhole drop all incoming tables into black hole,
// i.e. don't execute checksum, just increase the process anyhow.
func dropToBlackhole(
	ctx context.Context,
	tableStream <-chan restore.CreatedTable,
	errCh chan<- error,
	updateCh glue.Progress,
) <-chan struct{} {
	outCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			outCh <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case _, ok := <-tableStream:
				if !ok {
					return
				}
				updateCh.Inc()
			}
		}
	}()
	return outCh
}

func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backuppb.File, tables []*metautil.Table, dbs []*utils.Database) {
	for _, db := range client.GetDatabases() {
		createdDatabase := false
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}
		for _, table := range db.Tables {
			if !cfg.TableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			if !createdDatabase {
				dbs = append(dbs, db)
				createdDatabase = true
			}
			files = append(files, table.Files...)
			tables = append(tables, table)
		}
	}
	return
}

// restorePreWork executes some prepare work before restore.
// TODO make this function returns a restore post work.
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) (pdutil.UndoFunc, error) {
	if client.IsOnline() {
		return pdutil.Nop, nil
	}

	// Switch TiKV cluster to import mode (adjust rocksdb configuration).
	client.SwitchToImportMode(ctx)

	return mgr.RemoveSchedulers(ctx)
}

// restorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func restorePostWork(
	ctx context.Context, client *restore.Client, restoreSchedulers pdutil.UndoFunc,
) {
	if ctx.Err() != nil {
		log.Warn("context canceled, try shutdown")
		ctx = context.Background()
	}
	if client.IsOnline() {
		return
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode", zap.Error(err))
	}
	if err := restoreSchedulers(ctx); err != nil {
		log.Warn("failed to restore PD schedulers", zap.Error(err))
	}
}

// enableTiDBConfig tweaks some of configs of TiDB to make the restore progress go well.
// return a function that could restore the config to origin.
func enableTiDBConfig() func() {
	restoreConfig := config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		// set max-index-length before execute DDLs and create tables
		// we set this value to max(3072*4), otherwise we might not restore table
		// when upstream and downstream both set this value greater than default(3072)
		conf.MaxIndexLength = config.DefMaxOfMaxIndexLength
		log.Warn("set max-index-length to max(3072*4) to skip check index length in DDL")
	})
	return restoreConfig
}

// restoreTableStream blocks current goroutine and restore a stream of tables,
// by send tables to batcher.
func restoreTableStream(
	ctx context.Context,
	inputCh <-chan restore.TableWithRange,
	batcher *restore.Batcher,
	errCh chan<- error,
) {
	// We cache old tables so that we can 'batch' recover TiFlash and tables.
	oldTables := []*metautil.Table{}
	defer func() {
		// when things done, we must clean pending requests.
		batcher.Close()
		log.Info("doing postwork",
			zap.Int("table count", len(oldTables)),
		)
	}()

	for {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		case t, ok := <-inputCh:
			if !ok {
				return
			}
			oldTables = append(oldTables, t.OldTable)

			batcher.Add(t)
		}
	}
}
