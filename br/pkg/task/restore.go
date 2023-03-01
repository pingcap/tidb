// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	pconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/tikv"
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
	// FlagPDConcurrency controls concurrency pd-relative operations like split & scatter.
	FlagPDConcurrency = "pd-concurrency"
	// FlagBatchFlushInterval controls after how long the restore batch would be auto sended.
	FlagBatchFlushInterval = "batch-flush-interval"
	// FlagDdlBatchSize controls batch ddl size to create a batch of tables
	FlagDdlBatchSize = "ddl-batch-size"
	// FlagWithPlacementPolicy corresponds to tidb config with-tidb-placement-mode
	// current only support STRICT or IGNORE, the default is STRICT according to tidb.
	FlagWithPlacementPolicy = "with-tidb-placement-mode"
	// FlagKeyspaceName corresponds to tidb config keyspace-name
	FlagKeyspaceName = "keyspace-name"

	// FlagStreamStartTS and FlagStreamRestoreTS is used for log restore timestamp range.
	FlagStreamStartTS   = "start-ts"
	FlagStreamRestoreTS = "restored-ts"
	// FlagStreamFullBackupStorage is used for log restore, represents the full backup storage.
	FlagStreamFullBackupStorage = "full-backup-storage"
	// FlagPiTRBatchCount and FlagPiTRBatchSize are used for restore log with batch method.
	FlagPiTRBatchCount  = "pitr-batch-count"
	FlagPiTRBatchSize   = "pitr-batch-size"
	FlagPiTRConcurrency = "pitr-concurrency"

	FlagResetSysUsers = "reset-sys-users"

	defaultPiTRBatchCount     = 8
	defaultPiTRBatchSize      = 16 * 1024 * 1024
	defaultRestoreConcurrency = 128
	defaultPiTRConcurrency    = 16
	maxRestoreBatchSizeLimit  = 10240
	defaultPDConcurrency      = 1
	defaultBatchFlushInterval = 16 * time.Second
	defaultFlagDdlBatchSize   = 128
	resetSpeedLimitRetryTimes = 3
)

const (
	FullRestoreCmd  = "Full Restore"
	DBRestoreCmd    = "DataBase Restore"
	TableRestoreCmd = "Table Restore"
	PointRestoreCmd = "Point Restore"
	RawRestoreCmd   = "Raw Restore"
)

// RestoreCommonConfig is the common configuration for all BR restore tasks.
type RestoreCommonConfig struct {
	Online bool `json:"online" toml:"online"`

	// MergeSmallRegionSizeBytes is the threshold of merging small regions (Default 96MB, region split size).
	// MergeSmallRegionKeyCount is the threshold of merging smalle regions (Default 960_000, region split key count).
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	MergeSmallRegionSizeBytes uint64 `json:"merge-region-size-bytes" toml:"merge-region-size-bytes"`
	MergeSmallRegionKeyCount  uint64 `json:"merge-region-key-count" toml:"merge-region-key-count"`

	// determines whether enable restore sys table on default, see fullClusterRestore in restore/client.go
	WithSysTable bool `json:"with-sys-table" toml:"with-sys-table"`

	ResetSysUsers []string `json:"reset-sys-users" toml:"reset-sys-users"`
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *RestoreCommonConfig) adjust() {
	if cfg.MergeSmallRegionKeyCount == 0 {
		cfg.MergeSmallRegionKeyCount = conn.DefaultMergeRegionKeyCount
	}
	if cfg.MergeSmallRegionSizeBytes == 0 {
		cfg.MergeSmallRegionSizeBytes = conn.DefaultMergeRegionSizeBytes
	}
}

// DefineRestoreCommonFlags defines common flags for the restore command.
func DefineRestoreCommonFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool(flagOnline, false, "(experimental) Whether online when restore")
	flags.Uint32(flagConcurrency, 128, "The size of thread pool on BR that executes tasks, "+
		"where each task restores one SST file to TiKV")
	flags.Uint64(FlagMergeRegionSizeBytes, conn.DefaultMergeRegionSizeBytes,
		"the threshold of merging small regions (Default 96MB, region split size)")
	flags.Uint64(FlagMergeRegionKeyCount, conn.DefaultMergeRegionKeyCount,
		"the threshold of merging small regions (Default 960_000, region split key count)")
	flags.Uint(FlagPDConcurrency, defaultPDConcurrency,
		"concurrency pd-relative operations like split & scatter.")
	flags.Duration(FlagBatchFlushInterval, defaultBatchFlushInterval,
		"after how long a restore batch would be auto sent.")
	flags.Uint(FlagDdlBatchSize, defaultFlagDdlBatchSize,
		"batch size for ddl to create a batch of tables once.")
	flags.Bool(flagWithSysTable, false, "whether restore system privilege tables on default setting")
	flags.StringArrayP(FlagResetSysUsers, "", []string{"cloud_admin", "root"}, "whether reset these users after restoration")
	_ = flags.MarkHidden(FlagResetSysUsers)
	_ = flags.MarkHidden(FlagMergeRegionSizeBytes)
	_ = flags.MarkHidden(FlagMergeRegionKeyCount)
	_ = flags.MarkHidden(FlagPDConcurrency)
	_ = flags.MarkHidden(FlagBatchFlushInterval)
	_ = flags.MarkHidden(FlagDdlBatchSize)
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

	if flags.Lookup(flagWithSysTable) != nil {
		cfg.WithSysTable, err = flags.GetBool(flagWithSysTable)
		if err != nil {
			return errors.Trace(err)
		}
	}
	cfg.ResetSysUsers, err = flags.GetStringArray(FlagResetSysUsers)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

// RestoreConfig is the configuration specific for restore tasks.
type RestoreConfig struct {
	Config
	RestoreCommonConfig

	NoSchema           bool          `json:"no-schema" toml:"no-schema"`
	PDConcurrency      uint          `json:"pd-concurrency" toml:"pd-concurrency"`
	BatchFlushInterval time.Duration `json:"batch-flush-interval" toml:"batch-flush-interval"`
	// DdlBatchSize use to define the size of batch ddl to create tables
	DdlBatchSize uint `json:"ddl-batch-size" toml:"ddl-batch-size"`

	WithPlacementPolicy string `json:"with-tidb-placement-mode" toml:"with-tidb-placement-mode"`

	// FullBackupStorage is used to  run `restore full` before `restore log`.
	// if it is empty, directly take restoring log justly.
	FullBackupStorage string `json:"full-backup-storage" toml:"full-backup-storage"`

	// [startTs, RestoreTS] is used to `restore log` from StartTS to RestoreTS.
	StartTS         uint64                      `json:"start-ts" toml:"start-ts"`
	RestoreTS       uint64                      `json:"restore-ts" toml:"restore-ts"`
	tiflashRecorder *tiflashrec.TiFlashRecorder `json:"-" toml:"-"`
	PitrBatchCount  uint32                      `json:"pitr-batch-count" toml:"pitr-batch-count"`
	PitrBatchSize   uint32                      `json:"pitr-batch-size" toml:"pitr-batch-size"`
	PitrConcurrency uint32                      `json:"-" toml:"-"`

	// for ebs-based restore
	FullBackupType      FullBackupType        `json:"full-backup-type" toml:"full-backup-type"`
	Prepare             bool                  `json:"prepare" toml:"prepare"`
	OutputFile          string                `json:"output-file" toml:"output-file"`
	SkipAWS             bool                  `json:"skip-aws" toml:"skip-aws"`
	CloudAPIConcurrency uint                  `json:"cloud-api-concurrency" toml:"cloud-api-concurrency"`
	VolumeType          pconfig.EBSVolumeType `json:"volume-type" toml:"volume-type"`
	VolumeIOPS          int64                 `json:"volume-iops" toml:"volume-iops"`
	VolumeThroughput    int64                 `json:"volume-throughput" toml:"volume-throughput"`
	ProgressFile        string                `json:"progress-file" toml:"progress-file"`
}

// DefineRestoreFlags defines common flags for the restore tidb command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool(flagNoSchema, false, "skip creating schemas and tables, reuse existing empty ones")
	// Do not expose this flag
	_ = flags.MarkHidden(flagNoSchema)
	flags.String(FlagWithPlacementPolicy, "STRICT", "correspond to tidb global/session variable with-tidb-placement-mode")
	flags.String(FlagKeyspaceName, "", "correspond to tidb config keyspace-name")

	DefineRestoreCommonFlags(flags)
}

// DefineStreamRestoreFlags defines for the restore log command.
func DefineStreamRestoreFlags(command *cobra.Command) {
	command.Flags().String(FlagStreamStartTS, "", "the start timestamp which log restore from.\n"+
		"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23+0800'")
	command.Flags().String(FlagStreamRestoreTS, "", "the point of restore, used for log restore.\n"+
		"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23+0800'")
	command.Flags().String(FlagStreamFullBackupStorage, "", "specify the backup full storage. "+
		"fill it if want restore full backup before restore log.")
	command.Flags().Uint32(FlagPiTRBatchCount, defaultPiTRBatchCount, "specify the batch count to restore log.")
	command.Flags().Uint32(FlagPiTRBatchSize, defaultPiTRBatchSize, "specify the batch size to retore log.")
	command.Flags().Uint32(FlagPiTRConcurrency, defaultPiTRConcurrency, "specify the concurrency to restore log.")
}

// ParseStreamRestoreFlags parses the `restore stream` flags from the flag set.
func (cfg *RestoreConfig) ParseStreamRestoreFlags(flags *pflag.FlagSet) error {
	tsString, err := flags.GetString(FlagStreamStartTS)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.StartTS, err = ParseTSString(tsString, true); err != nil {
		return errors.Trace(err)
	}
	tsString, err = flags.GetString(FlagStreamRestoreTS)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.RestoreTS, err = ParseTSString(tsString, true); err != nil {
		return errors.Trace(err)
	}

	if cfg.FullBackupStorage, err = flags.GetString(FlagStreamFullBackupStorage); err != nil {
		return errors.Trace(err)
	}

	if cfg.StartTS > 0 && len(cfg.FullBackupStorage) > 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "%v and %v are mutually exclusive",
			FlagStreamStartTS, FlagStreamFullBackupStorage)
	}

	if cfg.PitrBatchCount, err = flags.GetUint32(FlagPiTRBatchCount); err != nil {
		return errors.Trace(err)
	}
	if cfg.PitrBatchSize, err = flags.GetUint32(FlagPiTRBatchSize); err != nil {
		return errors.Trace(err)
	}
	if cfg.PitrConcurrency, err = flags.GetUint32(FlagPiTRConcurrency); err != nil {
		return errors.Trace(err)
	}
	return nil
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
	cfg.Concurrency, err = flags.GetUint32(flagConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	cfg.PDConcurrency, err = flags.GetUint(FlagPDConcurrency)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagPDConcurrency)
	}
	cfg.BatchFlushInterval, err = flags.GetDuration(FlagBatchFlushInterval)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagBatchFlushInterval)
	}

	cfg.DdlBatchSize, err = flags.GetUint(FlagDdlBatchSize)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagDdlBatchSize)
	}
	cfg.WithPlacementPolicy, err = flags.GetString(FlagWithPlacementPolicy)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagWithPlacementPolicy)
	}
	cfg.KeyspaceName, err = flags.GetString(FlagKeyspaceName)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagKeyspaceName)
	}

	if flags.Lookup(flagFullBackupType) != nil {
		// for restore full only
		fullBackupType, err := flags.GetString(flagFullBackupType)
		if err != nil {
			return errors.Trace(err)
		}
		if !FullBackupType(fullBackupType).Valid() {
			return errors.New("invalid full backup type")
		}
		cfg.FullBackupType = FullBackupType(fullBackupType)
		cfg.Prepare, err = flags.GetBool(flagPrepare)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.SkipAWS, err = flags.GetBool(flagSkipAWS)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.CloudAPIConcurrency, err = flags.GetUint(flagCloudAPIConcurrency)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.OutputFile, err = flags.GetString(flagOutputMetaFile)
		if err != nil {
			return errors.Trace(err)
		}
		volumeType, err := flags.GetString(flagVolumeType)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.VolumeType = pconfig.EBSVolumeType(volumeType)
		if !cfg.VolumeType.Valid() {
			return errors.New("invalid volume type: " + volumeType)
		}
		if cfg.VolumeIOPS, err = flags.GetInt64(flagVolumeIOPS); err != nil {
			return errors.Trace(err)
		}
		if cfg.VolumeThroughput, err = flags.GetInt64(flagVolumeThroughput); err != nil {
			return errors.Trace(err)
		}

		cfg.ProgressFile, err = flags.GetString(flagProgressFile)
		if err != nil {
			return errors.Trace(err)
		}

		// iops: gp3 [3,000-16,000]; io1/io2 [100-32,000]
		// throughput: gp3 [125, 1000]; io1/io2 cannot set throughput
		// io1 and io2 volumes support up to 64,000 IOPS only on Instances built on the Nitro System.
		// Other instance families support performance up to 32,000 IOPS.
		// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CreateVolume.html
		// todo: check lower/upper bound
	}

	return nil
}

// Adjust is use for BR(binary) and BR in TiDB.
// When new config was added and not included in parser.
// we should set proper value in this function.
// so that both binary and TiDB will use same default value.
func (cfg *RestoreConfig) Adjust() {
	cfg.Config.adjust()
	cfg.RestoreCommonConfig.adjust()

	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	if cfg.Config.SwitchModeInterval == 0 {
		cfg.Config.SwitchModeInterval = defaultSwitchInterval
	}
	if cfg.PDConcurrency == 0 {
		cfg.PDConcurrency = defaultPDConcurrency
	}
	if cfg.BatchFlushInterval == 0 {
		cfg.BatchFlushInterval = defaultBatchFlushInterval
	}
	if cfg.DdlBatchSize == 0 {
		cfg.DdlBatchSize = defaultFlagDdlBatchSize
	}
	if cfg.CloudAPIConcurrency == 0 {
		cfg.CloudAPIConcurrency = defaultCloudAPIConcurrency
	}
}

func (cfg *RestoreConfig) adjustRestoreConfigForStreamRestore() {
	if cfg.PitrConcurrency == 0 {
		cfg.PitrConcurrency = defaultPiTRConcurrency
	}
	if cfg.PitrBatchCount == 0 {
		cfg.PitrBatchCount = defaultPiTRBatchCount
	}
	if cfg.PitrBatchSize == 0 {
		cfg.PitrBatchSize = defaultPiTRBatchSize
	}
	// another goroutine is used to iterate the backup file
	cfg.PitrConcurrency += 1
	log.Info("set restore kv files concurrency", zap.Int("concurrency", int(cfg.PitrConcurrency)))
	cfg.Config.Concurrency = cfg.PitrConcurrency
}

func configureRestoreClient(ctx context.Context, client *restore.Client, cfg *RestoreConfig) error {
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}
	if cfg.NoSchema {
		client.EnableSkipCreateSQL()
	}
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	client.SetBatchDdlSize(cfg.DdlBatchSize)
	client.SetPlacementPolicyMode(cfg.WithPlacementPolicy)
	client.SetWithSysTable(cfg.WithSysTable)

	err := restore.CheckKeyspaceBREnable(ctx, client.GetPDClient())
	if err != nil {
		log.Warn("Keyspace BR is not supported in this cluster, fallback to legacy restore", zap.Error(err))
		client.SetRewriteMode(restore.RewriteModeLegacy)
	} else {
		client.SetRewriteMode(restore.RewriteModeKeyspace)
	}

	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
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
		dbName := db.Info.Name.L
		if dbCIStrName, ok := utils.GetSysDBCIStrName(db.Info.Name); utils.IsSysDB(dbCIStrName.O) && ok {
			dbName = dbCIStrName.L
		}
		schemasMap[utils.EncloseName(dbName)] = struct{}{}
		for _, table := range db.Tables {
			if table.Info == nil {
				// we may back up empty database.
				continue
			}
			tablesMap[utils.EncloseDBAndTable(dbName, table.Info.Name.L)] = struct{}{}
		}
	}
	restoreSchemas := cfg.Schemas
	restoreTables := cfg.Tables
	for schema := range restoreSchemas {
		schemaLName := strings.ToLower(schema)
		if _, ok := schemasMap[schemaLName]; !ok {
			return errors.Annotatef(berrors.ErrUndefinedRestoreDbOrTable,
				"[database: %v] has not been backup, please ensure you has input a correct database name", schema)
		}
	}
	for table := range restoreTables {
		tableLName := strings.ToLower(table)
		if _, ok := tablesMap[tableLName]; !ok {
			return errors.Annotatef(berrors.ErrUndefinedRestoreDbOrTable,
				"[table: %v] has not been backup, please ensure you has input a correct table name", table)
		}
	}
	return nil
}

func isFullRestore(cmdName string) bool {
	return cmdName == FullRestoreCmd
}

// IsStreamRestore checks the command is `restore point`
func IsStreamRestore(cmdName string) bool {
	return cmdName == PointRestoreCmd
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	if err := checkTaskExists(c, cfg); err != nil {
		return errors.Annotate(err, "failed to check task exits")
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = cfg.KeyspaceName
	})
	if IsStreamRestore(cmdName) {
		return RunStreamRestore(c, g, cmdName, cfg)
	}
	return runRestore(c, g, cmdName, cfg)
}

func runRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.Adjust()
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
	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()
	codec := mgr.GetStorage().GetCodec()

	mergeRegionSize := cfg.MergeSmallRegionSizeBytes
	mergeRegionCount := cfg.MergeSmallRegionKeyCount
	if mergeRegionSize == conn.DefaultMergeRegionSizeBytes &&
		mergeRegionCount == conn.DefaultMergeRegionKeyCount {
		// according to https://github.com/pingcap/tidb/issues/34167.
		// we should get the real config from tikv to adapt the dynamic region.
		httpCli := httputil.NewClient(mgr.GetTLSConfig())
		mergeRegionSize, mergeRegionCount = mgr.GetMergeRegionSizeAndCount(ctx, httpCli)
	}

	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, false)
	err = configureRestoreClient(ctx, client, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	// Init DB connection sessions
	err = client.Init(g, mgr.GetStorage())
	defer client.Close()

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
	if err = restore.CheckNewCollationEnable(backupMeta.GetNewCollationsEnabled(), g, mgr.GetStorage(), cfg.CheckRequirements); err != nil {
		return errors.Trace(err)
	}

	reader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.InitBackupMeta(c, backupMeta, u, reader); err != nil {
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
	restoreTS, err := client.GetTSWithRetry(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// todo: move this check into InitFullClusterRestore, we should move restore config into a separate package
	// to avoid import cycle problem which we won't do it in this pr, then refactor this
	//
	// if it's point restore and reached here, then cmdName=FullRestoreCmd and len(cfg.FullBackupStorage) > 0
	if cmdName == FullRestoreCmd && cfg.WithSysTable {
		client.InitFullClusterRestore(cfg.ExplicitFilter)
	}
	if client.IsFullClusterRestore() && client.HasBackedUpSysDB() {
		if err = client.CheckTargetClusterFresh(ctx); err != nil {
			return errors.Trace(err)
		}
		if err = client.CheckSysTableCompatibility(mgr.GetDomain(), tables); err != nil {
			return errors.Trace(err)
		}
	}

	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}
	g.Record("BackupTS", backupMeta.EndVersion)
	g.Record("RestoreTS", restoreTS)

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
	ddlJobs = restore.FilterDDLJobByRules(ddlJobs, restore.DDLJobBlockListRule)

	err = client.PreCheckTableTiFlashReplica(ctx, tables, cfg.tiflashRecorder)
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

	if client.GetSupportPolicy() {
		// create policy if backupMeta has policies.
		policies, err := client.GetPlacementPolicies()
		if err != nil {
			return errors.Trace(err)
		}
		if isFullRestore(cmdName) {
			// we should restore all policies during full restoration.
			err = client.CreatePolicies(ctx, policies)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			client.SetPolicyMap(policies)
		}
	}

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

	tableStream := client.GoCreateTables(ctx, mgr.GetDomain(), tables, newTS, errCh)

	if len(files) == 0 {
		log.Info("no files, empty databases and tables are restored")
		summary.SetSuccessStatus(true)
		// don't return immediately, wait all pipeline done.
	} else {
		oldKeyspace, _, err := tikv.DecodeKey(files[0].GetStartKey(), backupMeta.ApiVersion)
		if err != nil {
			return errors.Trace(err)
		}
		newKeyspace := codec.GetKeyspace()

		// If the API V2 data occurs in the restore process, the cluster must
		// support the keyspace rewrite mode.
		if (len(oldKeyspace) > 0 || len(newKeyspace) > 0) && client.GetRewriteMode() == restore.RewriteModeLegacy {
			return errors.Annotate(berrors.ErrRestoreModeMismatch, "cluster only supports legacy rewrite mode")
		}

		// Hijack the tableStream and rewrite the rewrite rules.
		tableStream = util.ChanMap(tableStream, func(t restore.CreatedTable) restore.CreatedTable {
			// Set the keyspace info for the checksum requests
			t.RewriteRule.OldKeyspace = oldKeyspace
			t.RewriteRule.NewKeyspace = newKeyspace

			for _, rule := range t.RewriteRule.Data {
				rule.OldKeyPrefix = append(append([]byte{}, oldKeyspace...), rule.OldKeyPrefix...)
				rule.NewKeyPrefix = codec.EncodeKey(rule.NewKeyPrefix)
			}
			return t
		})
	}

	if cfg.tiflashRecorder != nil {
		tableStream = util.ChanMap(tableStream, func(t restore.CreatedTable) restore.CreatedTable {
			if cfg.tiflashRecorder != nil {
				cfg.tiflashRecorder.Rewrite(t.OldTable.Info.ID, t.Table.ID)
			}
			return t
		})
	}

	tableFileMap := restore.MapTableToFiles(files)
	log.Debug("mapped table to files", zap.Any("result map", tableFileMap))

	rangeStream := restore.GoValidateFileRanges(
		ctx, tableStream, tableFileMap, mergeRegionSize, mergeRegionCount, errCh)

	rangeSize := restore.EstimateRangeSize(files)
	summary.CollectInt("restore ranges", rangeSize)
	log.Info("range and file prepared", zap.Int("file count", len(files)), zap.Int("range count", rangeSize))

	restoreSchedulers, err := restorePreWork(ctx, client, mgr, true)
	if err != nil {
		return errors.Trace(err)
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restorePostWork(ctx, client, restoreSchedulers)

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(ctx, mgr.PdController); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	// Restore sst files in batch.
	batchSize := mathutil.Clamp(int(cfg.Concurrency), defaultRestoreConcurrency, maxRestoreBatchSizeLimit)
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
	sender, err := restore.NewTiKVSender(ctx, client, updateCh, cfg.PDConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	manager := restore.NewBRContextManager(client)
	batcher, afterRestoreStream := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(batchSize)
	batcher.EnableAutoCommit(ctx, cfg.BatchFlushInterval)
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

	// Reset speed limit. ResetSpeedLimit must be called after client.InitBackupMeta has been called.
	defer func() {
		var resetErr error
		// In future we may need a mechanism to set speed limit in ttl. like what we do in switchmode. TODO
		for retry := 0; retry < resetSpeedLimitRetryTimes; retry++ {
			resetErr = client.ResetSpeedLimit(ctx)
			if resetErr != nil {
				log.Warn("failed to reset speed limit, retry it",
					zap.Int("retry time", retry), logutil.ShortError(resetErr))
				time.Sleep(time.Duration(retry+3) * time.Second)
				continue
			}
			break
		}
		if resetErr != nil {
			log.Error("failed to reset speed limit", zap.Error(resetErr))
		}
	}()

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
			close(outCh)
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

// filterRestoreFiles filters tables that can't be processed after applying cfg.TableFilter.MatchTable.
// if the db has no table that can be processed, the db will be filtered too.
func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backuppb.File, tables []*metautil.Table, dbs []*utils.Database) {
	for _, db := range client.GetDatabases() {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}
		if !cfg.TableFilter.MatchSchema(dbName) {
			continue
		}
		dbs = append(dbs, db)
		for _, table := range db.Tables {
			if table.Info == nil || !cfg.TableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			files = append(files, table.Files...)
			tables = append(tables, table)
		}
	}
	return
}

// restorePreWork executes some prepare work before restore.
// TODO make this function returns a restore post work.
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr, switchToImport bool) (pdutil.UndoFunc, error) {
	if client.IsOnline() {
		return pdutil.Nop, nil
	}

	if switchToImport {
		// Switch TiKV cluster to import mode (adjust rocksdb configuration).
		client.SwitchToImportMode(ctx)
	}

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
