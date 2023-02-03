// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	flagBackupTimeago    = "timeago"
	flagBackupTS         = "backupts"
	flagLastBackupTS     = "lastbackupts"
	flagCompressionType  = "compression"
	flagCompressionLevel = "compression-level"
	flagRemoveSchedulers = "remove-schedulers"
	flagIgnoreStats      = "ignore-stats"
	flagUseBackupMetaV2  = "use-backupmeta-v2"
	flagUseCheckpoint    = "use-checkpoint"
	flagKeyspaceName     = "keyspace-name"

	flagGCTTL = "gcttl"

	defaultBackupConcurrency = 4
	maxBackupConcurrency     = 256
	checkpointDefaultGCTTL   = 72 * 60 // 72 minutes
)

const (
	FullBackupCmd  = "Full Backup"
	DBBackupCmd    = "Database Backup"
	TableBackupCmd = "Table Backup"
	RawBackupCmd   = "Raw Backup"
	EBSBackupCmd   = "EBS Backup"
)

// CompressionConfig is the configuration for sst file compression.
type CompressionConfig struct {
	CompressionType  backuppb.CompressionType `json:"compression-type" toml:"compression-type"`
	CompressionLevel int32                    `json:"compression-level" toml:"compression-level"`
}

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo          time.Duration `json:"time-ago" toml:"time-ago"`
	BackupTS         uint64        `json:"backup-ts" toml:"backup-ts"`
	LastBackupTS     uint64        `json:"last-backup-ts" toml:"last-backup-ts"`
	GCTTL            int64         `json:"gc-ttl" toml:"gc-ttl"`
	RemoveSchedulers bool          `json:"remove-schedulers" toml:"remove-schedulers"`
	IgnoreStats      bool          `json:"ignore-stats" toml:"ignore-stats"`
	UseBackupMetaV2  bool          `json:"use-backupmeta-v2"`
	UseCheckpoint    bool          `json:"use-checkpoint" toml:"use-checkpoint"`
	CompressionConfig

	// for ebs-based backup
	FullBackupType      FullBackupType `json:"full-backup-type" toml:"full-backup-type"`
	VolumeFile          string         `json:"volume-file" toml:"volume-file"`
	SkipAWS             bool           `json:"skip-aws" toml:"skip-aws"`
	CloudAPIConcurrency uint           `json:"cloud-api-concurrency" toml:"cloud-api-concurrency"`
	ProgressFile        string         `json:"progress-file" toml:"progress-file"`
}

// DefineBackupFlags defines common flags for the backup command.
func DefineBackupFlags(flags *pflag.FlagSet) {
	flags.Duration(
		flagBackupTimeago, 0,
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	// TODO: remove experimental tag if it's stable
	flags.Uint64(flagLastBackupTS, 0, "(experimental) the last time backup ts,"+
		" use for incremental backup, support TSO only")
	flags.String(flagBackupTS, "", "the backup ts support TSO or datetime,"+
		" e.g. '400036290571534337', '2018-05-11 01:42:23'")
	flags.Int64(flagGCTTL, utils.DefaultBRGCSafePointTTL, "the TTL (in seconds) that PD holds for BR's GC safepoint")
	flags.String(flagCompressionType, "zstd",
		"backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
	flags.Int32(flagCompressionLevel, 0, "compression level used for sst file compression")

	flags.Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = flags.MarkHidden(flagRemoveSchedulers)

	// Disable stats by default. because of
	// 1. DumpStatsToJson is not stable
	// 2. It increases memory usage and might cause BR OOM.
	// TODO: we need a better way to backup/restore stats.
	flags.Bool(flagIgnoreStats, true, "ignore backup stats, used for test")
	// This flag is used for test. we should backup stats all the time.
	_ = flags.MarkHidden(flagIgnoreStats)

	flags.Bool(flagUseBackupMetaV2, false,
		"use backup meta v2 to store meta info")

	flags.String(flagKeyspaceName, "", "keyspace name for backup")
	// This flag will change the structure of backupmeta.
	// we must make sure the old three version of br can parse the v2 meta to keep compatibility.
	// so this flag should set to false for three version by default.
	// for example:
	// if we put this feature in v4.0.14, then v4.0.14 br can parse v2 meta
	// but will generate v1 meta due to this flag is false. the behaviour is as same as v4.0.15, v4.0.16.
	// finally v4.0.17 will set this flag to true, and generate v2 meta.
	_ = flags.MarkHidden(flagUseBackupMetaV2)

	flags.Bool(flagUseCheckpoint, true, "use checkpoint mode")
	_ = flags.MarkHidden(flagUseCheckpoint)
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	timeAgo, err := flags.GetDuration(flagBackupTimeago)
	if err != nil {
		return errors.Trace(err)
	}
	if timeAgo < 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "negative timeago is not allowed")
	}
	cfg.TimeAgo = timeAgo
	cfg.LastBackupTS, err = flags.GetUint64(flagLastBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	backupTS, err := flags.GetString(flagBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.BackupTS, err = ParseTSString(backupTS, false)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.UseBackupMetaV2, err = flags.GetBool(flagUseBackupMetaV2)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.UseCheckpoint, err = flags.GetBool(flagUseCheckpoint)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.LastBackupTS > 0 {
		// TODO: compatible with incremental backup
		cfg.UseCheckpoint = false
		log.Info("since incremental backup is used, turn off checkpoint mode")
	}
	if cfg.UseBackupMetaV2 {
		// TODO: compatible with backup meta v2, maybe just clean the meta files
		cfg.UseCheckpoint = false
		log.Info("since backup meta v2 is used, turn off checkpoint mode")
	}
	gcTTL, err := flags.GetInt64(flagGCTTL)
	if err != nil {
		return errors.Trace(err)
	}
	// if use checkpoint and gcTTL is the default value
	// update gcttl to checkpoint's default gc ttl
	if cfg.UseCheckpoint && gcTTL == utils.DefaultBRGCSafePointTTL {
		gcTTL = checkpointDefaultGCTTL
		log.Info("use checkpoint's default GC TTL", zap.Int64("GC TTL", gcTTL))
	}
	cfg.GCTTL = gcTTL

	compressionCfg, err := parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.IgnoreStats, err = flags.GetBool(flagIgnoreStats)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.KeyspaceName, err = flags.GetString(flagKeyspaceName)
	if err != nil {
		return errors.Trace(err)
	}

	if flags.Lookup(flagFullBackupType) != nil {
		// for backup full
		fullBackupType, err := flags.GetString(flagFullBackupType)
		if err != nil {
			return errors.Trace(err)
		}
		if !FullBackupType(fullBackupType).Valid() {
			return errors.New("invalid full backup type")
		}
		cfg.FullBackupType = FullBackupType(fullBackupType)
		cfg.SkipAWS, err = flags.GetBool(flagSkipAWS)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.CloudAPIConcurrency, err = flags.GetUint(flagCloudAPIConcurrency)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.VolumeFile, err = flags.GetString(flagBackupVolumeFile)
		if err != nil {
			return errors.Trace(err)
		}
		cfg.ProgressFile, err = flags.GetString(flagProgressFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// parseCompressionFlags parses the backup-related flags from the flag set.
func parseCompressionFlags(flags *pflag.FlagSet) (*CompressionConfig, error) {
	compressionStr, err := flags.GetString(flagCompressionType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	compressionType, err := parseCompressionType(compressionStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CompressionConfig{
		CompressionLevel: level,
		CompressionType:  compressionType,
	}, nil
}

// Adjust is use for BR(binary) and BR in TiDB.
// When new config was add and not included in parser.
// we should set proper value in this function.
// so that both binary and TiDB will use same default value.
func (cfg *BackupConfig) Adjust() {
	cfg.adjust()
	usingDefaultConcurrency := false
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultBackupConcurrency
		usingDefaultConcurrency = true
	}
	if cfg.Config.Concurrency > maxBackupConcurrency {
		cfg.Config.Concurrency = maxBackupConcurrency
	}
	if cfg.RateLimit != unlimited {
		// TiKV limits the upload rate by each backup request.
		// When the backup requests are sent concurrently,
		// the ratelimit couldn't work as intended.
		// Degenerating to sequentially sending backup requests to avoid this.
		if !usingDefaultConcurrency {
			logutil.WarnTerm("setting `--ratelimit` and `--concurrency` at the same time, "+
				"ignoring `--concurrency`: `--ratelimit` forces sequential (i.e. concurrency = 1) backup",
				zap.String("ratelimit", units.HumanSize(float64(cfg.RateLimit))+"/s"),
				zap.Uint32("concurrency-specified", cfg.Config.Concurrency))
		}
		cfg.Config.Concurrency = 1
	}

	if cfg.GCTTL == 0 {
		cfg.GCTTL = utils.DefaultBRGCSafePointTTL
	}
	// Use zstd as default
	if cfg.CompressionType == backuppb.CompressionType_UNKNOWN {
		cfg.CompressionType = backuppb.CompressionType_ZSTD
	}
	if cfg.CloudAPIConcurrency == 0 {
		cfg.CloudAPIConcurrency = defaultCloudAPIConcurrency
	}
}

// a rough hash for checkpoint checker
func (cfg *BackupConfig) Hash() ([]byte, error) {
	config := &BackupConfig{
		LastBackupTS:  cfg.LastBackupTS,
		IgnoreStats:   cfg.IgnoreStats,
		UseCheckpoint: cfg.UseCheckpoint,
		Config:        cfg.Config,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hash := sha256.Sum256(data)

	return hash[:], nil
}

func isFullBackup(cmdName string) bool {
	return cmdName == FullBackupCmd
}

// RunBackup starts a backup task inside the current goroutine.
func RunBackup(c context.Context, g glue.Glue, cmdName string, cfg *BackupConfig) error {
	cfg.Adjust()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = cfg.KeyspaceName
	})

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	skipStats := cfg.IgnoreStats
	// For backup, Domain is not needed if user ignores stats.
	// Domain loads all table info into memory. By skipping Domain, we save
	// lots of memory (about 500MB for 40K 40 fields YCSB tables).
	needDomain := !skipStats
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()
	// after version check, check the cluster whether support checkpoint mode
	if cfg.UseCheckpoint {
		err = version.CheckCheckpointSupport()
		if err != nil {
			log.Warn("unable to use checkpoint mode, fall back to normal mode", zap.Error(err))
			cfg.UseCheckpoint = false
		}
	}
	var statsHandle *handle.Handle
	if !skipStats {
		statsHandle = mgr.GetDomain().StatsHandle()
	}

	var newCollationEnable string
	err = g.UseOneShotSession(mgr.GetStorage(), !needDomain, func(se glue.Session) error {
		newCollationEnable, err = se.GetGlobalVariable(utils.GetTidbNewCollationEnabled())
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("get new_collations_enabled_on_first_bootstrap config from system table",
			zap.String(utils.GetTidbNewCollationEnabled(), newCollationEnable))
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	client := backup.NewBackupClient(ctx, mgr)

	// set cipher only for checkpoint
	client.SetCipher(&cfg.CipherInfo)

	opts := storage.ExternalStorageOptions{
		NoCredentials:            cfg.NoCreds,
		SendCredentials:          cfg.SendCreds,
		CheckS3ObjectLockOptions: true,
	}
	if err = client.SetStorageAndCheckNotInUse(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	// if checkpoint mode is unused at this time but there is checkpoint meta,
	// CheckCheckpoint will stop backing up
	cfgHash, err := cfg.Hash()
	if err != nil {
		return errors.Trace(err)
	}
	err = client.CheckCheckpoint(cfgHash)
	if err != nil {
		return errors.Trace(err)
	}
	err = client.SetLockFile(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	client.SetGCTTL(cfg.GCTTL)

	backupTS, err := client.GetTS(ctx, cfg.TimeAgo, cfg.BackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	g.Record("BackupTS", backupTS)
	safePointID := client.GetSafePointID()
	sp := utils.BRServiceSafePoint{
		BackupTS: backupTS,
		TTL:      client.GetGCTTL(),
		ID:       safePointID,
	}

	// use lastBackupTS as safePoint if exists
	isIncrementalBackup := cfg.LastBackupTS > 0
	if isIncrementalBackup {
		sp.BackupTS = cfg.LastBackupTS
	}

	log.Info("current backup safePoint job", zap.Object("safePoint", sp))
	cctx, gcSafePointKeeperCancel := context.WithCancel(ctx)
	gcSafePointKeeperRemovable := false
	defer func() {
		// don't reset the gc-safe-point if checkpoint mode is used and backup is not finished
		if cfg.UseCheckpoint && !gcSafePointKeeperRemovable {
			log.Info("skip removing gc-safepoint keeper for next retry", zap.String("gc-id", sp.ID))
			return
		}
		log.Info("start to remove gc-safepoint keeper")
		// close the gc safe point keeper at first
		gcSafePointKeeperCancel()
		// set the ttl to 0 to remove the gc-safe-point
		sp.TTL = 0
		if err := utils.UpdateServiceSafePoint(ctx, mgr.GetPDClient(), sp); err != nil {
			log.Warn("failed to update service safe point, backup may fail if gc triggered",
				zap.Error(err),
			)
		}
		log.Info("finish removing gc-safepoint keeper")
	}()
	err = utils.StartServiceSafePointKeeper(cctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.RemoveSchedulers {
		log.Debug("removing some PD schedulers")
		restore, e := mgr.RemoveSchedulers(ctx)
		defer func() {
			if ctx.Err() != nil {
				log.Warn("context canceled, doing clean work with background context")
				ctx = context.Background()
			}
			if restoreE := restore(ctx); restoreE != nil {
				log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
			}
		}()
		if e != nil {
			return errors.Trace(err)
		}
	}

	req := backuppb.BackupRequest{
		ClusterId:        client.GetClusterID(),
		StartVersion:     cfg.LastBackupTS,
		EndVersion:       backupTS,
		RateLimit:        cfg.RateLimit,
		StorageBackend:   client.GetStorageBackend(),
		Concurrency:      defaultBackupConcurrency,
		CompressionType:  cfg.CompressionType,
		CompressionLevel: cfg.CompressionLevel,
		CipherInfo:       &cfg.CipherInfo,
	}
	brVersion := g.GetVersion()
	clusterVersion, err := mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ranges, schemas, policies, err := client.BuildBackupRangeAndSchema(mgr.GetStorage(), cfg.TableFilter, backupTS, isFullBackup(cmdName))
	if err != nil {
		return errors.Trace(err)
	}
	// Add keyspace prefix to BackupRequest
	for i := range ranges {
		start, end := ranges[i].StartKey, ranges[i].EndKey
		ranges[i].StartKey, ranges[i].EndKey = mgr.GetStorage().GetCodec().EncodeRange(start, end)
	}

	// Metafile size should be less than 64MB.
	metawriter := metautil.NewMetaWriter(client.GetStorage(),
		metautil.MetaFileSize, cfg.UseBackupMetaV2, "", &cfg.CipherInfo)
	// Hack way to update backupmeta.
	metawriter.Update(func(m *backuppb.BackupMeta) {
		m.StartVersion = req.StartVersion
		m.EndVersion = req.EndVersion
		m.IsRawKv = req.IsRawKv
		m.ClusterId = req.ClusterId
		m.ClusterVersion = clusterVersion
		m.BrVersion = brVersion
		m.NewCollationsEnabled = newCollationEnable
		m.ApiVersion = mgr.GetStorage().GetCodec().GetAPIVersion()
	})

	log.Info("get placement policies", zap.Int("count", len(policies)))
	if len(policies) != 0 {
		metawriter.Update(func(m *backuppb.BackupMeta) {
			m.Policies = policies
		})
	}

	// nothing to backup
	if len(ranges) == 0 {
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to backup, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))

		err = metawriter.FlushBackupMeta(ctx)
		if err == nil {
			summary.SetSuccessStatus(true)
		}
		return err
	}

	if isIncrementalBackup {
		if backupTS <= cfg.LastBackupTS {
			log.Error("LastBackupTS is larger or equal to current TS")
			return errors.Annotate(berrors.ErrInvalidArgument, "LastBackupTS is larger or equal to current TS")
		}
		err = utils.CheckGCSafePoint(ctx, mgr.GetPDClient(), cfg.LastBackupTS)
		if err != nil {
			log.Error("Check gc safepoint for last backup ts failed", zap.Error(err))
			return errors.Trace(err)
		}

		metawriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
		err = backup.WriteBackupDDLJobs(metawriter, g, mgr.GetStorage(), cfg.LastBackupTS, backupTS, needDomain)
		if err != nil {
			return errors.Trace(err)
		}
		if err = metawriter.FinishWriteMetas(ctx, metautil.AppendDDL); err != nil {
			return errors.Trace(err)
		}
	}

	summary.CollectInt("backup total ranges", len(ranges))

	var updateCh glue.Progress
	var unit backup.ProgressUnit
	if len(ranges) < 100 {
		unit = backup.RegionUnit
		// The number of regions need to backup
		approximateRegions := 0
		for _, r := range ranges {
			var regionCount int
			regionCount, err = mgr.GetRegionCount(ctx, r.StartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			approximateRegions += regionCount
		}
		// Redirect to log if there is no log file to avoid unreadable output.
		updateCh = g.StartProgress(
			ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)
		summary.CollectInt("backup total regions", approximateRegions)
	} else {
		unit = backup.RangeUnit
		// To reduce the costs, we can use the range as unit of progress.
		updateCh = g.StartProgress(
			ctx, cmdName, int64(len(ranges)), !cfg.LogProgress)
	}

	progressCount := 0
	progressCallBack := func(callBackUnit backup.ProgressUnit) {
		if unit == callBackUnit {
			updateCh.Inc()
			progressCount++
			failpoint.Inject("progress-call-back", func(v failpoint.Value) {
				log.Info("failpoint progress-call-back injected")
				if fileName, ok := v.(string); ok {
					f, osErr := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
					if osErr != nil {
						log.Warn("failed to create file", zap.Error(osErr))
					}
					msg := []byte(fmt.Sprintf("%s:%d\n", unit, progressCount))
					_, err = f.Write(msg)
					if err != nil {
						log.Warn("failed to write data to file", zap.Error(err))
					}
				}
			})
		}
	}

	if cfg.UseCheckpoint {
		if err = client.StartCheckpointRunner(ctx, cfgHash, backupTS, ranges, safePointID, progressCallBack); err != nil {
			return errors.Trace(err)
		}
		defer func() {
			if !gcSafePointKeeperRemovable {
				log.Info("wait for flush checkpoint...")
				client.WaitForFinishCheckpoint(ctx)
			}
		}()
	}
	metawriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	err = client.BackupRanges(ctx, ranges, req, uint(cfg.Concurrency), metawriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup has finished
	updateCh.Close()

	err = metawriter.FinishWriteMetas(ctx, metautil.AppendDataFile)
	if err != nil {
		return errors.Trace(err)
	}

	skipChecksum := !cfg.Checksum || isIncrementalBackup
	checksumProgress := int64(schemas.Len())
	if skipChecksum {
		checksumProgress = 1
		if isIncrementalBackup {
			// Since we don't support checksum for incremental data, fast checksum should be skipped.
			log.Info("Skip fast checksum in incremental backup")
		} else {
			// When user specified not to calculate checksum, don't calculate checksum.
			log.Info("Skip fast checksum")
		}
	}
	updateCh = g.StartProgress(ctx, "Checksum", checksumProgress, !cfg.LogProgress)
	schemasConcurrency := uint(mathutil.Min(backup.DefaultSchemaConcurrency, schemas.Len()))

	err = schemas.BackupSchemas(
		ctx, metawriter, client.GetCheckpointRunner(), mgr.GetStorage(), statsHandle, backupTS, schemasConcurrency, cfg.ChecksumConcurrency, skipChecksum, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	err = metawriter.FlushBackupMeta(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// Since backupmeta is flushed on the external storage,
	// we can remove the gc safepoint keeper
	gcSafePointKeeperRemovable = true

	// Checksum has finished, close checksum progress.
	updateCh.Close()

	if !skipChecksum {
		// Check if checksum from files matches checksum from coprocessor.
		err = checksum.FastChecksum(ctx, metawriter.Backupmeta(), client.GetStorage(), &cfg.CipherInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	archiveSize := metawriter.ArchiveSize()
	g.Record(summary.BackupDataSize, archiveSize)
	//backup from tidb will fetch a general Size issue https://github.com/pingcap/tidb/issues/27247
	g.Record("Size", archiveSize)
	failpoint.Inject("s3-outage-during-writing-file", func(v failpoint.Value) {
		log.Info("failpoint s3-outage-during-writing-file injected, " +
			"process will sleep for 3s and notify the shell to kill s3 service.")
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
	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// ParseTSString port from tidb setSnapshotTS.
func ParseTSString(ts string, tzCheck bool) (uint64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	if tso, err := strconv.ParseUint(ts, 10, 64); err == nil {
		return tso, nil
	}

	loc := time.Local
	sc := &stmtctx.StatementContext{
		TimeZone: loc,
	}
	if tzCheck {
		tzIdx, _, _, _, _ := types.GetTimezone(ts)
		if tzIdx < 0 {
			return 0, errors.Errorf("must set timezone when using datetime format ts, e.g. '2018-05-11 01:42:23+0800'")
		}
	}
	t, err := types.ParseTime(sc, ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}
	t1, err := t.GoTime(loc)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return oracle.GoTimeToTS(t1), nil
}

func parseCompressionType(s string) (backuppb.CompressionType, error) {
	var ct backuppb.CompressionType
	switch s {
	case "lz4":
		ct = backuppb.CompressionType_LZ4
	case "snappy":
		ct = backuppb.CompressionType_SNAPPY
	case "zstd":
		ct = backuppb.CompressionType_ZSTD
	default:
		return backuppb.CompressionType_UNKNOWN, errors.Annotatef(berrors.ErrInvalidArgument, "invalid compression type '%s'", s)
	}
	return ct, nil
}
