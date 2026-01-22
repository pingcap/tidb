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
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/conn"
	connutil "github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	flagBackupTimeago    = "timeago"
	flagBackupTS         = "backupts"
	flagLastBackupTS     = "lastbackupts"
	flagCompressionType  = "compression"
	flagCompressionLevel = "compression-level"
	flagRemoveSchedulers = "remove-schedulers"
	flagRangeLimit       = "range-limit"
	flagIgnoreStats      = "ignore-stats"
	flagUseBackupMetaV2  = "use-backupmeta-v2"
	flagUseCheckpoint    = "use-checkpoint"
	flagKeyspaceName     = "keyspace-name"
	flagReplicaReadLabel = "replica-read-label"
	flagTableConcurrency = "table-concurrency"

	flagParquetEnable                = "export-parquet"
	flagParquetOutput                = "parquet-output"
	flagParquetOutputPrefix          = "parquet-output-prefix"
	flagParquetRowGroupSize          = "parquet-row-group-size"
	flagParquetCompression           = "parquet-compression"
	flagParquetWriteIcebergManifest  = "parquet-write-iceberg-manifest"
	flagParquetIcebergWarehouse      = "parquet-iceberg-warehouse"
	flagParquetIcebergNamespace      = "parquet-iceberg-namespace"
	flagParquetIcebergTable          = "parquet-iceberg-table"
	flagParquetIcebergManifestPrefix = "parquet-iceberg-manifest-prefix"
	flagParquetOnly                  = "parquet-only"

	flagGCTTL = "gcttl"

	defaultBackupConcurrency   = 4
	maxBackupConcurrency       = 256
	defaultParquetRowGroupSize = 8192
	maxParquetRowGroupSize     = 1_000_000
)

const (
	FullBackupCmd  = "Full Backup"
	DBBackupCmd    = "Database Backup"
	TableBackupCmd = "Table Backup"
	RawBackupCmd   = "Raw Backup"
	TxnBackupCmd   = "Txn Backup"
)

// CompressionConfig is the configuration for sst file compression.
type CompressionConfig struct {
	CompressionType  backuppb.CompressionType `json:"compression-type" toml:"compression-type"`
	CompressionLevel int32                    `json:"compression-level" toml:"compression-level"`
}

// ParquetExportConfig controls the optional SST-to-Parquet conversion.
type ParquetExportConfig struct {
	Enable                bool   `json:"enable" toml:"enable"`
	Output                string `json:"output" toml:"output"`
	OutputPrefix          string `json:"output-prefix" toml:"output-prefix"`
	RowGroupSize          uint64 `json:"row-group-size" toml:"row-group-size"`
	Compression           string `json:"compression" toml:"compression"`
	WriteIcebergManifest  bool   `json:"write-iceberg-manifest" toml:"write-iceberg-manifest"`
	IcebergWarehouse      string `json:"iceberg-warehouse" toml:"iceberg-warehouse"`
	IcebergNamespace      string `json:"iceberg-namespace" toml:"iceberg-namespace"`
	IcebergTable          string `json:"iceberg-table" toml:"iceberg-table"`
	IcebergManifestPrefix string `json:"iceberg-manifest-prefix" toml:"iceberg-manifest-prefix"`
	Only                  bool   `json:"only" toml:"only"`
}

// shouldRun returns true when the Parquet export flow is enabled.
func (p *ParquetExportConfig) shouldRun() bool {
	return p != nil && p.Enable
}

// normalize applies defaults and cleans up user-facing options.
func (p *ParquetExportConfig) normalize() {
	if p.Only {
		p.Enable = true
	}
	if p.RowGroupSize == 0 {
		p.RowGroupSize = defaultParquetRowGroupSize
	}
	if p.OutputPrefix == "" {
		p.OutputPrefix = "parquet"
	}
	p.OutputPrefix = strings.Trim(p.OutputPrefix, "/")
	if p.OutputPrefix == "" {
		p.OutputPrefix = "parquet"
	}
	if p.Compression == "" {
		p.Compression = "snappy"
	}
	p.Compression = strings.ToLower(strings.TrimSpace(p.Compression))
	if p.IcebergManifestPrefix == "" {
		p.IcebergManifestPrefix = "manifest"
	}
}

// validate checks configuration sanity before running the export.
func (p *ParquetExportConfig) validate() error {
	if !p.shouldRun() {
		return nil
	}
	if p.Output == "" {
		return errors.Annotate(berrors.ErrInvalidArgument, "parquet output storage must be specified when --export-parquet is enabled")
	}
	if p.RowGroupSize == 0 || p.RowGroupSize > maxParquetRowGroupSize {
		return errors.Annotatef(berrors.ErrInvalidArgument, "parquet row group size must be between 1 and %d", maxParquetRowGroupSize)
	}
	switch p.Compression {
	case "snappy", "zstd", "gzip", "brotli", "lz4raw", "lz4", "none", "uncompressed":
	default:
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"unsupported parquet compression codec %q (supported: snappy|zstd|gzip|brotli|lz4raw|lz4|none|uncompressed)", p.Compression)
	}
	if p.WriteIcebergManifest {
		if p.IcebergWarehouse == "" || p.IcebergNamespace == "" || p.IcebergTable == "" {
			return errors.Annotate(berrors.ErrInvalidArgument, "iceberg warehouse, namespace and table are required when writing manifests")
		}
	}
	return nil
}

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo          time.Duration     `json:"time-ago" toml:"time-ago"`
	BackupTS         uint64            `json:"backup-ts" toml:"backup-ts"`
	LastBackupTS     uint64            `json:"last-backup-ts" toml:"last-backup-ts"`
	GCTTL            int64             `json:"gc-ttl" toml:"gc-ttl"`
	RemoveSchedulers bool              `json:"remove-schedulers" toml:"remove-schedulers"`
	RangeLimit       int               `json:"range-limit" toml:"range-limit"`
	IgnoreStats      bool              `json:"ignore-stats" toml:"ignore-stats"`
	UseBackupMetaV2  bool              `json:"use-backupmeta-v2"`
	UseCheckpoint    bool              `json:"use-checkpoint" toml:"use-checkpoint"`
	ReplicaReadLabel map[string]string `json:"replica-read-label" toml:"replica-read-label"`
	TableConcurrency uint              `json:"table-concurrency" toml:"table-concurrency"`
	CompressionConfig
	Parquet ParquetExportConfig `json:"parquet" toml:"parquet"`

	// for ebs-based backup
	FullBackupType          FullBackupType `json:"full-backup-type" toml:"full-backup-type"`
	VolumeFile              string         `json:"volume-file" toml:"volume-file"`
	SkipAWS                 bool           `json:"skip-aws" toml:"skip-aws"`
	CloudAPIConcurrency     uint           `json:"cloud-api-concurrency" toml:"cloud-api-concurrency"`
	ProgressFile            string         `json:"progress-file" toml:"progress-file"`
	SkipPauseGCAndScheduler bool           `json:"skip-pause-gc-and-scheduler" toml:"skip-pause-gc-and-scheduler"`
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

	flags.Uint32(flagConcurrency, 4,
		"Controls how many backup requests are sent out in parallel to one TiKV node. "+
			"This doesn't directly impact performance — keeping the default is fine in most cases. "+
			"Change TiKV's 'backup.num-threads' to adjust actual backup throughput.")

	flags.Uint(flagTableConcurrency, backup.DefaultSchemaConcurrency, "The size of a BR thread pool used for backup table metas, "+
		"including tableInfo/checksum and stats.")

	flags.Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = flags.MarkHidden(flagRemoveSchedulers)

	flags.Int(flagRangeLimit, backup.RangesSentThreshold, "limits the number of ranges marshaled at the same time when sent to many TiKVs.")

	// Disable stats by default.
	// TODO: we need a better way to backup/restore stats.
	flags.Bool(flagIgnoreStats, true, "ignore backup stats")

	flags.Bool(flagUseBackupMetaV2, true,
		"use backup meta v2 to store meta info")

	flags.String(flagKeyspaceName, "", "keyspace name for backup")
	// This flag will change the structure of backupmeta.
	// we must make sure the old three version of br can parse the v2 meta to keep compatibility.
	// so this flag should set to false for three version by default.
	// for example:
	// if we put this feature in v4.0.14, then v4.0.14 br can parse v2 meta
	// but will generate v1 meta due to this flag is false. the behaviour is as same as v4.0.15, v4.0.16.
	// finally v4.0.17 will set this flag to true, and generate v2 meta.
	//
	// the version currently is v7.4.0, the flag can be set to true as default value.
	// _ = flags.MarkHidden(flagUseBackupMetaV2)

	flags.Bool(flagUseCheckpoint, true, "use checkpoint mode")
	_ = flags.MarkHidden(flagUseCheckpoint)

	flags.String(flagReplicaReadLabel, "", "specify the label of the stores to be used for backup, e.g. 'label_key:label_value'")

	flags.Bool(flagParquetEnable, false,
		"also convert the finished backup into Parquet files via the TiKV backup RPC; defaults to storing the Parquet files in the same storage as --storage")
	flags.String(flagParquetOutput, "",
		"optional destination storage for Parquet output; leave empty to reuse --storage")
	flags.String(flagParquetOutputPrefix, "parquet",
		"path prefix for Parquet files under the destination storage")
	flags.Uint64(flagParquetRowGroupSize, defaultParquetRowGroupSize,
		"row group size used by the Parquet exporter")
	flags.String(flagParquetCompression, "snappy",
		"compression codec for Parquet exporter (snappy|zstd|gzip|brotli|lz4raw|lz4|none|uncompressed)")
	flags.Bool(flagParquetWriteIcebergManifest, false,
		"emit Iceberg manifest files referencing the generated Parquet files")
	flags.String(flagParquetIcebergWarehouse, "",
		"Iceberg warehouse URI used when --parquet-write-iceberg-manifest is set")
	flags.String(flagParquetIcebergNamespace, "",
		"Iceberg namespace used when --parquet-write-iceberg-manifest is set")
	flags.String(flagParquetIcebergTable, "",
		"Iceberg table name used when --parquet-write-iceberg-manifest is set")
	flags.String(flagParquetIcebergManifestPrefix, "manifest",
		"manifest file prefix when --parquet-write-iceberg-manifest is set")
	flags.Bool(flagParquetOnly, false,
		"skip generating new SST backups and only run the Parquet exporter against the destination specified by --storage")
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupConfig) ParseFromFlags(flags *pflag.FlagSet, skipCommonConfig bool) error {
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
	cfg.Parquet.Enable, err = flags.GetBool(flagParquetEnable)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.Output, err = flags.GetString(flagParquetOutput)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.OutputPrefix, err = flags.GetString(flagParquetOutputPrefix)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.RowGroupSize, err = flags.GetUint64(flagParquetRowGroupSize)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.Compression, err = flags.GetString(flagParquetCompression)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.WriteIcebergManifest, err = flags.GetBool(flagParquetWriteIcebergManifest)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.IcebergWarehouse, err = flags.GetString(flagParquetIcebergWarehouse)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.IcebergNamespace, err = flags.GetString(flagParquetIcebergNamespace)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.IcebergTable, err = flags.GetString(flagParquetIcebergTable)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.IcebergManifestPrefix, err = flags.GetString(flagParquetIcebergManifestPrefix)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Parquet.Only, err = flags.GetBool(flagParquetOnly)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.LastBackupTS > 0 {
		// TODO: compatible with incremental backup
		cfg.UseCheckpoint = false
		log.Info("since incremental backup is used, turn off checkpoint mode")
	}
	gcTTL, err := flags.GetInt64(flagGCTTL)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GCTTL = gcTTL
	cfg.Concurrency, err = flags.GetUint32(flagConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.TableConcurrency, err = flags.GetUint(flagTableConcurrency); err != nil {
		return errors.Trace(err)
	}

	compressionCfg, err := parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	// parse common flags if needed
	if !skipCommonConfig {
		if err = cfg.Config.ParseFromFlags(flags); err != nil {
			return errors.Trace(err)
		}
	}

	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RangeLimit, err = flags.GetInt(flagRangeLimit)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.RangeLimit <= 0 {
		return errors.Errorf("the parameter `--range-limit` should be larger than 0")
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
		cfg.SkipPauseGCAndScheduler, err = flags.GetBool(flagOperatorPausedGCAndSchedulers)
		if err != nil {
			return errors.Trace(err)
		}
	}

	cfg.ReplicaReadLabel, err = parseReplicaReadLabelFlag(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.Parquet.Output != "" && !cfg.Parquet.Enable {
		log.Warn("parquet export enabled because parquet output is set",
			zap.String("parquet-output", cfg.Parquet.Output),
		)
		cfg.Parquet.Enable = true
	}
	if cfg.Parquet.Enable && cfg.Parquet.Output == "" && cfg.Storage != "" {
		log.Warn("parquet output not set; falling back to --storage",
			zap.String("storage", cfg.Storage),
		)
		cfg.Parquet.Output = cfg.Storage
	}
	cfg.Parquet.normalize()
	if err := cfg.Parquet.validate(); err != nil {
		return errors.Trace(err)
	}
	if cfg.Parquet.Only && len(cfg.PD) == 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "--pd must be specified when --parquet-only is set")
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

type immutableBackupConfig struct {
	LastBackupTS  uint64 `json:"last-backup-ts"`
	IgnoreStats   bool   `json:"ignore-stats"`
	UseCheckpoint bool   `json:"use-checkpoint"`

	objstore.BackendOptions
	Storage      string              `json:"storage"`
	PD           []string            `json:"pd"`
	SendCreds    bool                `json:"send-credentials-to-tikv"`
	NoCreds      bool                `json:"no-credentials"`
	FilterStr    []string            `json:"filter-strings"`
	CipherInfo   backuppb.CipherInfo `json:"cipher"`
	KeyspaceName string              `json:"keyspace-name"`
}

// a rough hash for checkpoint checker
func (cfg *BackupConfig) Hash() ([]byte, error) {
	config := &immutableBackupConfig{
		LastBackupTS:  cfg.LastBackupTS,
		IgnoreStats:   cfg.IgnoreStats,
		UseCheckpoint: cfg.UseCheckpoint,

		BackendOptions: cfg.BackendOptions,
		Storage:        cfg.Storage,
		PD:             cfg.PD,
		SendCreds:      cfg.SendCreds,
		NoCreds:        cfg.NoCreds,
		FilterStr:      cfg.FilterStr,
		CipherInfo:     cfg.CipherInfo,
		KeyspaceName:   cfg.KeyspaceName,
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

	isIncrementalBackup := cfg.LastBackupTS > 0
	skipChecksum := !cfg.Checksum || isIncrementalBackup
	skipStats := cfg.IgnoreStats
	// For backup, Domain is not needed if user ignores stats.
	// Domain loads all table info into memory. By skipping Domain, we save
	// lots of memory (about 500MB for 40K 40 fields YCSB tables).
	needDomain := !skipStats
	if cfg.Parquet.Only {
		needDomain = false
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	if cfg.Parquet.Only {
		if cfg.Storage == "" {
			return errors.Annotate(berrors.ErrInvalidArgument, "--storage must be specified when --parquet-only is set")
		}
		inputBackend, err := objstore.ParseBackend(cfg.Storage, &cfg.BackendOptions)
		if err != nil {
			return errors.Trace(err)
		}
		if err := cfg.runParquetExport(ctx, mgr, inputBackend); err != nil {
			return err
		}
		summary.SetSuccessStatus(true)
		return nil
	}

	u, err := objstore.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	// if use noop as external storage, turn off the checkpoint mode
	if u.GetNoop() != nil {
		log.Info("since noop external storage is used, turn off checkpoint mode")
		cfg.UseCheckpoint = false
	}
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
		log.Info(fmt.Sprintf("get %s config from mysql.tidb table", utils.TidbNewCollationEnabled),
			zap.String(utils.GetTidbNewCollationEnabled(), newCollationEnable))
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	client := backup.NewTableBackupClient(ctx, mgr)

	// set cipher only for checkpoint
	client.SetCipher(&cfg.CipherInfo)
	// set skip checksum status
	client.SetSkipChecksum(skipChecksum)

	opts := storeapi.Options{
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
	// if use checkpoint and gcTTL is the default value
	// update gcttl to checkpoint's default gc ttl
	if cfg.UseCheckpoint && cfg.GCTTL == utils.DefaultBRGCSafePointTTL {
		cfg.GCTTL = utils.DefaultCheckpointGCSafePointTTL
		log.Info("use checkpoint's default GC TTL", zap.Int64("GC TTL", cfg.GCTTL))
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
		ReplicaRead:      len(cfg.ReplicaReadLabel) != 0,
		Context: &kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: "", // TODO,
			},
			RequestSource: kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR),
		},
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

	// check on ranges and schemas and if nothing to back up do early return
	if len(ranges) == 0 && (schemas == nil || schemas.Len() == 0) {
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
	progressTotalCount, progressUnit, err := getProgressCountOfRanges(ctx, mgr, ranges)
	if err != nil {
		return errors.Trace(err)
	}
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx, cmdName, int64(progressTotalCount), !cfg.LogProgress)

	progressCount := uint64(0)
	progressCallBack := func(callBackUnit backup.ProgressUnit) {
		if progressUnit == callBackUnit {
			updateCh.Inc()
			failpoint.Inject("progress-call-back", func(v failpoint.Value) {
				log.Info("failpoint progress-call-back injected")
				atomic.AddUint64(&progressCount, 1)
				if fileName, ok := v.(string); ok {
					f, osErr := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
					if osErr != nil {
						log.Warn("failed to create file", zap.Error(osErr))
					}
					msg := fmt.Appendf(nil, "%s:%d\n", progressUnit, atomic.LoadUint64(&progressCount))
					_, err = f.Write(msg)
					if err != nil {
						log.Warn("failed to write data to file", zap.Error(err))
					}
				}
			})
		}
	}

	if cfg.UseCheckpoint {
		if err = client.StartCheckpointRunner(ctx, cfgHash, backupTS, safePointID, progressCallBack); err != nil {
			return errors.Trace(err)
		}
		defer func() {
			if !gcSafePointKeeperRemovable {
				log.Info("wait for flush checkpoint...")
				client.WaitForFinishCheckpoint(ctx, true)
			} else {
				log.Info("start to remove checkpoint data for backup")
				client.WaitForFinishCheckpoint(ctx, false)
				if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, client.GetStorage()); removeErr != nil {
					log.Warn("failed to remove checkpoint data for backup", zap.Error(removeErr))
				} else {
					log.Info("the checkpoint data for backup is removed.")
				}
			}
		}()
	}

	failpoint.Inject("s3-outage-during-writing-file", func(v failpoint.Value) {
		log.Info("failpoint s3-outage-during-writing-file injected, " +
			"process will sleep for 5s and notify the shell to kill s3 service.")
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
		}
		time.Sleep(5 * time.Second)
	})

	metawriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	checksumMap, err := client.BackupRanges(ctx, ranges, req, uint(cfg.Concurrency), cfg.RangeLimit, cfg.ReplicaReadLabel, metawriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup has finished
	updateCh.Close()

	err = metawriter.FinishWriteMetas(ctx, metautil.AppendDataFile)
	if err != nil {
		return errors.Trace(err)
	}

	var checksumProgress int64 = 0
	// if checksumMap is not empty, then checksumProgress will be set to len(schemas)
	if len(checksumMap) > 0 {
		checksumProgress = int64(schemas.Len())
	}

	if skipChecksum {
		if isIncrementalBackup {
			// Since we don't support checksum for incremental data, fast checksum should be skipped.
			log.Info("Skip fast checksum in incremental backup")
		} else {
			// When user specified not to calculate checksum, don't calculate checksum.
			log.Info("Skip fast checksum")
		}
	}
	updateCh = g.StartProgress(ctx, "Checksum", checksumProgress, !cfg.LogProgress)

	if schemas != nil && schemas.Len() > 0 {
		schemasConcurrency := min(cfg.TableConcurrency, uint(schemas.Len()))
		err = schemas.BackupSchemas(
			ctx, metawriter, client.GetCheckpointRunner(), mgr.GetStorage(), statsHandle, backupTS, checksumMap, schemasConcurrency, cfg.ChecksumConcurrency, skipChecksum, updateCh)
		if err != nil {
			return errors.Trace(err)
		}
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

	archiveSize := metawriter.ArchiveSize()
	g.Record(summary.BackupDataSize, archiveSize)
	//backup from tidb will fetch a general Size issue https://github.com/pingcap/tidb/issues/27247
	g.Record("Size", archiveSize)
	// Set task summary to success status.
	if err := cfg.runParquetExport(ctx, mgr, client.GetStorageBackend()); err != nil {
		return err
	}
	summary.SetSuccessStatus(true)
	return nil
}

// runParquetExport invokes TiKV to convert SST backup files to Parquet output.
func (cfg *BackupConfig) runParquetExport(ctx context.Context, mgr *conn.Mgr, inputBackend *backuppb.StorageBackend) error {
	if !cfg.Parquet.shouldRun() {
		return nil
	}
	outputBackend, err := objstore.ParseBackend(cfg.Parquet.Output, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), connutil.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	if len(stores) == 0 {
		return errors.Annotate(berrors.ErrPDInvalidResponse, "no available TiKV stores for parquet export")
	}
	store := stores[0]
	cli, err := mgr.GetBackupClient(ctx, store.Id)
	if err != nil {
		return errors.Trace(err)
	}
	req := &backuppb.ParquetExportRequest{
		InputStorage:          inputBackend,
		OutputStorage:         outputBackend,
		BackupMeta:            metautil.MetaFile,
		OutputPrefix:          cfg.Parquet.OutputPrefix,
		RowGroupSize:          cfg.Parquet.RowGroupSize,
		Compression:           cfg.Parquet.Compression,
		WriteIcebergManifest:  cfg.Parquet.WriteIcebergManifest,
		IcebergWarehouse:      cfg.Parquet.IcebergWarehouse,
		IcebergNamespace:      cfg.Parquet.IcebergNamespace,
		IcebergTable:          cfg.Parquet.IcebergTable,
		IcebergManifestPrefix: cfg.Parquet.IcebergManifestPrefix,
	}
	log.Info("starting parquet export via backup RPC",
		zap.String("tikv", store.Address),
		zap.String("output", cfg.Parquet.Output),
		zap.String("prefix", cfg.Parquet.OutputPrefix),
		zap.Uint64("row-group-size", cfg.Parquet.RowGroupSize),
		zap.String("compression", cfg.Parquet.Compression))
	resp, err := cli.ParquetExport(ctx, req)
	if err != nil {
		return errors.Annotate(err, "parquet export")
	}
	if respErr := resp.GetError(); respErr != nil && respErr.GetMsg() != "" {
		return errors.Annotatef(berrors.ErrInvalidArgument, "parquet export failed: %s", respErr.GetMsg())
	}
	log.Info("parquet export finished",
		zap.String("tikv", store.Address),
		zap.Uint64("files", resp.GetFileCount()),
		zap.Uint64("rows", resp.GetTotalRows()),
		zap.Uint64("bytes", resp.GetTotalBytes()))
	return nil
}

func getProgressCountOfRanges(
	ctx context.Context,
	mgr *conn.Mgr,
	ranges []rtree.KeyRange,
) (int, backup.ProgressUnit, error) {
	if len(ranges) > 1000 {
		return len(ranges), backup.UnitRange, nil
	}
	failpoint.Inject("progress-call-back", func(_ failpoint.Value) {
		if len(ranges) > 100 {
			failpoint.Return(len(ranges), backup.UnitRange, nil)
		}
	})
	// The number of regions need to backup
	approximateRegions := 0
	for _, r := range ranges {
		regionCount, err := mgr.GetRegionCount(ctx, r.StartKey, r.EndKey)
		if err != nil {
			return 0, backup.UnitRegion, errors.Trace(err)
		}
		approximateRegions += regionCount
	}
	summary.CollectInt("backup total regions", approximateRegions)
	return approximateRegions, backup.UnitRegion, nil
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
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	if tzCheck {
		tzIdx, _, _, _, _ := types.GetTimezone(ts)
		if tzIdx < 0 {
			return 0, errors.Errorf("must set timezone when using datetime format ts, e.g. '2018-05-11 01:42:23+0800'")
		}
	}
	t, err := types.ParseTime(sc.TypeCtx(), ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}
	t1, err := t.GoTime(loc)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return oracle.GoTimeToTS(t1), nil
}

func DefaultBackupConfig(commonConfig Config) BackupConfig {
	fs := pflag.NewFlagSet("dummy", pflag.ContinueOnError)
	DefineBackupFlags(fs)
	cfg := BackupConfig{}
	err := cfg.ParseFromFlags(fs, true)
	if err != nil {
		log.Panic("failed to parse backup flags to config", zap.Error(err))
	}
	cfg.Config = commonConfig
	return cfg
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

func parseReplicaReadLabelFlag(flags *pflag.FlagSet) (map[string]string, error) {
	replicaReadLabelStr, err := flags.GetString(flagReplicaReadLabel)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if replicaReadLabelStr == "" {
		return nil, nil
	}
	kv := strings.Split(replicaReadLabelStr, ":")
	if len(kv) != 2 {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "invalid replica read label '%s'", replicaReadLabelStr)
	}
	return map[string]string{kv[0]: kv[1]}, nil
}
