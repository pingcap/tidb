// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	pconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	connutil "github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	flagOnline                   = "online"
	flagNoSchema                 = "no-schema"
	flagLoadStats                = "load-stats"
	flagGranularity              = "granularity"
	flagConcurrencyPerStore      = "tikv-max-restore-concurrency"
	flagAllowPITRFromIncremental = "allow-pitr-from-incremental"

	// FlagMergeRegionSizeBytes is the flag name of merge small regions by size
	FlagMergeRegionSizeBytes = "merge-region-size-bytes"
	// FlagMergeRegionKeyCount is the flag name of merge small regions by key count
	FlagMergeRegionKeyCount = "merge-region-key-count"
	// FlagPDConcurrency controls concurrency pd-relative operations like split & scatter.
	FlagPDConcurrency = "pd-concurrency"
	// FlagStatsConcurrency controls concurrency to restore statistic.
	FlagStatsConcurrency = "stats-concurrency"
	// FlagBatchFlushInterval controls after how long the restore batch would be auto sended.
	FlagBatchFlushInterval = "batch-flush-interval"
	// FlagDdlBatchSize controls batch ddl size to create a batch of tables
	FlagDdlBatchSize = "ddl-batch-size"
	// FlagWithPlacementPolicy corresponds to tidb config with-tidb-placement-mode
	// current only support STRICT or IGNORE, the default is STRICT according to tidb.
	FlagWithPlacementPolicy = "with-tidb-placement-mode"
	// FlagKeyspaceName corresponds to tidb config keyspace-name
	FlagKeyspaceName = "keyspace-name"

	// flagCheckpointStorage use
	flagCheckpointStorage = "checkpoint-storage"

	// FlagWaitTiFlashReady represents whether wait tiflash replica ready after table restored and checksumed.
	FlagWaitTiFlashReady = "wait-tiflash-ready"

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
	defaultPDConcurrency      = 1
	defaultStatsConcurrency   = 12
	defaultBatchFlushInterval = 16 * time.Second
	defaultFlagDdlBatchSize   = 128
)

const (
	FullRestoreCmd  = "Full Restore"
	DBRestoreCmd    = "DataBase Restore"
	TableRestoreCmd = "Table Restore"
	PointRestoreCmd = "Point Restore"
	RawRestoreCmd   = "Raw Restore"
	TxnRestoreCmd   = "Txn Restore"
)

// RestoreCommonConfig is the common configuration for all BR restore tasks.
type RestoreCommonConfig struct {
	Online              bool                     `json:"online" toml:"online"`
	Granularity         string                   `json:"granularity" toml:"granularity"`
	ConcurrencyPerStore pconfig.ConfigTerm[uint] `json:"tikv-max-restore-concurrency" toml:"tikv-max-restore-concurrency"`

	// MergeSmallRegionSizeBytes is the threshold of merging small regions (Default 96MB, region split size).
	// MergeSmallRegionKeyCount is the threshold of merging smalle regions (Default 960_000, region split key count).
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	MergeSmallRegionSizeBytes pconfig.ConfigTerm[uint64] `json:"merge-region-size-bytes" toml:"merge-region-size-bytes"`
	MergeSmallRegionKeyCount  pconfig.ConfigTerm[uint64] `json:"merge-region-key-count" toml:"merge-region-key-count"`

	// determines whether enable restore sys table on default, see fullClusterRestore in restore/client.go
	WithSysTable bool `json:"with-sys-table" toml:"with-sys-table"`

	ResetSysUsers []string `json:"reset-sys-users" toml:"reset-sys-users"`
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *RestoreCommonConfig) adjust() {
	if !cfg.MergeSmallRegionKeyCount.Modified {
		cfg.MergeSmallRegionKeyCount.Value = conn.DefaultMergeRegionKeyCount
	}
	if !cfg.MergeSmallRegionSizeBytes.Modified {
		cfg.MergeSmallRegionSizeBytes.Value = conn.DefaultMergeRegionSizeBytes
	}
	if len(cfg.Granularity) == 0 {
		cfg.Granularity = string(restore.CoarseGrained)
	}
	if !cfg.ConcurrencyPerStore.Modified {
		cfg.ConcurrencyPerStore.Value = conn.DefaultImportNumGoroutines
	}
}

// DefineRestoreCommonFlags defines common flags for the restore command.
func DefineRestoreCommonFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool(flagOnline, false, "(experimental) Whether online when restore")
	flags.String(flagGranularity, string(restore.CoarseGrained), "(deprecated) Whether split & scatter regions using fine-grained way during restore")
	flags.Uint(flagConcurrencyPerStore, 128, "The size of thread pool on each store that executes tasks")
	flags.Uint32(flagConcurrency, 128, "(deprecated) The size of thread pool on BR that executes tasks, "+
		"where each task restores one SST file to TiKV")
	flags.Uint64(FlagMergeRegionSizeBytes, conn.DefaultMergeRegionSizeBytes,
		"the threshold of merging small regions (Default 96MB, region split size)")
	flags.Uint64(FlagMergeRegionKeyCount, conn.DefaultMergeRegionKeyCount,
		"the threshold of merging small regions (Default 960_000, region split key count)")
	flags.Uint(FlagPDConcurrency, defaultPDConcurrency,
		"(deprecated) concurrency pd-relative operations like split & scatter.")
	flags.Uint(FlagStatsConcurrency, defaultStatsConcurrency,
		"concurrency to restore statistic")
	flags.Duration(FlagBatchFlushInterval, defaultBatchFlushInterval,
		"after how long a restore batch would be auto sent.")
	flags.Uint(FlagDdlBatchSize, defaultFlagDdlBatchSize,
		"batch size for ddl to create a batch of tables once.")
	flags.Bool(flagWithSysTable, true, "whether restore system privilege tables on default setting")
	flags.StringArrayP(FlagResetSysUsers, "", []string{"cloud_admin", "root"}, "whether reset these users after restoration")
	flags.Bool(flagUseFSR, false, "whether enable FSR for AWS snapshots")

	_ = flags.MarkHidden(FlagResetSysUsers)
	_ = flags.MarkHidden(FlagMergeRegionSizeBytes)
	_ = flags.MarkHidden(FlagMergeRegionKeyCount)
	_ = flags.MarkHidden(FlagPDConcurrency)
	_ = flags.MarkHidden(FlagStatsConcurrency)
	_ = flags.MarkHidden(FlagBatchFlushInterval)
	_ = flags.MarkHidden(FlagDdlBatchSize)
	_ = flags.MarkHidden(flagUseFSR)
}

// ParseFromFlags parses the config from the flag set.
func (cfg *RestoreCommonConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Granularity, err = flags.GetString(flagGranularity)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.ConcurrencyPerStore.Value, err = flags.GetUint(flagConcurrencyPerStore)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.ConcurrencyPerStore.Modified = flags.Changed(flagConcurrencyPerStore)

	cfg.MergeSmallRegionKeyCount.Value, err = flags.GetUint64(FlagMergeRegionKeyCount)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionKeyCount.Modified = flags.Changed(FlagMergeRegionKeyCount)

	cfg.MergeSmallRegionSizeBytes.Value, err = flags.GetUint64(FlagMergeRegionSizeBytes)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionSizeBytes.Modified = flags.Changed(FlagMergeRegionSizeBytes)

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
	LoadStats          bool          `json:"load-stats" toml:"load-stats"`
	PDConcurrency      uint          `json:"pd-concurrency" toml:"pd-concurrency"`
	StatsConcurrency   uint          `json:"stats-concurrency" toml:"stats-concurrency"`
	BatchFlushInterval time.Duration `json:"batch-flush-interval" toml:"batch-flush-interval"`
	// DdlBatchSize use to define the size of batch ddl to create tables
	DdlBatchSize uint `json:"ddl-batch-size" toml:"ddl-batch-size"`

	WithPlacementPolicy string `json:"with-tidb-placement-mode" toml:"with-tidb-placement-mode"`

	// FullBackupStorage is used to  run `restore full` before `restore log`.
	// if it is empty, directly take restoring log justly.
	FullBackupStorage string `json:"full-backup-storage" toml:"full-backup-storage"`

	// AllowPITRFromIncremental indicates whether this restore should enter a compatibility mode for incremental restore.
	// In this restore mode, the restore will not perform timestamp rewrite on the incremental data.
	AllowPITRFromIncremental bool `json:"allow-pitr-from-incremental" toml:"allow-pitr-from-incremental"`

	// [startTs, RestoreTS] is used to `restore log` from StartTS to RestoreTS.
	StartTS uint64 `json:"start-ts" toml:"start-ts"`
	// if not specified system will restore to the max TS available
	RestoreTS       uint64                      `json:"restore-ts" toml:"restore-ts"`
	tiflashRecorder *tiflashrec.TiFlashRecorder `json:"-" toml:"-"`
	PitrBatchCount  uint32                      `json:"pitr-batch-count" toml:"pitr-batch-count"`
	PitrBatchSize   uint32                      `json:"pitr-batch-size" toml:"pitr-batch-size"`
	PitrConcurrency uint32                      `json:"-" toml:"-"`

	UseCheckpoint                 bool                            `json:"use-checkpoint" toml:"use-checkpoint"`
	CheckpointStorage             string                          `json:"checkpoint-storage" toml:"checkpoint-storage"`
	upstreamClusterID             uint64                          `json:"-" toml:"-"`
	snapshotCheckpointMetaManager checkpoint.SnapshotMetaManagerT `json:"-" toml:"-"`
	logCheckpointMetaManager      checkpoint.LogMetaManagerT      `json:"-" toml:"-"`
	sstCheckpointMetaManager      checkpoint.SnapshotMetaManagerT `json:"-" toml:"-"`

	WaitTiflashReady bool `json:"wait-tiflash-ready" toml:"wait-tiflash-ready"`

	// for ebs-based restore
	FullBackupType      FullBackupType        `json:"full-backup-type" toml:"full-backup-type"`
	Prepare             bool                  `json:"prepare" toml:"prepare"`
	OutputFile          string                `json:"output-file" toml:"output-file"`
	SkipAWS             bool                  `json:"skip-aws" toml:"skip-aws"`
	CloudAPIConcurrency uint                  `json:"cloud-api-concurrency" toml:"cloud-api-concurrency"`
	VolumeType          pconfig.EBSVolumeType `json:"volume-type" toml:"volume-type"`
	VolumeIOPS          int64                 `json:"volume-iops" toml:"volume-iops"`
	VolumeThroughput    int64                 `json:"volume-throughput" toml:"volume-throughput"`
	VolumeEncrypted     bool                  `json:"volume-encrypted" toml:"volume-encrypted"`
	ProgressFile        string                `json:"progress-file" toml:"progress-file"`
	TargetAZ            string                `json:"target-az" toml:"target-az"`
	UseFSR              bool                  `json:"use-fsr" toml:"use-fsr"`
}

func (cfg *RestoreConfig) LocalEncryptionEnabled() bool {
	return cfg.CipherInfo.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT
}

// DefineRestoreFlags defines common flags for the restore tidb command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool(flagNoSchema, false, "skip creating schemas and tables, reuse existing empty ones")
	flags.Bool(flagLoadStats, true, "Run load stats or update stats_meta to trigger auto-analyze at end of snapshot restore task")
	// Do not expose this flag
	_ = flags.MarkHidden(flagNoSchema)
	flags.String(FlagWithPlacementPolicy, "STRICT", "correspond to tidb global/session variable with-tidb-placement-mode")
	flags.String(FlagKeyspaceName, "", "correspond to tidb config keyspace-name")

	flags.Bool(flagUseCheckpoint, true, "use checkpoint mode")
	_ = flags.MarkHidden(flagUseCheckpoint)

	flags.String(flagCheckpointStorage, "", "specify the external storage url where checkpoint data is saved, eg, s3://bucket/path/prefix")

	flags.Bool(FlagWaitTiFlashReady, false, "whether wait tiflash replica ready if tiflash exists")
	flags.Bool(flagAllowPITRFromIncremental, true, "whether make incremental restore compatible with later log restore"+
		" default is true, the incremental restore will not perform rewrite on the incremental data"+
		" meanwhile the incremental restore will not allow to restore 3 backfilled type ddl jobs,"+
		" these ddl jobs are Add index, Modify column and Reorganize partition")

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
func (cfg *RestoreConfig) ParseFromFlags(flags *pflag.FlagSet, skipCommonConfig bool) error {
	var err error
	cfg.NoSchema, err = flags.GetBool(flagNoSchema)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.LoadStats, err = flags.GetBool(flagLoadStats)
	if err != nil {
		return errors.Trace(err)
	}

	// parse common config if needed
	if !skipCommonConfig {
		err = cfg.Config.ParseFromFlags(flags)
		if err != nil {
			return errors.Trace(err)
		}
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
	cfg.StatsConcurrency, err = flags.GetUint(FlagStatsConcurrency)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagStatsConcurrency)
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
	cfg.UseCheckpoint, err = flags.GetBool(flagUseCheckpoint)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", flagUseCheckpoint)
	}
	cfg.CheckpointStorage, err = flags.GetString(flagCheckpointStorage)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", flagCheckpointStorage)
	}
	cfg.WaitTiflashReady, err = flags.GetBool(FlagWaitTiFlashReady)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", FlagWaitTiFlashReady)
	}

	cfg.AllowPITRFromIncremental, err = flags.GetBool(flagAllowPITRFromIncremental)
	if err != nil {
		return errors.Annotatef(err, "failed to get flag %s", flagAllowPITRFromIncremental)
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
		if cfg.VolumeEncrypted, err = flags.GetBool(flagVolumeEncrypted); err != nil {
			return errors.Trace(err)
		}

		cfg.ProgressFile, err = flags.GetString(flagProgressFile)
		if err != nil {
			return errors.Trace(err)
		}

		cfg.TargetAZ, err = flags.GetString(flagTargetAZ)
		if err != nil {
			return errors.Trace(err)
		}

		cfg.UseFSR, err = flags.GetBool(flagUseFSR)
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
	if cfg.StatsConcurrency == 0 {
		cfg.StatsConcurrency = defaultStatsConcurrency
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
	if cfg.ConcurrencyPerStore.Value > 0 {
		log.Info("set restore compacted sst files concurrency per store",
			zap.Int("concurrency", int(cfg.ConcurrencyPerStore.Value)))
	}
}

func (cfg *RestoreConfig) newStorageCheckpointMetaManagerPITR(
	ctx context.Context,
	downstreamClusterID uint64,
) error {
	_, checkpointStorage, err := GetStorage(ctx, cfg.CheckpointStorage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	if len(cfg.FullBackupStorage) > 0 {
		cfg.snapshotCheckpointMetaManager = checkpoint.NewSnapshotStorageMetaManager(
			checkpointStorage, &cfg.CipherInfo, downstreamClusterID, "snapshot")
	}
	cfg.logCheckpointMetaManager = checkpoint.NewLogStorageMetaManager(
		checkpointStorage, &cfg.CipherInfo, downstreamClusterID, "log")
	cfg.sstCheckpointMetaManager = checkpoint.NewSnapshotStorageMetaManager(
		checkpointStorage, &cfg.CipherInfo, downstreamClusterID, "sst")
	return nil
}

func (cfg *RestoreConfig) newStorageCheckpointMetaManagerSnapshot(
	ctx context.Context,
	downstreamClusterID uint64,
) error {
	if cfg.snapshotCheckpointMetaManager != nil {
		return nil
	}
	_, checkpointStorage, err := GetStorage(ctx, cfg.CheckpointStorage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.snapshotCheckpointMetaManager = checkpoint.NewSnapshotStorageMetaManager(
		checkpointStorage, &cfg.CipherInfo, downstreamClusterID, "snapshot")
	return nil
}

func (cfg *RestoreConfig) newTableCheckpointMetaManagerPITR(g glue.Glue, dom *domain.Domain) (err error) {
	if len(cfg.FullBackupStorage) > 0 {
		if cfg.snapshotCheckpointMetaManager, err = checkpoint.NewSnapshotTableMetaManager(
			g, dom, checkpoint.SnapshotRestoreCheckpointDatabaseName,
		); err != nil {
			return errors.Trace(err)
		}
	}
	if cfg.logCheckpointMetaManager, err = checkpoint.NewLogTableMetaManager(
		g, dom, checkpoint.LogRestoreCheckpointDatabaseName,
	); err != nil {
		return errors.Trace(err)
	}
	if cfg.sstCheckpointMetaManager, err = checkpoint.NewSnapshotTableMetaManager(
		g, dom, checkpoint.CustomSSTRestoreCheckpointDatabaseName,
	); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cfg *RestoreConfig) newTableCheckpointMetaManagerSnapshot(g glue.Glue, dom *domain.Domain) (err error) {
	if cfg.snapshotCheckpointMetaManager != nil {
		return nil
	}
	if cfg.snapshotCheckpointMetaManager, err = checkpoint.NewSnapshotTableMetaManager(
		g, dom, checkpoint.SnapshotRestoreCheckpointDatabaseName,
	); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cfg *RestoreConfig) CloseCheckpointMetaManager() {
	if cfg.logCheckpointMetaManager != nil {
		cfg.logCheckpointMetaManager.Close()
	}
	if cfg.snapshotCheckpointMetaManager != nil {
		cfg.snapshotCheckpointMetaManager.Close()
	}
	if cfg.sstCheckpointMetaManager != nil {
		cfg.sstCheckpointMetaManager.Close()
	}
}

func configureRestoreClient(ctx context.Context, client *snapclient.SnapClient, cfg *RestoreConfig) error {
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	if cfg.NoSchema {
		client.EnableSkipCreateSQL()
	}
	client.SetBatchDdlSize(cfg.DdlBatchSize)
	client.SetPlacementPolicyMode(cfg.WithPlacementPolicy)
	client.SetWithSysTable(cfg.WithSysTable)
	client.SetRewriteMode(ctx)
	return nil
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

// VerifyDBAndTableInBackup is used to check whether the restore dbs or tables have been backup
func VerifyDBAndTableInBackup(schemas []*metautil.Database, cfg *RestoreConfig) error {
	if len(cfg.Schemas) == 0 && len(cfg.Tables) == 0 {
		return nil
	}
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

	// check on if explicit schema/table filter matches
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

func registerTaskToPD(ctx context.Context, etcdCLI *clientv3.Client) (closeF func(context.Context) error, err error) {
	register := utils.NewTaskRegister(etcdCLI, utils.RegisterRestore, fmt.Sprintf("restore-%s", uuid.New()))
	err = register.RegisterTask(ctx)
	return register.Close, errors.Trace(err)
}

func DefaultRestoreConfig(commonConfig Config) RestoreConfig {
	fs := pflag.NewFlagSet("dummy", pflag.ContinueOnError)
	DefineRestoreFlags(fs)
	cfg := RestoreConfig{}
	err := cfg.ParseFromFlags(fs, true)
	if err != nil {
		log.Panic("failed to parse restore flags to config", zap.Error(err))
	}

	cfg.Config = commonConfig
	return cfg
}

func printRestoreMetrics() {
	log.Info("Metric: import_file_seconds", zap.Object("metric", logutil.MarshalHistogram(metrics.RestoreImportFileSeconds)))
	log.Info("Metric: upload_sst_for_pitr_seconds", zap.Object("metric", logutil.MarshalHistogram(metrics.RestoreUploadSSTForPiTRSeconds)))
	log.Info("Metric: upload_sst_meta_for_pitr_seconds", zap.Object("metric", logutil.MarshalHistogram(metrics.RestoreUploadSSTMetaForPiTRSeconds)))
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	etcdCLI, err := dialEtcdWithCfg(c, cfg.Config)
	if err != nil {
		return err
	}
	defer func() {
		if err := etcdCLI.Close(); err != nil {
			log.Error("failed to close the etcd client", zap.Error(err))
		}
	}()
	if err := checkConflictingLogBackup(c, cfg, etcdCLI); err != nil {
		return errors.Annotate(err, "failed to check task exists")
	}
	closeF, err := registerTaskToPD(c, etcdCLI)
	if err != nil {
		return errors.Annotate(err, "failed to register task to pd")
	}
	defer func() {
		_ = closeF(c)
	}()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = cfg.KeyspaceName
	})

	// TODO: remove version checker from `NewMgr`
	mgr, err := NewMgr(c, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, true, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()
	defer cfg.CloseCheckpointMetaManager()

	if err = g.UseOneShotSession(mgr.GetStorage(), false, func(se glue.Session) error {
		enableFollowerHandleRegion, err := se.GetGlobalSysVar(vardef.PDEnableFollowerHandleRegion)
		if err != nil {
			return err
		}
		return mgr.SetFollowerHandle(variable.TiDBOptOn(enableFollowerHandleRegion))
	}); err != nil {
		return errors.Trace(err)
	}

	defer printRestoreMetrics()

	var restoreError error
	if IsStreamRestore(cmdName) {
		if err := version.CheckClusterVersion(c, mgr.GetPDClient(), version.CheckVersionForBRPiTR); err != nil {
			return errors.Trace(err)
		}
		restoreError = RunStreamRestore(c, mgr, g, cfg)
	} else {
		if err := version.CheckClusterVersion(c, mgr.GetPDClient(), version.CheckVersionForBR); err != nil {
			return errors.Trace(err)
		}
		snapshotRestoreConfig := SnapshotRestoreConfig{
			RestoreConfig: cfg,
		}
		restoreError = runSnapshotRestore(c, mgr, g, cmdName, &snapshotRestoreConfig)
	}
	if restoreError != nil {
		return errors.Trace(restoreError)
	}
	// Clear the checkpoint data
	if cfg.UseCheckpoint {
		if IsStreamRestore(cmdName) {
			log.Info("start to remove checkpoint data for PITR restore")
			err = cfg.logCheckpointMetaManager.RemoveCheckpointData(c)
			if err != nil {
				log.Warn("failed to remove checkpoint data for log restore", zap.Error(err))
			}
			err = cfg.sstCheckpointMetaManager.RemoveCheckpointData(c)
			if err != nil {
				log.Warn("failed to remove checkpoint data for compacted restore", zap.Error(err))
			}
			// Skip removing snapshot checkpoint data if this is a pure log restore
			// (i.e. restoring only from log backup without a base snapshot backup),
			// since snapshotCheckpointMetaManager would be nil in that case
			if cfg.snapshotCheckpointMetaManager != nil {
				err = cfg.snapshotCheckpointMetaManager.RemoveCheckpointData(c)
				if err != nil {
					log.Warn("failed to remove checkpoint data for snapshot restore", zap.Error(err))
				}
			}
		} else {
			err = cfg.snapshotCheckpointMetaManager.RemoveCheckpointData(c)
			if err != nil {
				log.Warn("failed to remove checkpoint data for snapshot restore", zap.Error(err))
			}
		}
		log.Info("all the checkpoint data is removed.")
	}
	return nil
}

type SnapshotRestoreConfig struct {
	*RestoreConfig
	piTRTaskInfo           *PiTRTaskInfo
	logTableHistoryManager *stream.LogBackupTableHistoryManager
	tableMappingManager    *stream.TableMappingManager
}

func (s *SnapshotRestoreConfig) isPiTR() (bool, error) {
	if s.piTRTaskInfo != nil && s.logTableHistoryManager != nil && s.tableMappingManager != nil {
		return true, nil
	}

	if s.piTRTaskInfo == nil && s.logTableHistoryManager == nil && s.tableMappingManager == nil {
		return false, nil
	}

	errMsg := "inconsistent PiTR components detected"
	log.Error(errMsg,
		zap.Any("piTRTaskInfo", s.piTRTaskInfo),
		zap.Any("logTableHistoryManager", s.logTableHistoryManager),
		zap.Any("tableMappingManager", s.tableMappingManager))

	return false, errors.New(errMsg)
}

func runSnapshotRestore(c context.Context, mgr *conn.Mgr, g glue.Glue, cmdName string, cfg *SnapshotRestoreConfig) error {
	cfg.Adjust()
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	log.Info("starting snapshot restore")
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestore", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// check if this is part of the PiTR operation
	isPiTR, err := cfg.isPiTR()
	if err != nil {
		return errors.Trace(err)
	}

	// reads out information from backup meta file and do requirement checking if needed
	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	if backupMeta.IsRawKv || backupMeta.IsTxnKv {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do transactional restore from raw/txn kv data")
	}
	if cfg.CheckRequirements {
		log.Info("Checking incompatible TiCDC changefeeds before restoring.",
			logutil.ShortError(err), zap.Uint64("restore-ts", backupMeta.EndVersion))
		if err := checkIncompatibleChangefeed(ctx, backupMeta.EndVersion, mgr.GetDomain().GetEtcdClient()); err != nil {
			return errors.Trace(err)
		}

		backupVersion := version.NormalizeBackupVersion(backupMeta.ClusterVersion)
		if backupVersion != nil {
			if versionErr := version.CheckClusterVersion(ctx, mgr.GetPDClient(), version.CheckVersionForBackup(backupVersion)); versionErr != nil {
				return errors.Trace(versionErr)
			}
		}
	}
	if _, err = CheckNewCollationEnable(backupMeta.GetNewCollationsEnabled(), g, mgr.GetStorage(), cfg.CheckRequirements); err != nil {
		return errors.Trace(err)
	}

	// build restore client
	// need to retrieve these configs from tikv if not set in command.
	kvConfigs := &pconfig.KVConfig{
		ImportGoroutines:    cfg.ConcurrencyPerStore,
		MergeRegionSize:     cfg.MergeSmallRegionSizeBytes,
		MergeRegionKeyCount: cfg.MergeSmallRegionKeyCount,
	}

	// according to https://github.com/pingcap/tidb/issues/34167.
	// we should get the real config from tikv to adapt the dynamic region.
	httpCli := httputil.NewClient(mgr.GetTLSConfig())
	mgr.ProcessTiKVConfigs(ctx, kvConfigs, httpCli)

	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := snapclient.NewRestoreClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)
	defer client.Close()
	// set to cfg so that restoreStream can use it.
	cfg.ConcurrencyPerStore = kvConfigs.ImportGoroutines
	// using tikv config to set the concurrency-per-store for client.
	client.SetConcurrencyPerStore(cfg.ConcurrencyPerStore.Value)
	err = configureRestoreClient(ctx, client, cfg.RestoreConfig)
	if err != nil {
		return errors.Trace(err)
	}
	// InitConnections DB connection sessions
	err = client.InitConnections(g, mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}

	metaReader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.LoadSchemaIfNeededAndInitClient(ctx, backupMeta, u, metaReader, cfg.LoadStats, nil, nil,
		cfg.ExplicitFilter, isFullRestore(cmdName), cfg.WithSysTable); err != nil {
		return errors.Trace(err)
	}

	if client.IsIncremental() {
		// don't support checkpoint for the ddl restore
		log.Info("the incremental snapshot restore doesn't support checkpoint mode, disable checkpoint.")
		cfg.UseCheckpoint = false
	}
	cpEnabledAndExists, err := checkpointEnabledAndExists(ctx, g, cfg, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("checkpoint status in restore", zap.Bool("enabled", cfg.UseCheckpoint), zap.Bool("exists", cpEnabledAndExists))
	if err := checkMandatoryClusterRequirements(client, cfg, cpEnabledAndExists, cmdName); err != nil {
		return errors.Trace(err)
	}

	// filters out db/table/files using filter
	tableMap, dbMap, err := filterRestoreFiles(client, cfg.RestoreConfig)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("found items to restore after filtering",
		zap.Int("tables", len(tableMap)),
		zap.Int("db", len(dbMap)))

	// only run when this full restore is part of the PiTR
	if isPiTR {
		// adjust tables to restore in the snapshot restore phase since it will later be renamed during
		// log restore and will fall into or out of the filter range.
		err = AdjustTablesToRestoreAndCreateTableTracker(
			cfg.logTableHistoryManager,
			cfg.RestoreConfig,
			client.GetDatabaseMap(),
			client.GetTableMap(),
			client.GetPartitionMap(),
			tableMap,
			dbMap,
		)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("adjusted items to restore",
			zap.Int("tables", len(tableMap)),
			zap.Int("db", len(dbMap)))
	}
	tables := utils.Values(tableMap)
	dbs := utils.Values(dbMap)

	setTablesRestoreModeIfNeeded(tables, cfg, isPiTR)

	archiveSize := metautil.ArchiveTablesSize(tables)
	// some more checks once we get tables and files information
	if err := checkOptionalClusterRequirements(ctx, client, cfg, cpEnabledAndExists, mgr, tables, archiveSize, isPiTR); err != nil {
		return errors.Trace(err)
	}

	g.Record(summary.RestoreDataSize, archiveSize)
	//restore from tidb will fetch a general Size issue https://github.com/pingcap/tidb/issues/27247
	g.Record("Size", archiveSize)
	restoreTS, err := restore.GetTSWithRetry(ctx, mgr.GetPDClient())
	if err != nil {
		return errors.Trace(err)
	}

	if client.IsFullClusterRestore() && client.HasBackedUpSysDB() {
		if err = snapclient.CheckSysTableCompatibility(mgr.GetDomain(), tables); err != nil {
			return errors.Trace(err)
		}
	}

	// preallocate the table id, because any ddl job or database creation(include checkpoint) also allocates the global ID
	if err = client.AllocTableIDs(ctx, tables); err != nil {
		return errors.Trace(err)
	}

	err = client.InstallPiTRSupport(ctx, snapclient.PiTRCollDep{
		PDCli:   mgr.GetPDClient(),
		EtcdCli: mgr.GetDomain().GetEtcdClient(),
		Storage: util.ProtoV1Clone(u),
	})
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}
	g.Record("BackupTS", backupMeta.EndVersion)
	g.Record("RestoreTS", restoreTS)
	cctx, gcSafePointKeeperCancel := context.WithCancel(ctx)
	defer func() {
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
	// restore checksum will check safe point with its start ts, see details at
	// https://github.com/pingcap/tidb/blob/180c02127105bed73712050594da6ead4d70a85f/store/tikv/kv.go#L186-L190
	// so, we should keep the safe point unchangeable. to avoid GC life time is shorter than transaction duration.
	err = utils.StartServiceSafePointKeeper(cctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	ddlJobs := FilterDDLJobs(client.GetDDLJobs(), tables)
	ddlJobs = FilterDDLJobByRules(ddlJobs, DDLJobBlockListRule)
	if cfg.AllowPITRFromIncremental {
		err = CheckDDLJobByRules(ddlJobs, DDLJobLogIncrementalCompactBlockListRule)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = PreCheckTableTiFlashReplica(ctx, mgr.GetPDClient(), tables, cfg.tiflashRecorder)
	if err != nil {
		return errors.Trace(err)
	}

	err = PreCheckTableClusterIndex(tables, ddlJobs, mgr.GetDomain())
	if err != nil {
		return errors.Trace(err)
	}

	// pre-set TiDB config for restore
	restoreDBConfig := tweakLocalConfForRestore()
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
	if err = client.ExecDDLs(ctx, ddlJobs); err != nil {
		return errors.Trace(err)
	}

	// nothing to restore, maybe only ddl changes in incremental restore
	if len(dbs) == 0 && len(tables) == 0 {
		log.Info("nothing to restore, all databases and tables are filtered out")
		// even nothing to restore, we show a success message since there is no failure.
	}

	createdTables, err := createDBsAndTables(ctx, client, cfg, mgr, dbs, tables)
	if err != nil {
		return errors.Trace(err)
	}

	// update table mapping manager with new table ids if PiTR
	if isPiTR {
		if err = cfg.tableMappingManager.UpdateDownstreamIds(dbs, createdTables, client.GetDomain()); err != nil {
			return errors.Trace(err)
		}
		log.Info("updated table mapping manager after creating tables")
	}

	anyFileKey := getAnyFileKeyFromTables(tables)
	if len(anyFileKey) == 0 {
		log.Info("no files, empty databases and tables are restored")
		summary.SetSuccessStatus(true)
		// don't return immediately, wait all pipeline done.
	} else {
		codec := mgr.GetStorage().GetCodec()
		oldKeyspace, _, err := tikv.DecodeKey(anyFileKey, backupMeta.ApiVersion)
		if err != nil {
			return errors.Trace(err)
		}
		newKeyspace := codec.GetKeyspace()

		// If the API V2 data occurs in the restore process, the cluster must
		// support the keyspace rewrite mode.
		if (len(oldKeyspace) > 0 || len(newKeyspace) > 0) && client.GetRewriteMode() == snapclient.RewriteModeLegacy {
			return errors.Annotate(berrors.ErrRestoreModeMismatch, "cluster only supports legacy rewrite mode")
		}

		// Hijack the tableStream and rewrite the rewrite rules.
		for _, createdTable := range createdTables {
			// Set the keyspace info for the checksum requests
			createdTable.RewriteRule.OldKeyspace = oldKeyspace
			createdTable.RewriteRule.NewKeyspace = newKeyspace

			for _, rule := range createdTable.RewriteRule.Data {
				rule.OldKeyPrefix = slices.Concat(oldKeyspace, rule.OldKeyPrefix)
				rule.NewKeyPrefix = codec.EncodeKey(rule.NewKeyPrefix)
			}
		}
	}

	importModeSwitcher := restore.NewImportModeSwitcher(mgr.GetPDClient(), cfg.Config.SwitchModeInterval, mgr.GetTLSConfig())
	var restoreSchedulersFunc pdutil.UndoFunc
	var schedulersConfig *pdutil.ClusterConfig
	if (isFullRestore(cmdName) && !cfg.ExplicitFilter) || client.IsIncremental() {
		restoreSchedulersFunc, schedulersConfig, err = restore.RestorePreWork(ctx, mgr, importModeSwitcher, cfg.Online, true)
	} else {
		var preAllocRange [2]int64
		preAllocRange, err = client.GetPreAllocedTableIDRange()
		if err != nil {
			return errors.Trace(err)
		}
		var tableIDs []int64
		for _, table := range createdTables {
			tableIDs = append(tableIDs, table.Table.ID)
			if table.Table.Partition != nil {
				for _, p := range table.Table.Partition.Definitions {
					tableIDs = append(tableIDs, p.ID)
				}
			}
		}
		keyRange := SortKeyRanges(tableIDs, preAllocRange)
		restoreSchedulersFunc, schedulersConfig, err = restore.FineGrainedRestorePreWork(ctx, mgr, importModeSwitcher, keyRange, cfg.Online, true)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// need to know whether restore has been completed so can restore schedulers
	canRestoreSchedulers := false
	defer func() {
		cancel()
		// don't reset pd scheduler if checkpoint mode is used and restored is not finished
		if cfg.UseCheckpoint && !canRestoreSchedulers {
			log.Info("skip removing pd scheduler for next retry")
			return
		}
		log.Info("start to restore pd scheduler")
		// run the post-work to avoid being stuck in the import
		// mode or emptied schedulers.
		restore.RestorePostWork(ctx, importModeSwitcher, restoreSchedulersFunc, cfg.Online)
		log.Info("finish restoring pd scheduler")
	}()

	// reload or register the checkpoint
	var checkpointSetWithTableID map[int64]map[string]struct{}
	if cfg.UseCheckpoint {
		logRestoredTS := uint64(0)
		if cfg.piTRTaskInfo != nil {
			logRestoredTS = cfg.piTRTaskInfo.RestoreTS
		}
		sets, restoreSchedulersConfigFromCheckpoint, err := client.InitCheckpoint(
			ctx, cfg.snapshotCheckpointMetaManager, schedulersConfig, logRestoredTS, cpEnabledAndExists)
		if err != nil {
			return errors.Trace(err)
		}
		if restoreSchedulersConfigFromCheckpoint != nil {
			// The last range rule will be dropped when the last restore quits.
			restoreSchedulersConfigFromCheckpoint.RuleID = schedulersConfig.RuleID
			restoreSchedulersFunc = mgr.MakeUndoFunctionByConfig(*restoreSchedulersConfigFromCheckpoint)
		}
		checkpointSetWithTableID = sets

		defer func() {
			// need to flush the whole checkpoint data so that br can quickly jump to
			// the log kv restore step when the next retry.
			log.Info("wait for flush checkpoint...")
			client.WaitForFinishCheckpoint(ctx, len(cfg.FullBackupStorage) > 0 || !canRestoreSchedulers)
		}()
	}

	failpoint.Inject("sleep_for_check_scheduler_status", func(val failpoint.Value) {
		fileName, ok := val.(string)
		func() {
			if !ok {
				return
			}
			_, osErr := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
			if osErr != nil {
				log.Warn("failed to create file", zap.Error(osErr))
				return
			}
		}()
		for {
			_, statErr := os.Stat(fileName)
			if os.IsNotExist(statErr) {
				break
			} else if statErr != nil {
				log.Warn("error checking file", zap.Error(statErr))
				break
			}
			time.Sleep(1 * time.Second)
		}
	})

	if cfg.tiflashRecorder != nil {
		for _, createdTable := range createdTables {
			cfg.tiflashRecorder.Rewrite(createdTable.OldTable.Info.ID, createdTable.Table.ID)
		}
	}

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(ctx, mgr.PdController); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	rtCtx := snapclient.RestoreTablesContext{
		LogProgress:    cfg.LogProgress,
		SplitSizeBytes: kvConfigs.MergeRegionSize.Value,
		SplitKeyCount:  kvConfigs.MergeRegionKeyCount.Value,
		// If the command is from BR binary, the ddl.EnableSplitTableRegion is always 0,
		// If the command is from BRIE SQL, the ddl.EnableSplitTableRegion is TiDB config split-table.
		// Notice that `split-region-on-table` configure from TiKV split on the region having data, it may trigger after restore done.
		// It's recommended to enable TiDB configure `split-table` instead.
		SplitOnTable: atomic.LoadUint32(&ddl.EnableSplitTableRegion) == 1,
		Online:       cfg.Online,

		CreatedTables:            createdTables,
		CheckpointSetWithTableID: checkpointSetWithTableID,

		Glue: g,
	}
	if err := client.RestoreTables(ctx, rtCtx); err != nil {
		return errors.Trace(err)
	}

	plCtx := snapclient.PipelineContext{
		// pipeline checksum only when enabled and is not incremental snapshot repair mode cuz incremental doesn't have
		// enough information in backup meta to validate checksum
		Checksum:         cfg.Checksum && !client.IsIncremental(),
		LoadStats:        cfg.LoadStats,
		WaitTiflashReady: cfg.WaitTiflashReady,

		LogProgress:         cfg.LogProgress,
		ChecksumConcurrency: cfg.ChecksumConcurrency,
		StatsConcurrency:    cfg.StatsConcurrency,

		KvClient:   mgr.GetStorage().GetClient(),
		ExtStorage: s,
		Glue:       g,
	}
	// Do some work in pipeline, such as checkum, load stats and wait tiflash ready.
	if err := client.RestorePipeline(ctx, plCtx, createdTables); err != nil {
		return errors.Trace(err)
	}

	// The cost of rename user table / replace into system table wouldn't be so high.
	// So leave it out of the pipeline for easier implementation.
	log.Info("restoring system schemas", zap.Bool("withSys", cfg.WithSysTable),
		zap.Strings("filter", cfg.FilterStr))
	err = client.RestoreSystemSchemas(ctx, cfg.TableFilter)
	if err != nil {
		return errors.Trace(err)
	}

	failpoint.InjectCall("run-snapshot-restore-about-to-finish", &err)
	if err != nil {
		return err
	}

	canRestoreSchedulers = true

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

func getMaxReplica(ctx context.Context, mgr *conn.Mgr) (cnt uint64, err error) {
	var resp map[string]any
	err = utils.WithRetry(ctx, func() error {
		resp, err = mgr.GetPDHTTPClient().GetReplicateConfig(ctx)
		return err
	}, utils.NewAggressivePDBackoffStrategy())
	if err != nil {
		return 0, errors.Trace(err)
	}

	key := "max-replicas"
	val, ok := resp[key]
	if !ok {
		return 0, errors.Errorf("key %s not found in response %v", key, resp)
	}
	return uint64(val.(float64)), nil
}

func getStores(ctx context.Context, mgr *conn.Mgr) (stores *http.StoresInfo, err error) {
	err = utils.WithRetry(ctx, func() error {
		stores, err = mgr.GetPDHTTPClient().GetStores(ctx)
		return err
	}, utils.NewAggressivePDBackoffStrategy())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return stores, nil
}

func EstimateTikvUsage(archiveSize uint64, replicaCnt uint64, storeCnt uint64) uint64 {
	if storeCnt == 0 {
		return 0
	}
	if replicaCnt > storeCnt {
		replicaCnt = storeCnt
	}
	log.Info("estimate tikv usage", zap.Uint64("total size", archiveSize), zap.Uint64("replicaCnt", replicaCnt), zap.Uint64("store count", storeCnt))
	return archiveSize * replicaCnt / storeCnt
}

func EstimateTiflashUsage(tables []*metautil.Table, storeCnt uint64) uint64 {
	if storeCnt == 0 {
		return 0
	}
	tiflashTotal := uint64(0)
	for _, table := range tables {
		if table.Info.TiFlashReplica == nil || table.Info.TiFlashReplica.Count <= 0 {
			continue
		}
		tableBytes := metautil.ArchiveTableSize(table)
		tiflashTotal += tableBytes * table.Info.TiFlashReplica.Count
	}
	log.Info("estimate tiflash usage", zap.Uint64("total size", tiflashTotal), zap.Uint64("store count", storeCnt))
	return tiflashTotal / storeCnt
}

func CheckStoreSpace(necessary uint64, store *http.StoreInfo) error {
	available, err := units.RAMInBytes(store.Status.Available)
	if err != nil {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "store %d has invalid available space %s", store.Store.ID, store.Status.Available)
	}
	if available <= 0 {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "store %d has invalid available space %s", store.Store.ID, store.Status.Available)
	}
	if uint64(available) < necessary {
		return errors.Annotatef(berrors.ErrKVDiskFull, "store %d has no space left on device, available %s, necessary %s",
			store.Store.ID, units.BytesSize(float64(available)), units.BytesSize(float64(necessary)))
	}
	return nil
}

func checkDiskSpace(ctx context.Context, mgr *conn.Mgr, tables []*metautil.Table, archiveSize uint64) error {
	maxReplica, err := getMaxReplica(ctx, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	stores, err := getStores(ctx, mgr)
	if err != nil {
		return errors.Trace(err)
	}

	var tikvCnt, tiflashCnt uint64 = 0, 0
	for i := range stores.Stores {
		store := &stores.Stores[i]
		if engine.IsTiFlashHTTPResp(&store.Store) {
			tiflashCnt += 1
			continue
		}
		tikvCnt += 1
	}

	// We won't need to restore more than 1800 PB data at one time, right?
	preserve := func(base uint64, ratio float32) uint64 {
		if base > 1000*units.PB {
			return base
		}
		return base * uint64(ratio*10) / 10
	}

	// The preserve rate for tikv is quite accurate, while rate for tiflash is a
	// number calculated from tpcc testing with variable data sizes.  1.4 is a
	// relative conservative value.
	tikvUsage := preserve(EstimateTikvUsage(archiveSize, maxReplica, tikvCnt), 1.1)
	tiflashUsage := preserve(EstimateTiflashUsage(tables, tiflashCnt), 1.4)
	log.Info("preserved disk space", zap.Uint64("tikv", tikvUsage), zap.Uint64("tiflash", tiflashUsage))

	err = utils.WithRetry(ctx, func() error {
		stores, err = getStores(ctx, mgr)
		if err != nil {
			return errors.Trace(err)
		}
		for _, store := range stores.Stores {
			if engine.IsTiFlashHTTPResp(&store.Store) {
				if err := CheckStoreSpace(tiflashUsage, &store); err != nil {
					return errors.Trace(err)
				}
				continue
			}
			if err := CheckStoreSpace(tikvUsage, &store); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, utils.NewDiskCheckBackoffStrategy())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkTableExistence(ctx context.Context, mgr *conn.Mgr, tables []*metautil.Table) error {
	message := "table already exists: "
	allUnique := true
	for _, table := range tables {
		_, err := mgr.GetDomain().InfoSchema().TableByName(ctx, table.DB.Name, table.Info.Name)
		if err == nil {
			message += fmt.Sprintf("%s.%s ", table.DB.Name, table.Info.Name)
			allUnique = false
		} else if !infoschema.ErrTableNotExists.Equal(err) {
			return errors.Trace(err)
		}
	}
	if !allUnique {
		return errors.Annotate(berrors.ErrTablesAlreadyExisted, message)
	}
	return nil
}

func getAnyFileKeyFromTables(tables []*metautil.Table) []byte {
	for _, table := range tables {
		for _, files := range table.FilesOfPhysicals {
			for _, f := range files {
				if len(f.StartKey) > 0 {
					return f.StartKey
				}
			}
		}
	}
	return nil
}

// filterRestoreFiles filters out dbs and tables.
func filterRestoreFiles(
	client *snapclient.SnapClient,
	cfg *RestoreConfig,
) (tableMap map[int64]*metautil.Table, dbMap map[int64]*metautil.Database, err error) {
	tableMap = make(map[int64]*metautil.Table)
	dbMap = make(map[int64]*metautil.Database)

	for _, db := range client.GetDatabases() {
		dbName := db.Info.Name.O
		if checkpoint.IsCheckpointDB(db.Info.Name) {
			continue
		}
		if !utils.MatchSchema(cfg.TableFilter, dbName, cfg.WithSysTable) {
			continue
		}
		dbMap[db.Info.ID] = db
		for _, table := range db.Tables {
			if table.Info == nil || !utils.MatchTable(cfg.TableFilter, dbName, table.Info.Name.O, cfg.WithSysTable) {
				continue
			}

			// Add table to tableMap using table ID as key
			tableMap[table.Info.ID] = table
		}
	}

	// sanity check
	if len(dbMap) == 0 && len(tableMap) != 0 {
		err = errors.Annotate(berrors.ErrRestoreInvalidBackup, "contains tables but no databases")
	}
	return
}

// getDBNameFromBackup gets database name from either snapshot or log backup history
func getDBNameFromBackup(
	dbID int64,
	snapshotDBMap map[int64]*metautil.Database,
	logBackupTableHistory *stream.LogBackupTableHistoryManager,
) (dbName string, exists bool) {
	// check in snapshot
	if snapDb, exists := snapshotDBMap[dbID]; exists {
		return snapDb.Info.Name.O, true
	}
	// check during log backup
	if name, exists := logBackupTableHistory.GetDBNameByID(dbID); exists {
		return name, true
	}
	log.Warn("did not find db id in full/log backup, "+
		"likely different filters are specified for full/log backup and restore, ignoring this db",
		zap.Any("dbId", dbID))
	return "", false
}

// processLogBackupTableHistory processes table-level operations (renames) and determines which tables need to be restored
func processLogBackupTableHistory(
	history *stream.LogBackupTableHistoryManager,
	snapshotDBMap map[int64]*metautil.Database,
	snapshotTableMap map[int64]*metautil.Table,
	partitionMap map[int64]*stream.TableLocationInfo,
	cfg *RestoreConfig,
	existingTableMap map[int64]*metautil.Table,
	existingDBMap map[int64]*metautil.Database,
	pitrIdTracker *utils.PiTRIdTracker,
) {
	for tableId, dbIDAndTableName := range history.GetTableHistory() {
		start := buildStartTableLocationInfo(tableId, &dbIDAndTableName[0], snapshotTableMap, partitionMap)
		end := &dbIDAndTableName[1]

		endDBName, exists := getDBNameFromBackup(end.DbID, snapshotDBMap, history)
		if !exists {
			continue
		}

		endMatches := utils.MatchTable(cfg.TableFilter, endDBName, end.TableName, cfg.WithSysTable)

		// if end matches, add to tracker
		if endMatches {
			if !end.IsPartition {
				pitrIdTracker.TrackTableId(end.DbID, tableId)
				// used to check if existing cluster has same table already so can error out
				pitrIdTracker.TrackTableName(endDBName, end.TableName)
				log.Info("tracking table", zap.Int64("schemaID", end.DbID),
					zap.Int64("tableID", tableId), zap.String("tableName", end.TableName))
			} else {
				// only used for partition violation checking later
				pitrIdTracker.TrackPartitionId(tableId)
			}
		}

		// skip if partition
		if start.IsPartition || end.IsPartition {
			continue
		}

		_, isStartInSnap := snapshotTableMap[tableId]
		// no need to adjust tables if start is not in snapshot
		if !isStartInSnap {
			continue
		}

		startDBName, exists := getDBNameFromBackup(start.DbID, snapshotDBMap, history)
		if !exists {
			continue
		}
		startMatches := utils.MatchTable(cfg.TableFilter, startDBName, start.TableName, cfg.WithSysTable)

		// skip if both not match
		if !startMatches && !endMatches {
			continue
		}

		// skip if both match and nothing changes
		if startMatches && endMatches {
			// skip if both match and in same db
			if start.DbID == end.DbID {
				continue
			}
		}

		// at here only three cases left
		// 1. both match but start and end are not in same DB due to rename
		// 2. end matches but start doesn't -> need to restore (add)
		// 3. start matches but end doesn't -> need to remove
		if startDB, exists := snapshotDBMap[start.DbID]; exists {
			for _, table := range startDB.Tables {
				if table.Info != nil && table.Info.ID == tableId {
					if endMatches {
						log.Info("table renamed into the filter, adding this table",
							zap.Int64("table_id", tableId),
							zap.String("table_name", table.Info.Name.O))
						existingTableMap[tableId] = table
						existingDBMap[start.DbID] = startDB
					} else if startMatches {
						log.Info("table renamed out of filter, removing this table",
							zap.Int64("table_id", tableId),
							zap.String("table_name", table.Info.Name.O))
						delete(existingTableMap, table.Info.ID)
					}
					break
				}
			}
		}
	}
}

// shouldRestoreTable checks if a table or partition is being tracked for restore
func shouldRestoreTable(
	physicalId int64,
	locationInfo *stream.TableLocationInfo,
	cfg *RestoreConfig,
) bool {
	// if is a partition, check whether its parent table is included to restore
	if locationInfo.IsPartition {
		return cfg.PiTRTableTracker.ContainsTableId(locationInfo.ParentTableID) ||
			cfg.PiTRTableTracker.ContainsPartitionId(locationInfo.ParentTableID)
	}

	// if is tabla, check if will be restored
	return cfg.PiTRTableTracker.ContainsTableId(physicalId) || cfg.PiTRTableTracker.ContainsPartitionId(physicalId)
}

func buildStartTableLocationInfo(
	physicalID int64,
	start *stream.TableLocationInfo,
	snapshotTableMap map[int64]*metautil.Table,
	partitionMap map[int64]*stream.TableLocationInfo) *stream.TableLocationInfo {
	if partitionInfo, exist := partitionMap[physicalID]; exist {
		return partitionInfo
	}
	if tableInfo, exist := snapshotTableMap[physicalID]; exist {
		return &stream.TableLocationInfo{
			DbID:          tableInfo.DB.ID,
			TableName:     tableInfo.Info.Name.O,
			IsPartition:   false,
			ParentTableID: 0,
		}
	}
	return start
}

// checkPartitionExchangesViolations checks for partition exchanges and returns an error if a partition
// was exchanged between tables where one is in the filter and one is not
func checkPartitionExchangesViolations(
	history *stream.LogBackupTableHistoryManager,
	snapshotDBMap map[int64]*metautil.Database,
	snapshotTableMap map[int64]*metautil.Table,
	partitionMap map[int64]*stream.TableLocationInfo,
	cfg *RestoreConfig,
) error {
	for tableId, dbIDAndTableName := range history.GetTableHistory() {
		start := &dbIDAndTableName[0]
		end := &dbIDAndTableName[1]

		// need to use snapshot start if exists
		start = buildStartTableLocationInfo(tableId, start, snapshotTableMap, partitionMap)

		// skip if none are partition, tables are handled in the previous step
		if !start.IsPartition && !end.IsPartition {
			continue
		}

		// skip if parent table id are the same (if it's a table, parent table id will be 0)
		if start.ParentTableID == end.ParentTableID {
			continue
		}

		restoreStart := shouldRestoreTable(tableId, start, cfg)
		restoreEnd := shouldRestoreTable(tableId, end, cfg)

		// error out if partition is exchanged between tables where one should restore and one shouldn't
		if restoreStart != restoreEnd {
			startDBName, exists := getDBNameFromBackup(start.DbID, snapshotDBMap, history)
			if !exists {
				startDBName = fmt.Sprintf("(unknown db name %d)", start.DbID)
			}
			endDBName, exists := getDBNameFromBackup(end.DbID, snapshotDBMap, history)
			if !exists {
				endDBName = fmt.Sprintf("(unknown db name %d)", end.DbID)
			}

			return errors.Annotatef(berrors.ErrRestoreModeMismatch,
				"partition exchange detected: partition ID %d was exchanged between table '%s.%s' (ID: %d) "+
					" and table '%s.%s' (ID: %d), but only one table will be restored (restoreStart=%v, restoreEnd=%v).",
				tableId, startDBName, start.TableName, start.ParentTableID,
				endDBName, end.TableName, end.ParentTableID, restoreStart, restoreEnd)
		}
	}
	return nil
}

func AdjustTablesToRestoreAndCreateTableTracker(
	logBackupTableHistory *stream.LogBackupTableHistoryManager,
	cfg *RestoreConfig,
	snapshotDBMap map[int64]*metautil.Database,
	snapshotTableMap map[int64]*metautil.Table,
	partitionMap map[int64]*stream.TableLocationInfo,
	tableMap map[int64]*metautil.Table,
	DBMap map[int64]*metautil.Database,
) (err error) {
	// build tracker for pitr restore to use later
	piTRIdTracker := utils.NewPiTRIdTracker()
	cfg.PiTRTableTracker = piTRIdTracker

	// track newly created databases
	newlyCreatedDBs := logBackupTableHistory.GetNewlyCreatedDBHistory()
	for dbId, dbName := range newlyCreatedDBs {
		if utils.MatchSchema(cfg.TableFilter, dbName, cfg.WithSysTable) {
			piTRIdTracker.AddDB(dbId)
		}
	}

	// first handle table renames to determine which tables we need
	processLogBackupTableHistory(logBackupTableHistory, snapshotDBMap, snapshotTableMap,
		partitionMap, cfg, tableMap, DBMap, piTRIdTracker)

	// track all snapshot tables that's going to restore in PiTR tracker
	for tableID, table := range tableMap {
		piTRIdTracker.TrackTableId(table.DB.ID, tableID)
	}

	// handle partition exchange after all tables are tracked
	if err := checkPartitionExchangesViolations(logBackupTableHistory, snapshotDBMap,
		snapshotTableMap, partitionMap, cfg); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// tweakLocalConfForRestore tweaks some of configs of TiDB to make the restore progress go well.
// return a function that could restore the config to origin.
func tweakLocalConfForRestore() func() {
	restoreConfig := config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		// set max-index-length before execute DDLs and create tables
		// we set this value to max(3072*4), otherwise we might not restore table
		// when upstream and downstream both set this value greater than default(3072)
		conf.MaxIndexLength = config.DefMaxOfMaxIndexLength
		log.Warn("set max-index-length to max(3072*4) to skip check index length in DDL")
		conf.IndexLimit = config.DefMaxOfIndexLimit
		log.Warn("set index-limit to max(64*8) to skip check index count in DDL")
		conf.TableColumnCountLimit = config.DefMaxOfTableColumnCountLimit
		log.Warn("set table-column-count to max(4096) to skip check column count in DDL")
	})
	return restoreConfig
}

func getTiFlashNodeCount(ctx context.Context, pdClient pd.Client) (uint64, error) {
	tiFlashStores, err := conn.GetAllTiKVStoresWithRetry(ctx, pdClient, connutil.TiFlashOnly)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint64(len(tiFlashStores)), nil
}

// PreCheckTableTiFlashReplica checks whether TiFlash replica is less than TiFlash node.
func PreCheckTableTiFlashReplica(
	ctx context.Context,
	pdClient pd.Client,
	tables []*metautil.Table,
	recorder *tiflashrec.TiFlashRecorder,
) error {
	tiFlashStoreCount, err := getTiFlashNodeCount(ctx, pdClient)
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
				log.Warn("table does not satisfy tiflash replica requirements, set tiflash replcia to unavailable",
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
func PreCheckTableClusterIndex(
	tables []*metautil.Table,
	ddlJobs []*model.Job,
	dom *domain.Domain,
) error {
	for _, table := range tables {
		oldTableInfo, err := restore.GetTableSchema(dom, table.DB.Name, table.Info.Name)
		// table exists in database
		if err == nil {
			if table.Info.IsCommonHandle != oldTableInfo.IsCommonHandle {
				log.Error("Clustered index option mismatch", zap.String("schemaName", table.DB.Name.O), zap.String("tableName", table.Info.Name.O))
				return errors.Annotatef(berrors.ErrRestoreModeMismatch,
					"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
					restore.TransferBoolToValue(table.Info.IsCommonHandle),
					table.Info.IsCommonHandle,
					oldTableInfo.IsCommonHandle)
			}
		}
	}
	for _, job := range ddlJobs {
		if job.Type == model.ActionCreateTable {
			tableInfo := job.BinlogInfo.TableInfo
			if tableInfo != nil {
				oldTableInfo, err := restore.GetTableSchema(dom, ast.NewCIStr(job.SchemaName), tableInfo.Name)
				// table exists in database
				if err == nil {
					if tableInfo.IsCommonHandle != oldTableInfo.IsCommonHandle {
						log.Error("Clustered index option mismatch", zap.String("schemaName", job.SchemaName), zap.String("tableName", tableInfo.Name.O))
						return errors.Annotatef(berrors.ErrRestoreModeMismatch,
							"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
							restore.TransferBoolToValue(tableInfo.IsCommonHandle),
							tableInfo.IsCommonHandle,
							oldTableInfo.IsCommonHandle)
					}
				}
			}
		}
	}
	return nil
}

func getDatabases(tables []*metautil.Table) (dbs []*model.DBInfo) {
	dbIDs := make(map[int64]bool)
	for _, table := range tables {
		if !dbIDs[table.DB.ID] {
			dbs = append(dbs, table.DB)
			dbIDs[table.DB.ID] = true
		}
	}
	return
}

// FilterDDLJobs filters ddl jobs.
func FilterDDLJobs(allDDLJobs []*model.Job, tables []*metautil.Table) (ddlJobs []*model.Job) {
	// Sort the ddl jobs by schema version in descending order.
	slices.SortFunc(allDDLJobs, func(i, j *model.Job) int {
		return cmp.Compare(j.BinlogInfo.SchemaVersion, i.BinlogInfo.SchemaVersion)
	})
	dbs := getDatabases(tables)
	for _, db := range dbs {
		// These maps is for solving some corner case.
		// e.g. let "t=2" indicates that the id of database "t" is 2, if the ddl execution sequence is:
		// rename "a" to "b"(a=1) -> drop "b"(b=1) -> create "b"(b=2) -> rename "b" to "a"(a=2)
		// Which we cannot find the "create" DDL by name and id directly.
		// To cover †his case, we must find all names and ids the database/table ever had.
		dbIDs := make(map[int64]bool)
		dbIDs[db.ID] = true
		dbNames := make(map[string]bool)
		dbNames[db.Name.String()] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.DBInfo != nil {
				if dbIDs[job.SchemaID] || dbNames[job.BinlogInfo.DBInfo.Name.String()] {
					ddlJobs = append(ddlJobs, job)
					// The the jobs executed with the old id, like the step 2 in the example above.
					dbIDs[job.SchemaID] = true
					// For the jobs executed after rename, like the step 3 in the example above.
					dbNames[job.BinlogInfo.DBInfo.Name.String()] = true
				}
			}
		}
	}

	for _, table := range tables {
		tableIDs := make(map[int64]bool)
		tableIDs[table.Info.ID] = true
		tableNames := make(map[restore.UniqueTableName]bool)
		name := restore.UniqueTableName{DB: table.DB.Name.String(), Table: table.Info.Name.String()}
		tableNames[name] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.TableInfo != nil {
				name = restore.UniqueTableName{DB: job.SchemaName, Table: job.BinlogInfo.TableInfo.Name.String()}
				if tableIDs[job.TableID] || tableNames[name] {
					ddlJobs = append(ddlJobs, job)
					tableIDs[job.TableID] = true
					// For truncate table, the id may be changed
					tableIDs[job.BinlogInfo.TableInfo.ID] = true
					tableNames[name] = true
				}
			}
		}
	}
	return ddlJobs
}

// CheckDDLJobByRules if one of rules returns true, the job in srcDDLJobs will be filtered.
func CheckDDLJobByRules(srcDDLJobs []*model.Job, rules ...DDLJobFilterRule) error {
	for _, ddlJob := range srcDDLJobs {
		for _, rule := range rules {
			if rule(ddlJob) {
				return errors.Annotatef(berrors.ErrRestoreModeMismatch, "DDL job %s is not allowed in incremental restore"+
					" when --allow-pitr-from-incremental enabled", ddlJob.String())
			}
		}
	}
	return nil
}

// FilterDDLJobByRules if one of rules returns true, the job in srcDDLJobs will be filtered.
func FilterDDLJobByRules(srcDDLJobs []*model.Job, rules ...DDLJobFilterRule) (dstDDLJobs []*model.Job) {
	dstDDLJobs = make([]*model.Job, 0, len(srcDDLJobs))
	for _, ddlJob := range srcDDLJobs {
		passed := true
		for _, rule := range rules {
			if rule(ddlJob) {
				passed = false
				break
			}
		}

		if passed {
			dstDDLJobs = append(dstDDLJobs, ddlJob)
		}
	}

	return
}

func SortKeyRanges(ids []int64, preAlloced [2]int64) [][2]kv.Key {
	if len(ids) == 0 {
		return nil
	}
	slices.Sort(ids)
	idRanges := calSortedTableIds(ids)

	if preAlloced[0] < preAlloced[1] {
		overlap := false
		for _, r := range idRanges {
			if r[0] < preAlloced[1] && r[1] > preAlloced[0] {
				overlap = true
				break
			}
		}
		if overlap {
			idRanges = append(idRanges, []int64{preAlloced[0], preAlloced[1]})
		}
	}

	mergedRanges := mergeIntervals(idRanges)

	keyRanges := make([][2]kv.Key, 0, len(mergedRanges))
	for _, r := range mergedRanges {
		startKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(r[0]))
		endKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(r[1]))
		keyRanges = append(keyRanges, [2]kv.Key{startKey, endKey})
	}
	return keyRanges
}

func calSortedTableIds(ids []int64) [][]int64 {
	if len(ids) == 0 {
		return [][]int64{}
	}

	var idRanges [][]int64

	start := ids[0]
	end := start + 1

	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1]+1 {
			end = ids[i] + 1
		} else {
			idRanges = append(idRanges, []int64{start, end})
			start = ids[i]
			end = start + 1
		}
	}
	idRanges = append(idRanges, []int64{start, end})

	return idRanges
}

func mergeIntervals(intervals [][]int64) [][]int64 {
	if len(intervals) == 0 {
		return nil
	}
	slices.SortFunc(intervals, func(a, b []int64) int {
		if a[0] < b[0] {
			return -1
		} else if a[0] > b[0] {
			return 1
		}
		return 0
	})
	merged := [][]int64{intervals[0]}
	for i := 1; i < len(intervals); i++ {
		last := merged[len(merged)-1]
		current := intervals[i]
		if current[0] <= last[1] {
			if current[1] > last[1] {
				last[1] = current[1]
			}
		} else {
			merged = append(merged, current)
		}
	}
	return merged
}

type DDLJobFilterRule func(ddlJob *model.Job) bool

var incrementalRestoreActionBlockList = map[model.ActionType]struct{}{
	model.ActionSetTiFlashReplica:          {},
	model.ActionUpdateTiFlashReplicaStatus: {},
	model.ActionLockTable:                  {},
	model.ActionUnlockTable:                {},
}

var logIncrementalRestoreCompactibleBlockList = map[model.ActionType]struct{}{
	model.ActionAddIndex:            {},
	model.ActionModifyColumn:        {},
	model.ActionReorganizePartition: {},
}

// DDLJobBlockListRule rule for filter ddl job with type in block list.
func DDLJobBlockListRule(ddlJob *model.Job) bool {
	return checkIsInActions(ddlJob.Type, incrementalRestoreActionBlockList)
}

func DDLJobLogIncrementalCompactBlockListRule(ddlJob *model.Job) bool {
	return checkIsInActions(ddlJob.Type, logIncrementalRestoreCompactibleBlockList)
}

func checkIsInActions(action model.ActionType, actions map[model.ActionType]struct{}) bool {
	_, ok := actions[action]
	return ok
}

// checkpointEnabledAndExists returns true if checkpoint has been enabled and already have checkpoint persisted
func checkpointEnabledAndExists(
	ctx context.Context,
	g glue.Glue,
	cfg *SnapshotRestoreConfig,
	mgr *conn.Mgr) (bool, error) {
	var checkpointEnabledAndExist = false
	if cfg.UseCheckpoint {
		if len(cfg.CheckpointStorage) > 0 {
			clusterID := mgr.PDClient().GetClusterID(ctx)
			if err := cfg.newStorageCheckpointMetaManagerSnapshot(ctx, clusterID); err != nil {
				return false, errors.Trace(err)
			}
		} else {
			if err := cfg.newTableCheckpointMetaManagerSnapshot(g, mgr.GetDomain()); err != nil {
				return false, errors.Trace(err)
			}
		}
		// if the checkpoint metadata exists in the checkpoint storage, the restore is not
		// for the first time.
		existsCheckpointMetadata, err := cfg.snapshotCheckpointMetaManager.ExistsCheckpointMetadata(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		checkpointEnabledAndExist = existsCheckpointMetadata
	}
	return checkpointEnabledAndExist, nil
}

// checkMandatoryClusterRequirements checks
// 1. kv mode
// 2. if db and tables are in backup if it's restore db or restore table command
// 3. check if cluster is empty if it's a full cluster restore without filter and checkpoints
// it's mandatory since it cannot be turned off
func checkMandatoryClusterRequirements(client *snapclient.SnapClient, cfg *SnapshotRestoreConfig,
	checkpointEnabledAndExists bool, cmdName string) error {
	// verify dbs and tables are in backup
	if err := VerifyDBAndTableInBackup(client.GetDatabases(), cfg.RestoreConfig); err != nil {
		return errors.Trace(err)
	}

	if isFullRestore(cmdName) {
		if client.NeedCheckFreshCluster(cfg.ExplicitFilter, checkpointEnabledAndExists) {
			if err := client.EnsureNoUserTables(); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// checkOptionalClusterRequirements checks disk space and table existence. It's optional since it can be turned off by
// config cfg.CheckRequirements
func checkOptionalClusterRequirements(
	ctx context.Context,
	client *snapclient.SnapClient,
	cfg *SnapshotRestoreConfig,
	checkpointEnabledAndExists bool,
	mgr *conn.Mgr,
	tables []*metautil.Table,
	archiveSize uint64,
	isPitr bool) error {
	if cfg.CheckRequirements && !checkpointEnabledAndExists {
		if err := checkDiskSpace(ctx, mgr, tables, archiveSize); err != nil {
			return errors.Trace(err)
		}
		if !client.IsIncremental() {
			if err := checkTableExistence(ctx, mgr, tables); err != nil {
				return errors.Trace(err)
			}
			if isPitr {
				if err := checkTableExistence(ctx, mgr, buildLogBackupMetaTables(cfg.PiTRTableTracker.DBNameToTableNames)); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
	return nil
}

func buildLogBackupMetaTables(dbNameToTableNames map[string]map[string]struct{}) []*metautil.Table {
	tables := make([]*metautil.Table, 0)

	for dbName, tableNames := range dbNameToTableNames {
		for tableName := range tableNames {
			table := &metautil.Table{
				DB: &model.DBInfo{
					Name: ast.NewCIStr(dbName),
				},
				Info: &model.TableInfo{
					Name: ast.NewCIStr(tableName),
				},
			}
			tables = append(tables, table)
		}
	}
	return tables
}

func createDBsAndTables(
	ctx context.Context,
	client *snapclient.SnapClient,
	cfg *SnapshotRestoreConfig,
	mgr *conn.Mgr,
	dbs []*metautil.Database,
	tables []*metautil.Table) ([]*restoreutils.CreatedTable, error) {
	var newTS uint64
	if client.IsIncremental() {
		if !cfg.AllowPITRFromIncremental {
			// we need to get the new ts after execDDL
			// or backfilled data in upstream may not be covered by
			// the new ts.
			// see https://github.com/pingcap/tidb/issues/54426
			var err error
			newTS, err = restore.GetTSWithRetry(ctx, mgr.GetPDClient())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	// create databases first, it will skip if already exists
	if err := client.CreateDatabases(ctx, dbs); err != nil {
		return nil, errors.Trace(err)
	}

	createdTables, err := client.CreateTables(ctx, tables, newTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return createdTables, nil
}

func setTablesRestoreModeIfNeeded(tables []*metautil.Table, cfg *SnapshotRestoreConfig, isPiTR bool) {
	if cfg.ExplicitFilter && isPiTR {
		for i, table := range tables {
			tableCopy := *table
			tableCopy.Info = table.Info.Clone()
			tableCopy.Info.Mode = model.TableModeRestore
			tables[i] = &tableCopy
		}
		log.Info("set tables to restore mode for filtered PiTR restore", zap.Int("table count", len(tables)))
	}
}
