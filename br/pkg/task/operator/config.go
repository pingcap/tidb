// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"regexp"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/backup"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/spf13/pflag"
)

const (
	flagTableConcurrency = "table-concurrency"
	flagStorePatterns    = "stores"
	flagTTL              = "ttl"
	flagSafePoint        = "safepoint"
	flagStorage          = "storage"
	flagLoadCreds        = "load-creds"
	flagJSON             = "json"
	flagRecent           = "recent"
	flagTo               = "to"
	flagBase             = "base"
	flagYes              = "yes"
	flagDryRun           = "dry-run"
)

type PauseGcConfig struct {
	task.Config

	SafePoint uint64        `json:"safepoint" yaml:"safepoint"`
	TTL       time.Duration `json:"ttl" yaml:"ttl"`

	OnAllReady func() `json:"-" yaml:"-"`
	OnExit     func() `json:"-" yaml:"-"`
}

func DefineFlagsForPrepareSnapBackup(f *pflag.FlagSet) {
	_ = f.DurationP(flagTTL, "i", 2*time.Minute, "The time-to-live of the safepoint.")
	_ = f.Uint64P(flagSafePoint, "t", 0, "The GC safepoint to be kept.")
}

// ParseFromFlags fills the config via the flags.
func (cfg *PauseGcConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	if err := cfg.Config.ParseFromFlags(flags); err != nil {
		return err
	}

	var err error
	cfg.SafePoint, err = flags.GetUint64(flagSafePoint)
	if err != nil {
		return err
	}
	cfg.TTL, err = flags.GetDuration(flagTTL)
	if err != nil {
		return err
	}

	return nil
}

type Base64ifyConfig struct {
	storage.BackendOptions
	StorageURI string
	LoadCerd   bool
}

func DefineFlagsForBase64ifyConfig(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringP(flagStorage, "s", "", "The external storage input.")
	flags.Bool(flagLoadCreds, false, "whether loading the credientials from current environment and marshal them to the base64 string. [!]")
}

func (cfg *Base64ifyConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	err = cfg.BackendOptions.ParseFromFlags(flags)
	if err != nil {
		return err
	}
	cfg.StorageURI, err = flags.GetString(flagStorage)
	if err != nil {
		return err
	}
	cfg.LoadCerd, err = flags.GetBool(flagLoadCreds)
	if err != nil {
		return err
	}
	return nil
}

type ListMigrationConfig struct {
	storage.BackendOptions
	StorageURI string
	JSONOutput bool
}

func DefineFlagsForListMigrationConfig(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringP(flagStorage, "s", "", "the external storage input.")
	flags.Bool(flagJSON, false, "output the result in json format.")
}

func (cfg *ListMigrationConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	err = cfg.BackendOptions.ParseFromFlags(flags)
	if err != nil {
		return err
	}
	cfg.StorageURI, err = flags.GetString(flagStorage)
	if err != nil {
		return err
	}
	cfg.JSONOutput, err = flags.GetBool(flagJSON)
	if err != nil {
		return err
	}
	return nil
}

type MigrateToConfig struct {
	storage.BackendOptions
	StorageURI string
	Recent     bool
	MigrateTo  int
	Base       bool

	Yes    bool
	DryRun bool
}

func DefineFlagsForMigrateToConfig(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringP(flagStorage, "s", "", "the external storage input.")
	flags.Bool(flagRecent, true, "migrate to the most recent migration and BASE.")
	flags.Int(flagTo, 0, "migrate all migrations from the BASE to the specified sequence number.")
	flags.Bool(flagBase, false, "don't merge any migrations, just retry run pending operations in BASE.")
	flags.BoolP(flagYes, "y", false, "skip all effect estimating and confirming. execute directly.")
	flags.Bool(flagDryRun, false, "do not actually perform the migration, just print the effect.")
}

func (cfg *MigrateToConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	err = cfg.BackendOptions.ParseFromFlags(flags)
	if err != nil {
		return err
	}
	cfg.StorageURI, err = flags.GetString(flagStorage)
	if err != nil {
		return err
	}
	cfg.Recent, err = flags.GetBool(flagRecent)
	if err != nil {
		return err
	}
	cfg.MigrateTo, err = flags.GetInt(flagTo)
	if err != nil {
		return err
	}
	cfg.Base, err = flags.GetBool(flagBase)
	if err != nil {
		return err
	}
	cfg.Yes, err = flags.GetBool(flagYes)
	if err != nil {
		return err
	}
	cfg.DryRun, err = flags.GetBool(flagDryRun)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *MigrateToConfig) Verify() error {
	if cfg.Recent && cfg.MigrateTo != 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"the --%s and --%s flag cannot be used at the same time",
			flagRecent, flagTo)
	}
	if cfg.Base && (cfg.Recent || cfg.MigrateTo != 0) {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"the --%s and ( --%s or --%s ) flag cannot be used at the same time",
			flagBase, flagTo, flagRecent)
	}
	return nil
}

type ForceFlushConfig struct {
	task.Config

	// StoresPattern matches the address of TiKV.
	// The address usually looks like "<host>:20160".
	// You may list the store by `pd-ctl stores`.
	StoresPattern *regexp.Regexp
}

func DefineFlagsForForceFlushConfig(f *pflag.FlagSet) {
	f.String(flagStorePatterns, ".*", "The regexp to match the store peer address to be force flushed.")
}

func (cfg *ForceFlushConfig) ParseFromFlags(flags *pflag.FlagSet) (err error) {
	storePat, err := flags.GetString(flagStorePatterns)
	if err != nil {
		return err
	}
	cfg.StoresPattern, err = regexp.Compile(storePat)
	if err != nil {
		return errors.Annotatef(err, "invalid expression in --%s", flagStorePatterns)
	}

	return cfg.Config.ParseFromFlags(flags)
}

type ChecksumWithRewriteRulesConfig struct {
	task.Config
}

func DefineFlagsForChecksumTableConfig(f *pflag.FlagSet) {
	f.Uint(flagTableConcurrency, backup.DefaultSchemaConcurrency, "The size of a BR thread pool used for backup table metas, "+
		"including tableInfo/checksum and stats.")
}

func (cfg *ChecksumWithRewriteRulesConfig) ParseFromFlags(flags *pflag.FlagSet) (err error) {
	cfg.TableConcurrency, err = flags.GetUint(flagTableConcurrency)
	if err != nil {
		return
	}
	return cfg.Config.ParseFromFlags(flags)
}
