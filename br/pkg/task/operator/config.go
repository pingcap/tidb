// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"regexp"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/spf13/pflag"
)

const (
	flagTableConcurrency = "table-concurrency"
	flagStorePatterns    = "stores"
	flagTTL              = "ttl"
	flagSafePoint        = "safepoint"
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
