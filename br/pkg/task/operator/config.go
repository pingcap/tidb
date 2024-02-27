// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"time"

	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/spf13/pflag"
)

type PauseGcConfig struct {
	task.Config

	SafePoint uint64        `json:"safepoint" yaml:"safepoint"`
	TTL       time.Duration `json:"ttl" yaml:"ttl"`

	OnAllReady func() `json:"-" yaml:"-"`
	OnExit     func() `json:"-" yaml:"-"`
}

func DefineFlagsForPrepareSnapBackup(f *pflag.FlagSet) {
	_ = f.DurationP("ttl", "i", 2*time.Minute, "The time-to-live of the safepoint.")
	_ = f.Uint64P("safepoint", "t", 0, "The GC safepoint to be kept.")
}

// ParseFromFlags fills the config via the flags.
func (cfg *PauseGcConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	if err := cfg.Config.ParseFromFlags(flags); err != nil {
		return err
	}

	var err error
	cfg.SafePoint, err = flags.GetUint64("safepoint")
	if err != nil {
		return err
	}
	cfg.TTL, err = flags.GetDuration("ttl")
	if err != nil {
		return err
	}

	return nil
}
