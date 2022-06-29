// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
	"runtime"
	"time"

	"github.com/spf13/pflag"
)

const (
	flagBackoffTime      = "backoff-time"
	flagMaxBackoffTime   = "max-backoff-time"
	flagTickInterval     = "tick-interval"
	flagFullScanDiffTick = "full-scan-tick"
	flagAdvancingByCache = "advancing-by-cache"

	DefaultConsistencyCheckTick = 5
	DefaultTryAdvanceThreshold  = 3 * time.Minute
)

var (
	DefaultMaxConcurrencyAdvance = uint((runtime.NumCPU() + 1) / 2)
)

type Config struct {
	// The gap between two retries.
	BackoffTime time.Duration `toml:"backoff-time" json:"backoff-time"`
	// When after this time we cannot collect the safe resolved ts, give up.
	MaxBackoffTime time.Duration `toml:"max-backoff-time" json:"max-backoff-time"`
	// The gap between calculating checkpoints.
	TickDuration time.Duration `toml:"tick-interval" json:"tick-interval"`
	// The backoff time of full scan.
	FullScanTick int `toml:"full-scan-tick" json:"full-scan-tick"`

	// Whether enable the optimization -- use a cached heap to advancing the global checkpoint.
	// This may reduce the gap of checkpoint but may cost more CPU.
	AdvancingByCache bool `toml:"advancing-by-cache" json:"advancing-by-cache"`
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagBackoffTime, 5*time.Second, "The gap between two retries.")
	f.Duration(flagMaxBackoffTime, 20*time.Minute, "After how long we should advance the checkpoint.")
	f.Duration(flagTickInterval, 12*time.Second, "From how log we trigger the tick (advancing the checkpoint).")
	f.Bool(flagAdvancingByCache, true, "Whether enable the optimization -- use a cached heap to advancing the global checkpoint.")
	f.Int(flagFullScanDiffTick, 4, "The backoff of full scan.")
}

func Default() Config {
	return Config{
		BackoffTime:      5 * time.Second,
		MaxBackoffTime:   20 * time.Minute,
		TickDuration:     12 * time.Second,
		FullScanTick:     4,
		AdvancingByCache: true,
	}
}

func (conf *Config) GetFromFlags(f *pflag.FlagSet) error {
	var err error
	conf.BackoffTime, err = f.GetDuration(flagBackoffTime)
	if err != nil {
		return err
	}
	conf.MaxBackoffTime, err = f.GetDuration(flagMaxBackoffTime)
	if err != nil {
		return err
	}
	conf.TickDuration, err = f.GetDuration(flagTickInterval)
	if err != nil {
		return err
	}
	conf.FullScanTick, err = f.GetInt(flagFullScanDiffTick)
	if err != nil {
		return err
	}
	conf.AdvancingByCache, err = f.GetBool(flagAdvancingByCache)
	if err != nil {
		return err
	}
	return nil
}
