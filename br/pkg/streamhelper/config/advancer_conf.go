// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
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
	DefaultBackOffTime          = 5 * time.Second
	DefaultTickInterval         = 12 * time.Second
	DefaultFullScanTick         = 4
	DefaultAdvanceByCache       = true
)

var (
	DefaultMaxConcurrencyAdvance = 8
)

type Config struct {
	// The gap between two retries.
	BackoffTime time.Duration `toml:"backoff-time" json:"backoff-time"`
	// The gap between calculating checkpoints.
	TickDuration time.Duration `toml:"tick-interval" json:"tick-interval"`
	// The backoff time of full scan.
	FullScanTick int `toml:"full-scan-tick" json:"full-scan-tick"`

	// Whether enable the optimization -- use a cached heap to advancing the global checkpoint.
	// This may reduce the gap of checkpoint but may cost more CPU.
	AdvancingByCache bool `toml:"advancing-by-cache" json:"advancing-by-cache"`
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagBackoffTime, DefaultBackOffTime, "The gap between two retries.")
	f.Duration(flagTickInterval, DefaultTickInterval, "From how log we trigger the tick (advancing the checkpoint).")
	f.Bool(flagAdvancingByCache, DefaultAdvanceByCache, "Whether enable the optimization -- use a cached heap to advancing the global checkpoint.")
	f.Int(flagFullScanDiffTick, DefaultFullScanTick, "The backoff of full scan.")
}

func Default() Config {
	return Config{
		BackoffTime:      DefaultBackOffTime,
		TickDuration:     DefaultTickInterval,
		FullScanTick:     DefaultFullScanTick,
		AdvancingByCache: DefaultAdvanceByCache,
	}
}

func (conf *Config) GetFromFlags(f *pflag.FlagSet) error {
	var err error
	conf.BackoffTime, err = f.GetDuration(flagBackoffTime)
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
