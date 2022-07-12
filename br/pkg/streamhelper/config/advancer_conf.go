// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
	"time"

	"github.com/spf13/pflag"
)

const (
	flagBackoffTime          = "backoff-time"
	flagMaxBackoffTime       = "max-backoff-time"
	flagTickInterval         = "tick-interval"
	flagFullScanDiffTick     = "full-scan-tick"
	flagAdvancingByCache     = "advancing-by-cache"
	flagConsistencyCheckTick = "consistency-check-tick"
	flagUpdateSmallTreeTick  = "update-small-tree-tick"

	DefaultConsistencyCheckTick            = 5
	DefaultTickInterval                    = 3 * time.Second
	DefaultBackOffTime                     = 5 * time.Second
	DefaultTryAdvanceThreshold             = 3 * time.Minute
	DefaultTickBeforeUpdateCheckpointLight = 5
	DefaultTickBeforeFullScanRetry         = 2
	DefaultAdvancingByCache                = true
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
	AdvancingByCache     bool `toml:"advancing-by-cache" json:"advancing-by-cache"`
	ConsistencyCheckTick int  `toml:"consistency-check-tick" json:"consistency-check-tick"`
	UpdateSmallTreeTick  int  `toml:"update-small-tree-tick" json:"update-small-tree-tick"`
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagTickInterval, DefaultTickInterval, "From how log we trigger the tick (advancing the checkpoint).")
	f.Bool(flagAdvancingByCache, DefaultAdvancingByCache, "Whether enable the optimization -- use a cached heap to advancing the global checkpoint.")
	f.Int(flagFullScanDiffTick, DefaultTickBeforeFullScanRetry, "The backoff of full scan.")
	f.Duration(flagBackoffTime, DefaultBackOffTime, "The gap between two retries.")
	f.Int(flagConsistencyCheckTick, DefaultConsistencyCheckTick, "The gap between two consistency check.")
	f.Int(flagUpdateSmallTreeTick, DefaultTickBeforeUpdateCheckpointLight, "The gap between two update small tree.")
}

func Default() Config {
	return Config{
		BackoffTime:          DefaultBackOffTime,
		TickDuration:         DefaultTickInterval,
		FullScanTick:         DefaultTickBeforeFullScanRetry,
		AdvancingByCache:     DefaultAdvancingByCache,
		ConsistencyCheckTick: DefaultConsistencyCheckTick,
		UpdateSmallTreeTick:  DefaultTickBeforeUpdateCheckpointLight,
	}
}

// GetFromFlags fills the config from the FlagSet.
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
	conf.ConsistencyCheckTick, err = f.GetInt(flagConsistencyCheckTick)
	if err != nil {
		return err
	}
	conf.UpdateSmallTreeTick, err = f.GetInt(flagUpdateSmallTreeTick)
	if err != nil {
		return err
	}
	return nil
}
