// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
	"time"

	"github.com/spf13/pflag"
)

const (
	flagBackoffTime         = "backoff-time"
	flagTickInterval        = "tick-interval"
	flagFullScanDiffTick    = "full-scan-tick"
	flagAdvancingByCache    = "advancing-by-cache"
	flagTryAdvanceThreshold = "try-advance-threshold"

	DefaultConsistencyCheckTick = 5
	DefaultTryAdvanceThreshold  = 9 * time.Minute
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
	// The threshold for polling TiKV for checkpoint of some range.
	TryAdvanceThreshold time.Duration `toml:"try-advance-threshold" json:"try-advance-threshold"`
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagBackoffTime, DefaultBackOffTime, "The gap between two retries.")
	f.Duration(flagTickInterval, DefaultTickInterval, "From how long we trigger the tick (advancing the checkpoint).")
	f.Duration(flagTryAdvanceThreshold, DefaultTryAdvanceThreshold, "If the checkpoint lag is greater than how long, we would try to poll TiKV for checkpoints.")
}

func Default() Config {
	return Config{
		BackoffTime:         DefaultBackOffTime,
		TickDuration:        DefaultTickInterval,
		TryAdvanceThreshold: DefaultTryAdvanceThreshold,
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
	conf.TryAdvanceThreshold, err = f.GetDuration(flagTryAdvanceThreshold)
	if err != nil {
		return err
	}
	return nil
}

// GetDefaultStartPollThreshold returns the threshold of begin polling the checkpoint
// in the normal condition (the subscribe manager is available.)
func (conf Config) GetDefaultStartPollThreshold() time.Duration {
	return conf.TryAdvanceThreshold
}

// GetSubscriberErrorStartPollThreshold returns the threshold of begin polling the checkpoint
// when the subscriber meets error.
func (conf Config) GetSubscriberErrorStartPollThreshold() time.Duration {
	return conf.TryAdvanceThreshold / 5
}
