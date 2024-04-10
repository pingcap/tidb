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
	flagCheckPointLagLimit  = "check-point-lag-limit"

	DefaultConsistencyCheckTick = 5
	DefaultTryAdvanceThreshold  = 4 * time.Minute
	DefaultCheckPointLagLimit   = 48 * time.Hour
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
	// The maximum lag could be tolerated for the checkpoint lag.
	CheckPointLagLimit time.Duration `toml:"check-point-lag-limit" json:"check-point-lag-limit"`
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagBackoffTime, DefaultBackOffTime,
		"The gap between two retries.")
	f.Duration(flagTickInterval, DefaultTickInterval,
		"From how long we trigger the tick (advancing the checkpoint).")
	f.Duration(flagTryAdvanceThreshold, DefaultTryAdvanceThreshold,
		"If the checkpoint lag is greater than how long, we would try to poll TiKV for checkpoints.")
	f.Duration(flagCheckPointLagLimit, DefaultCheckPointLagLimit,
		"The maximum lag could be tolerated for the checkpoint lag.")
}

func Default() Config {
	return Config{
		BackoffTime:         DefaultBackOffTime,
		TickDuration:        DefaultTickInterval,
		TryAdvanceThreshold: DefaultTryAdvanceThreshold,
		CheckPointLagLimit:  DefaultCheckPointLagLimit,
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
	conf.CheckPointLagLimit, err = f.GetDuration(flagCheckPointLagLimit)
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

// GetCheckPointLagLimit returns the maximum lag could be tolerated for the checkpoint lag.
func (conf Config) GetCheckPointLagLimit() time.Duration {
	return conf.CheckPointLagLimit
}

// GetSubscriberErrorStartPollThreshold returns the threshold of begin polling the checkpoint
// when the subscriber meets error.
func (conf Config) GetSubscriberErrorStartPollThreshold() time.Duration {
	// 0.45x of the origin threshold.
	// The origin threshold is 0.8x the target RPO,
	// and the default flush interval is about 0.5x the target RPO.
	// So the relationship between the RPO and the threshold is:
	// When subscription is all available, it is 1.7x of the flush interval (which allow us to save in abnormal condition).
	// When some of subscriptions are not available, it is 0.75x of the flush interval.
	// NOTE: can we make subscription better and give up the poll model?
	return conf.TryAdvanceThreshold * 9 / 20
}

// TickTimeout returns the max duration for each tick.
func (conf Config) TickTimeout() time.Duration {
	// If a tick blocks longer than the interval of ticking, we may need to break it and retry.
	return conf.TickDuration
}
