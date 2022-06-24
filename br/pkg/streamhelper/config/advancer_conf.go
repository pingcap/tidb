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
)

type Config struct {
	// The gap between two retries.
	BackoffTime time.Duration
	// When after this time we cannot collect the safe resolved ts, give up.
	MaxBackoffTime time.Duration
	// The gap between calculating checkpoints.
	TickDuration time.Duration
	// The backoff time of full scan.
	FullScanTick int
}

func DefineFlagsForCheckpointAdvancerConfig(f *pflag.FlagSet) {
	f.Duration(flagBackoffTime, 5*time.Second, "The gap between two retries.")
	f.Duration(flagMaxBackoffTime, 5*time.Minute, "After how long we should advance the checkpoint.")
	f.Duration(flagTickInterval, 5*time.Second, "From how log we trigger the tick (advancing the checkpoint).")
	f.Int(flagFullScanDiffTick, 60, "The backoff of full scan.")
}

func Default() Config {
	return Config{
		BackoffTime:    5 * time.Second,
		MaxBackoffTime: 5 * time.Minute,
		TickDuration:   5 * time.Second,
		FullScanTick:   60,
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
	return nil
}
