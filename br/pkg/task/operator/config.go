// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"time"

	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/pflag"
)

type PauseGcConfig struct {
	task.Config

	SafePoint uint64        `json:"safepoint" yaml:"safepoint"`
	TTL       time.Duration `json:"ttl" yaml:"ttl"`
}

func DefineFlagsForPauseGcConfig(f *pflag.FlagSet) {
	_ = f.DurationP("ttl", "i", 5*time.Minute, "The time-to-live of the safepoint.")
	_ = f.Uint64P("safepoint", "t", 0, "The GC safepoint to be kept.")
}

func (cfg *PauseGcConfig) ParseFromFlags(flags *pflag.FlagSet) {
	cfg.Config.ParseFromFlags(flags)

	cfg.SafePoint = utils.Must(flags.GetUint64("safepoint"))
	cfg.TTL = utils.Must(flags.GetDuration("ttl"))
}
