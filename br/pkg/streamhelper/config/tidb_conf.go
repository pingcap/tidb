// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

type TiDBConfig struct {
	*CommandConfig
}

func DefaultTiDBConfig() Config {
	return &TiDBConfig{
		CommandConfig: defaultCommandConfig(),
	}
}

// The maximum lag could be tolerated for the checkpoint lag.
func (conf *TiDBConfig) GetCheckPointLagLimit() time.Duration {
	return vardef.AdvancerCheckPointLagLimit.Load()
}
