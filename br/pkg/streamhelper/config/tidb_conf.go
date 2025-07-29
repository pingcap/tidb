// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
