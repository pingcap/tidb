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

package streamhelper

import (
	"math/rand"
	"time"

	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

func NewCheckpointAdvancer(env Env) *CheckpointAdvancer {
	if rand.Int()&1 == 0 {
		return NewTiDBCheckpointAdvancer(env)
	}
	return NewCommandCheckpointAdvancer(env)
}

// UpdateConfigWith updates the config by modifying the current config.
func (c *CheckpointAdvancer) UpdateConfigWith(f func(*config.CommandConfig)) {
	if cfg, ok := c.cfg.(*config.CommandConfig); ok {
		f(cfg)
		c.UpdateConfig(cfg)
	} else {
		cfg := c.cfg.(*config.TiDBConfig)
		f(cfg.CommandConfig)
		c.UpdateConfig(cfg)
	}
}

func (c *CheckpointAdvancer) UpdateCheckPointLagLimit(limit time.Duration) {
	if cfg, ok := c.cfg.(*config.CommandConfig); ok {
		cfg.CheckPointLagLimit = limit
		c.UpdateConfig(cfg)
	} else {
		variable.AdvancerCheckPointLagLimit.Store(limit)
	}
}
