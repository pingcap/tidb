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
	"context"
	"math/rand"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
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

func (c *CheckpointAdvancer) ForceResolveLocksForTest() {
	c.lastCheckpointMu.Lock()
	defer c.lastCheckpointMu.Unlock()
	if c.lastCheckpoint != nil {
		c.lastCheckpoint.resolveLockTime = time.Now().Add(-4 * time.Minute)
	}
}

func (c *CheckpointAdvancer) ResolveLockMaxVersionForTest() uint64 {
	c.lastCheckpointMu.Lock()
	lastCheckpoint := c.lastCheckpoint
	c.lastCheckpointMu.Unlock()
	if lastCheckpoint == nil {
		return 0
	}

	maxTs := uint64(0)
	c.checkpointsMu.Lock()
	defer c.checkpointsMu.Unlock()
	c.checkpoints.TraverseValuesLessThan(tsoAfter(lastCheckpoint.TS, time.Minute), func(v spans.Valued) bool {
		maxTs = max(maxTs, v.Value)
		return true
	})
	return maxTs + 1
}

func SetGlobalCheckpointStorageFactoryForTest(
	factory func(context.Context, *backuppb.StorageBackend, bool) (storage.ExternalStorage, error),
) func() {
	original := createGlobalCheckpointStorage
	createGlobalCheckpointStorage = factory
	return func() {
		createGlobalCheckpointStorage = original
	}
}
