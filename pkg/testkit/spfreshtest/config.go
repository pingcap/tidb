// Copyright 2026 PingCAP, Inc.
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

// Package spfreshtest provides shared setup for SPFresh tests.
package spfreshtest

import (
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
)

// SetEnabled configures the SPFresh feature gate and restores the previous
// endpoints when the test finishes. Its placeholder endpoint is for tests that
// do not send RPCs; network tests should configure their mock server endpoints.
// Tests using this process-wide configuration must not run in parallel.
func SetEnabled(t testing.TB, enabled bool) {
	t.Helper()
	previous := slices.Clone(config.GetGlobalConfig().TiKVAPIServiceAddrs)
	t.Cleanup(func() {
		config.UpdateGlobal(func(cfg *config.Config) {
			cfg.TiKVAPIServiceAddrs = previous
		})
	})
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.TiKVAPIServiceAddrs = nil
		if enabled {
			cfg.TiKVAPIServiceAddrs = []string{"http://127.0.0.1:1"}
		}
	})
}
