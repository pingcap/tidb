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

package spfreshtest_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit/spfreshtest"
	"github.com/stretchr/testify/require"
)

func TestSetEnabledRestoresNestedConfiguration(t *testing.T) {
	t.Cleanup(config.RestoreFunc())
	original := []string{"http://first-worker:20180", "http://second-worker:20180"}
	config.UpdateGlobal(func(cfg *config.Config) { cfg.TiKVAPIServiceAddrs = original })
	t.Run("enabled", func(t *testing.T) {
		spfreshtest.SetEnabled(t, true)
		require.True(t, config.SPFreshEnabled())
		endpoints := append([]string(nil), config.GetGlobalConfig().TiKVAPIServiceAddrs...)
		t.Run("disabled", func(t *testing.T) {
			spfreshtest.SetEnabled(t, false)
			require.False(t, config.SPFreshEnabled())
		})
		require.True(t, config.SPFreshEnabled())
		require.Equal(t, endpoints, config.GetGlobalConfig().TiKVAPIServiceAddrs)
	})
	require.Equal(t, original, config.GetGlobalConfig().TiKVAPIServiceAddrs)
}
