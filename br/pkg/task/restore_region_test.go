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

package task

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestRestoreRegionConfig(t *testing.T) {
	cfg := &RestoreConfig{}
	require.NoError(t, cfg.validateRestoreRegionConfig("full"))
	cfg.RestoreRegion = true
	if !kerneltype.IsNextGen() {
		require.ErrorContains(t, cfg.validateRestoreRegionConfig("full"), "nextgen")
		return
	}
	require.NoError(t, cfg.validateRestoreRegionConfig("full"))
	for _, test := range []struct {
		name string
		edit func(*RestoreConfig)
	}{
		{"online", func(c *RestoreConfig) { c.Online = true }},
		{"existing-tables", func(c *RestoreConfig) { c.NoSchema = true }},
		{"rate-limit", func(c *RestoreConfig) { c.RateLimit = 1 }},
		{"phased", func(c *RestoreConfig) { c.RestorePhase = 1 }},
		{"ebs", func(c *RestoreConfig) { c.FullBackupType = FullBackupTypeEBS }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := &RestoreConfig{RestoreRegion: true}
			test.edit(c)
			require.Error(t, c.validateRestoreRegionConfig("full"))
		})
	}
}

func TestRestoreRegionFlag(t *testing.T) {
	flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
	DefineRestoreFlags(flags)
	enabled, err := flags.GetBool(flagRestoreRegion)
	require.NoError(t, err)
	require.False(t, enabled)
	require.NoError(t, flags.Parse([]string{"--experimental-restore-region"}))
	enabled, err = flags.GetBool(flagRestoreRegion)
	require.NoError(t, err)
	require.True(t, enabled)
}
