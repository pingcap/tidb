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

package deploymode

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func TestModeJSON(t *testing.T) {
	data, err := json.Marshal(PremiumReserved)
	require.NoError(t, err)
	require.Equal(t, `"premium_reserved"`, string(data))

	data, err = json.Marshal(Starter)
	require.NoError(t, err)
	require.Equal(t, `"starter"`, string(data))

	var mode Mode
	require.NoError(t, json.Unmarshal([]byte(`"premium"`), &mode))
	require.Equal(t, Premium, mode)

	require.NoError(t, json.Unmarshal([]byte(`"premium_reserved"`), &mode))
	require.Equal(t, PremiumReserved, mode)

	require.NoError(t, json.Unmarshal([]byte(`"Premium_Reserved"`), &mode))
	require.Equal(t, PremiumReserved, mode)

	require.NoError(t, json.Unmarshal([]byte(`"Starter"`), &mode))
	require.Equal(t, Starter, mode)

	require.ErrorContains(t, json.Unmarshal([]byte(`"unknown"`), &mode), `invalid deploy mode "unknown"`)
	require.Error(t, json.Unmarshal([]byte(`1`), &mode))
}

func TestModeTOML(t *testing.T) {
	var cfg struct {
		Mode Mode `toml:"deploy-mode"`
	}

	_, err := toml.Decode(`deploy-mode = "premium_reserved"`, &cfg)
	require.NoError(t, err)
	require.Equal(t, PremiumReserved, cfg.Mode)

	_, err = toml.Decode(`deploy-mode = "Premium"`, &cfg)
	require.NoError(t, err)
	require.Equal(t, Premium, cfg.Mode)

	_, err = toml.Decode(`deploy-mode = "Starter"`, &cfg)
	require.NoError(t, err)
	require.Equal(t, Starter, cfg.Mode)
}

func TestCurrentMode(t *testing.T) {
	original := Get()
	t.Cleanup(func() {
		currentMode.Store(int32(original))
	})

	if !kerneltype.IsNextGen() {
		require.Equal(t, Premium, Get())
		currentMode.Store(int32(PremiumReserved))
		require.False(t, IsPremiumReserved())
		currentMode.Store(int32(Starter))
		require.False(t, IsStarter())
		require.ErrorContains(t, Set(PremiumReserved), "deploy mode can only be set for nextgen TiDB")
		return
	}

	require.Equal(t, Premium, Get())
	require.False(t, IsPremiumReserved())
	require.False(t, IsStarter())
	require.NoError(t, Set(PremiumReserved))
	require.Equal(t, PremiumReserved, Get())
	require.True(t, IsPremiumReserved())
	require.False(t, IsStarter())
	require.NoError(t, Set(Starter))
	require.Equal(t, Starter, Get())
	require.False(t, IsPremiumReserved())
	require.True(t, IsStarter())
	require.ErrorContains(t, Set(Mode(100)), "invalid deploy mode")
}
