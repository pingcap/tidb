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

package config

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticLogConfig(t *testing.T) {
	c := NewConfig()
	require.NotNil(t, c.DiagnosticLog.Types)
	require.Empty(t, c.DiagnosticLog.Types)
	for _, value := range []string{`[]`, `["deadlock"]`, `["index_usage"]`, `["deadlock","index_usage","deadlock"]`} {
		c = NewConfig()
		_, err := toml.Decode("[diagnostic-log]\ntypes = "+value, c)
		require.NoError(t, err)
		require.NoError(t, c.Valid())
		require.LessOrEqual(t, len(c.DiagnosticLog.Types), 2)
	}
	c.DiagnosticLog.Types = []string{"unknown"}
	require.ErrorContains(t, c.Valid(), "unsupported diagnostic-log.types")
}
