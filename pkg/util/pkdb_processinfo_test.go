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

package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessInfoToRowProxyHost(t *testing.T) {
	t.Run("proxy protocol connection", func(t *testing.T) {
		// Set a captured proxy peer, convert the process info to a row, and verify
		// the final information schema column preserves the address.
		info := &ProcessInfo{ProxyHost: "10.0.0.1:12345"}

		row := info.ToRow(time.UTC)

		require.Equal(t, "10.0.0.1:12345", row[len(row)-1])
	})

	t.Run("direct connection", func(t *testing.T) {
		// Leave the proxy peer empty, convert the process info to a row, and verify
		// the final information schema column is SQL NULL.
		info := &ProcessInfo{}

		row := info.ToRow(time.UTC)

		require.Nil(t, row[len(row)-1])
	})
}
