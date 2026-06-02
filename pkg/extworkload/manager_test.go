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

package extworkload

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestInitManagerGated verifies the front-door gates of InitManager:
// non-Starter deploy mode and Enable=false short-circuit before touching
// keyspace meta, so we can pass a nil meta without panicking. The Starter
// path requires a nextgen build and is exercised by integration tests in
// follow-up worker PRs.
func TestInitManagerGated(t *testing.T) {
	prev := globalManager
	t.Cleanup(func() { globalManager = prev })
	globalManager = nil

	// Non-Starter deploy mode (the default in tests) skips init entirely.
	require.NoError(t, InitManager(context.Background(), nil, config.ExternalWorkload{Enable: true}))
	require.Nil(t, globalManager)

	// Enable=false also skips init.
	require.NoError(t, InitManager(context.Background(), nil, config.ExternalWorkload{}))
	require.Nil(t, globalManager)
}
