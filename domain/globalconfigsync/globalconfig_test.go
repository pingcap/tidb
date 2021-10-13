// Copyright 2021 PingCAP, Inc.
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

package globalconfigsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlobalConfigSyncer(t *testing.T) {
	syncer := NewGlobalConfigSyncer(nil)
	syncer.Notify("a", "b")
	require.Equal(t, len(syncer.NotifyCh), 1)
	entry := <-syncer.NotifyCh
	require.Equal(t, entry.name, "a")
	require.Equal(t, true, syncer.needUpdate(entry))
	syncer.updateCache(entry)
	require.Equal(t, false, syncer.needUpdate(entry))
	entry.name = "c"
	require.Equal(t, true, syncer.needUpdate(entry))
	err := syncer.StoreGlobalConfig(context.Background(), entry)
	require.NoError(t, err)
}
