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

package session

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var GetBootstrapVersion = getBootstrapVersion
var CurrentBootstrapVersion = currentBootstrapVersion

func TestIndexUsageSyncLease(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	dom.SetStatsUpdating(true)
	se, err := CreateSession(store)
	require.NoError(t, err)

	sess, ok := se.(*session)
	require.True(t, ok)
	require.Nil(t, sess.idxUsageCollector)

	SetIndexUsageSyncLease(1)
	defer SetIndexUsageSyncLease(0)

	se, err = CreateSession(store)
	require.NoError(t, err)
	sess, ok = se.(*session)
	require.True(t, ok)
	require.NotNil(t, sess.idxUsageCollector)
}
