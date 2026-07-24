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

package sessiontest

import (
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestCrossKSRuntimeGCLoopStartedBySystemDomain(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockRuntimeGCLoopConfig",
		func(interval, idleTimeout *time.Duration) {
			*interval = 50 * time.Millisecond
			*idleTimeout = 100 * time.Millisecond
		})

	const targetKS = "keyspace1"
	_, systemDom := realtikvtest.CreateMockStoreAndDomainAndSetup(t,
		realtikvtest.WithKeyspaceName(keyspace.System),
		realtikvtest.WithAllocPort(true))

	handle, err := systemDom.AcquireKSRuntime(targetKS, "test/cross-ks-gc-loop")
	require.NoError(t, err)
	require.Contains(t, systemDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)

	handle.Release()

	require.Eventually(t, func() bool {
		return !slices.Contains(systemDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)
	}, 5*time.Second, 20*time.Millisecond)
}
