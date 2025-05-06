// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

// InjectMockBackendCtx mock LitBackCtxMgr.
func InjectMockBackendCtx(t *testing.T, store kv.Storage) (restore func()) {
	oldLitDiskRoot := ingest.LitDiskRoot
	oldLitMemRoot := ingest.LitMemRoot

	tk := testkit.NewTestKit(t, store)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockNewBackendContext",
		func(job *model.Job, cpMgr *ingest.CheckpointManager, mockBackendCtx *ingest.BackendCtx) {
			*mockBackendCtx = ingest.NewMockBackendCtx(job, tk.Session(), cpMgr)
		})
	ingest.LitInitialized = true
	ingest.LitDiskRoot = ingest.NewDiskRootImpl(t.TempDir())
	ingest.LitMemRoot = ingest.NewMemRootImpl(math.MaxInt64)

	return func() {
		ingest.LitInitialized = false
		ingest.LitDiskRoot = oldLitDiskRoot
		ingest.LitMemRoot = oldLitMemRoot
	}
}

// CheckIngestLeakageForTest is only used in test.
func CheckIngestLeakageForTest(exitCode int) {
	if exitCode == 0 {
		leakObj := ""
		if ingest.TrackerCountForTest.Load() != 0 {
			leakObj = "disk usage tracker"
		} else if ingest.BackendCounterForTest.Load() != 0 {
			leakObj = "backend context"
		}
		if len(leakObj) > 0 {
			fmt.Fprintf(os.Stderr, "add index leakage check failed: %s leak\n", leakObj)
			os.Exit(1)
		}
		if registeredJob := metrics.GetRegisteredJob(); len(registeredJob) > 0 {
			fmt.Fprintf(os.Stderr, "add index metrics leakage: %v\n", registeredJob)
			os.Exit(1)
		}
	}
	os.Exit(exitCode)
}
