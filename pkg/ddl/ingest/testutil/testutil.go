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
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

// InjectMockBackendMgr mock LitBackCtxMgr.
func InjectMockBackendMgr(t *testing.T, store kv.Storage) (restore func()) {
	oldInitialized := ingest.LitInitialized
	oldLitDiskRoot := ingest.LitDiskRoot

	tk := testkit.NewTestKit(t, store)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockNewBackendContext",
		func(job *model.Job, mockBackendCtx *ingest.BackendCtx) {
			tk.MustExec("rollback;")
			tk.MustExec("begin;")
			*mockBackendCtx = ingest.NewMockBackendCtx(job, tk.Session())
		})
	ingest.LitInitialized = true
	ingest.LitDiskRoot = ingest.NewDiskRootImpl(t.TempDir())

	return func() {
		ingest.LitInitialized = oldInitialized
		ingest.LitDiskRoot = oldLitDiskRoot
	}
}
