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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
)

// InjectMockBackendMgr mock LitBackCtxMgr.
func InjectMockBackendMgr(t *testing.T, store kv.Storage) (restore func()) {
	tk := testkit.NewTestKit(t, store)
	oldLitBackendMgr := ingest.LitBackCtxMgr
	oldInitialized := ingest.LitInitialized

	ingest.LitBackCtxMgr = ingest.NewMockBackendCtxMgr(func() sessionctx.Context {
		tk.MustExec("rollback;")
		tk.MustExec("begin;")
		return tk.Session()
	})
	ingest.LitInitialized = true

	return func() {
		ingest.LitBackCtxMgr = oldLitBackendMgr
		ingest.LitInitialized = oldInitialized
	}
}
