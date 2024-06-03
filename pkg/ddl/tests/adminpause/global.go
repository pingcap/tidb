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

package adminpause

import (
	"testing"
	"time"

	ddlctrl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
)

const dbTestLease = 600 * time.Millisecond

// Logger is the global logger in this package
var Logger = logutil.DDLLogger()

func prepareDomain(t *testing.T) (*domain.Domain, *testkit.TestKit, *testkit.TestKit) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	stmtKit := testkit.NewTestKit(t, store)
	adminCommandKit := testkit.NewTestKit(t, store)

	ddlctrl.ReorgWaitTimeout = 10 * time.Millisecond
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	stmtKit = testkit.NewTestKit(t, store)
	stmtKit.MustExec("use test")

	return dom, stmtKit, adminCommandKit
}
