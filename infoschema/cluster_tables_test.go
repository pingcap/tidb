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

package infoschema_test

import (
	"fmt"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type clusterTablesSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func TestStmtSummaryIssue35340(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)

	tk := s.newTestKitWithRoot(t)
	tk.MustExec("set global tidb_stmt_summary_refresh_interval=1800")
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 3000")
	for i := 0; i < 100; i++ {
		user := "user" + strconv.Itoa(i)
		tk.MustExec(fmt.Sprintf("create user '%v'@'localhost'", user))
	}
	tk.MustExec("flush privileges")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := s.newTestKitWithRoot(t)
			for j := 0; j < 100; j++ {
				user := "user" + strconv.Itoa(j)
				require.True(t, tk.Session().Auth(&auth.UserIdentity{
					Username: user,
					Hostname: "localhost",
				}, nil, nil))
				tk.MustQuery("select count(*) from information_schema.statements_summary;")
			}
		}()
	}
	wg.Wait()
}

func (s *clusterTablesSuite) newTestKitWithRoot(t *testing.T) *testkit.TestKit {
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	return tk
}
