// Copyright 2019 PingCAP, Inc.
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

package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/pingcap/tidb/ddl"
)

// SubTestDomainSession is batched in TestDomainSerial
func SubTestDomainSession(t *testing.T) {
	lease := 50 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	ddl.DisableTiFlashPoll(domain.DDL())
	defer domain.Close()

	// for NotifyUpdatePrivilege
	createRoleSQL := `CREATE ROLE 'test'@'localhost';`
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), createRoleSQL)
	require.NoError(t, err)

	// for BindHandle
	_, err = se.Execute(context.Background(), "use test")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "drop table if exists t")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "create table t(i int, s varchar(20), index index_t(i, s))")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	require.NoError(t, err)
}
