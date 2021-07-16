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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {
	store, err := mockstore.NewMockStore()
	assert.NoError(t, err)
	defer func() {
		err := store.Close()
		assert.NoError(t, err)
	}()

	session.SetSchemaLease(50 * time.Millisecond)

	domain, err := session.BootstrapSession(store)
	assert.NoError(t, err)
	defer domain.Close()

	// for NotifyUpdatePrivilege
	createRoleSQL := `CREATE ROLE 'test'@'localhost';`
	se, err := session.CreateSession4Test(store)
	assert.NoError(t, err)
	_, err = se.Execute(context.Background(), createRoleSQL)
	assert.NoError(t, err)

	// for BindHandle
	_, err = se.Execute(context.Background(), "use test")
	assert.NoError(t, err)
	_, err = se.Execute(context.Background(), "drop table if exists t")
	assert.NoError(t, err)
	_, err = se.Execute(context.Background(), "create table t(i int, s varchar(20), index index_t(i, s))")
	assert.NoError(t, err)
	_, err = se.Execute(context.Background(), "create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	assert.NoError(t, err)
}
