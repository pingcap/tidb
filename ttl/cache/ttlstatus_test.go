// Copyright 2022 PingCAP, Inc.
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

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/stretchr/testify/assert"
)

func TestTTLStatusCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	sctx := conn.Context().Session
	tk := testkit.NewTestKitWithSession(t, store, sctx)
	ttlSession := session.NewSession(sctx, tk.Session(), func() {})

	isc := cache.NewTableStatusCache(time.Hour)

	// test should update
	assert.True(t, isc.ShouldUpdate())
	assert.NoError(t, isc.Update(context.Background(), ttlSession))
	assert.False(t, isc.ShouldUpdate())

	// test new entries are synced
	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id, parent_table_id) values (1, 2)")
	assert.NoError(t, isc.Update(context.Background(), ttlSession))
	assert.Equal(t, 1, len(isc.Tables))
}
