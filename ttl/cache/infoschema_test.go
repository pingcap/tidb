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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/stretchr/testify/assert"
)

func TestInfoSchemaCache(t *testing.T) {
	parser.TTLFeatureGate = true

	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	sctx := conn.Context().Session
	tk := testkit.NewTestKitWithSession(t, store, sctx)
	se := session.NewSession(sctx, sctx, func() {})

	isc := cache.NewInfoSchemaCache(time.Hour)

	// test should update
	assert.True(t, isc.ShouldUpdate())
	assert.NoError(t, isc.Update(se))
	assert.False(t, isc.ShouldUpdate())

	// test new tables are synced
	assert.Equal(t, 0, len(isc.Tables))
	tk.MustExec("create table test.t(created_at datetime) ttl = created_at + INTERVAL 5 YEAR")
	assert.NoError(t, isc.Update(se))
	assert.Equal(t, 1, len(isc.Tables))
	for _, table := range isc.Tables {
		assert.Equal(t, "t", table.TableInfo.Name.L)
	}

	// test new partitioned table are synced
	tk.MustExec("drop table test.t")
	tk.MustExec(`create table test.t(created_at datetime)
		ttl = created_at + INTERVAL 5 YEAR
		partition by range (YEAR(created_at)) (
			partition p0 values less than (1991),
			partition p1 values less than (2000)
		)
	`)
	assert.NoError(t, isc.Update(se))
	assert.Equal(t, 2, len(isc.Tables))
	partitions := []string{}
	for id, table := range isc.Tables {
		assert.Equal(t, "t", table.TableInfo.Name.L)
		assert.Equal(t, id, table.PartitionDef.ID)
		partitions = append(partitions, table.PartitionDef.Name.L)
	}
	assert.ElementsMatch(t, []string{"p0", "p1"}, partitions)
}
