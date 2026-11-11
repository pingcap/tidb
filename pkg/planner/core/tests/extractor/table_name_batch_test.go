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

package extractor_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type tableMetaReadClient struct {
	tikv.Client
	sync.Mutex
	keys       map[string]struct{}
	pointGets  int
	batchSizes []int
}

func (c *tableMetaReadClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.Lock()
	switch req.Type {
	case tikvrpc.CmdGet:
		if _, ok := c.keys[string(req.Get().Key)]; ok {
			c.pointGets++
		}
	case tikvrpc.CmdBatchGet:
		n := 0
		for _, key := range req.BatchGet().Keys {
			if _, ok := c.keys[string(key)]; ok {
				n++
			}
		}
		if n > 0 {
			c.batchSizes = append(c.batchSizes, n)
		}
	}
	c.Unlock()
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestInfoSchemaTableNameBatchLoad(t *testing.T) {
	client := &tableMetaReadClient{}
	store := testkit.CreateMockStore(t, mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
		client.Client = c
		return client
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_schema_cache_size = 1073741824")
	t.Cleanup(func() { tk.MustExec("set global tidb_schema_cache_size = default") })
	tk.MustExec("create database batch_meta")
	tk.MustExec("use batch_meta")
	names := make([]string, 5)
	want := make([]string, len(names))
	for i := range names {
		names[i] = fmt.Sprintf("t%d", i)
		want[i] = names[i] + " a"
		tk.MustExec(fmt.Sprintf("create table %s (a int primary key, b int, key(b))", names[i]))
	}
	is := domain.GetDomain(tk.Session()).InfoSchema()
	ok, v2 := infoschema.IsV2(is)
	require.True(t, ok)
	schema := ast.NewCIStr("batch_meta")
	db, ok := is.SchemaByName(schema)
	require.True(t, ok)
	encoder := structure.NewStructure(nil, nil, []byte("m"))
	keys := make(map[string]struct{}, len(names))
	ids := make([]int64, len(names))
	for i, name := range names {
		tbl, err := is.TableByName(context.Background(), schema, ast.NewCIStr(name))
		require.NoError(t, err)
		ids[i] = tbl.Meta().ID
		key := encoder.EncodeHashDataKey(fmt.Appendf(nil, "DB:%d", db.ID), meta.TableKey(ids[i]))
		keys[string(key)] = struct{}{}
		v2.EvictTable(schema, ast.NewCIStr(name))
	}
	client.Lock()
	client.keys = keys
	client.Unlock()
	for _, source := range []string{"columns", "statistics"} {
		predicate := "column_name = 'a'"
		if source == "statistics" {
			predicate = "index_name = 'PRIMARY'"
		}
		sql := fmt.Sprintf("select table_name, column_name from information_schema.%s where table_schema = database() and table_name in ('%s', 'MISSING', 'T0') and %s order by table_name", source, strings.Join(names, "','"), predicate)
		for range 2 {
			client.Lock()
			client.pointGets, client.batchSizes = 0, nil
			client.Unlock()
			// MustQuery also re-executes information_schema queries to compare
			// extractor behavior; count only this execution's metadata requests.
			tk.MustQueryWithContext(context.Background(), sql).Check(testkit.Rows(want...))
			client.Lock()
			gets, batches := client.pointGets, append([]int(nil), client.batchSizes...)
			client.Unlock()
			require.Zero(t, gets, source)
			require.Equal(t, []int{len(names)}, batches, source)
			for _, id := range ids {
				require.False(t, v2.TableIsCached(id), source)
			}
		}
	}
}
