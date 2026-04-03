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

package loaddatatest

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type checkKVPrioClient struct {
	tikv.Client

	want    kvrpcpb.CommandPri
	enabled int32
}

func (c *checkKVPrioClient) enable(want kvrpcpb.CommandPri) {
	c.want = want
	atomic.StoreInt32(&c.enabled, 1)
}

func (c *checkKVPrioClient) disable() {
	atomic.StoreInt32(&c.enabled, 0)
}

func (c *checkKVPrioClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if ctx.Value(c) != nil && atomic.LoadInt32(&c.enabled) == 1 {
		// LOAD DATA uses KV reads for conflict checks and KV writes (2PC) for inserting rows.
		// Only check request types that are expected to be part of that path, to reduce noise.
		switch req.Type {
		case tikvrpc.CmdBatchGet, tikvrpc.CmdGet, tikvrpc.CmdScan,
			tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdCleanup, tikvrpc.CmdBatchRollback:
			if req.Priority != c.want {
				return nil, errors.New("unexpected kv request priority")
			}
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestLoadDataLowPrioritySetsKVLowPriority(t *testing.T) {
	cli := &checkKVPrioClient{}
	store := testkit.CreateMockStore(t, mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}))

	// Use a context marker so the priority checker only applies to requests issued by this test execution.
	ctx := context.WithValue(context.Background(), cli, 42)

	tk := testkit.NewTestKit(t, store)
	sctx := tk.Session().(sessionctx.Context)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists load_data_low_prio")
	// Use explicit primary key values so LOAD DATA doesn't need to allocate _tidb_rowid (autoid/meta txn),
	// which would generate high-priority internal KV requests.
	tk.MustExec("create table load_data_low_prio (a int primary key, b int unique)")

	var reader io.ReadCloser = mydump.NewStringReader("1\t10\n")
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (io.ReadCloser, error) {
		return reader, nil
	}
	sctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	cli.enable(kvrpcpb.CommandPri_Low)
	tk.MustExecWithContext(ctx, "load data low_priority local infile '/tmp/nonexistence.csv' into table load_data_low_prio")
	cli.disable()

	tk.MustQuery("select * from load_data_low_prio").Check(testkit.Rows("1 10"))
	require.NoError(t, reader.Close())
}
