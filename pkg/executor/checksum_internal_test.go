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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestChecksumTiKVClientReadTimeout(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	affinityTable := &model.TableInfo{Affinity: &model.TableAffinityInfo{Level: "table"}}
	plainTable := &model.TableInfo{}

	// An explicit @@tikv_client_read_timeout wins over everything, including
	// the affinity-derived deadline and non-affinity tables.
	sessVars.TiKVClientReadTimeout = 300000
	require.Equal(t, uint64(300000), checksumTiKVClientReadTimeout(sessVars, affinityTable, 999))
	require.Equal(t, uint64(300000), checksumTiKVClientReadTimeout(sessVars, plainTable, 999))

	// With the variable unset, non-affinity tables keep the global default (0),
	// and affinity tables use the derived deadline.
	sessVars.TiKVClientReadTimeout = 0
	require.Equal(t, uint64(0), checksumTiKVClientReadTimeout(sessVars, plainTable, 999))
	require.Equal(t, uint64(999), checksumTiKVClientReadTimeout(sessVars, affinityTable, 999))
}

// blockingChecksumClient simulates a checksum RPC stuck in the send path: it
// only returns once its context is cancelled.
type blockingChecksumClient struct {
	kv.Client
	entered chan struct{}
}

func (c *blockingChecksumClient) Send(
	ctx context.Context, _ *kv.Request, _ any, _ *kv.ClientSendOption,
) kv.Response {
	close(c.entered)
	<-ctx.Done()
	return nil
}

// checksumMockCtx overrides GetClient on a mock session context.
type checksumMockCtx struct {
	*mock.Context
	cli kv.Client
}

func (c *checksumMockCtx) GetClient() kv.Client { return c.cli }

func TestChecksumRequestCancellable(t *testing.T) {
	mctx := &checksumMockCtx{Context: mock.NewContext()}
	fake := &blockingChecksumClient{entered: make(chan struct{})}
	mctx.cli = fake
	e := &ChecksumTableExec{BaseExecutor: exec.NewBaseExecutor(mctx, nil, 0)}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := e.handleChecksumRequest(ctx, &kv.Request{})
		errCh <- err
	}()

	// Wait until the request is stuck in the send path, then cancel the
	// statement context: the RPC must give up instead of waiting out the
	// whole coprocessor deadline.
	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("checksum request never entered the send path")
	}
	cancel()
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("checksum request did not exit after cancellation")
	}
}
