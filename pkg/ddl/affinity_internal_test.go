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

package ddl

import (
	"context"
	"net/http"
	"testing"

	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type contextAffinityPDClient struct {
	pdhttp.Client
	calls int
}

func (c *contextAffinityPDClient) BatchDeleteAffinityGroups(ctx context.Context, _ []string, _ bool) error {
	c.calls++
	if _, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://pd/affinity-groups", nil); err != nil {
		return err
	}
	return ctx.Err()
}

func TestAffinityBatchDeleteStepContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil_step", true: "cancelled_step"}[cancelled], func(t *testing.T) {
			cli := &contextAffinityPDClient{}
			affinity.InitManager(cli)
			t.Cleanup(func() { affinity.InitManager(nil) })
			jobCtx := &jobContext{ctx: context.Background()}
			if cancelled {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				jobCtx.stepCtx = ctx
			}
			err := batchDeleteTableAffinityGroups(jobCtx, map[string]struct{}{"g1": {}})
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, cli.calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, cli.calls)
			}
		})
	}
}
