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

package domain

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pderr "github.com/tikv/pd/client/errs"
)

func TestShouldUseDegradedResourceGroupFallback(t *testing.T) {
	ctx := context.Background()
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{
			name: "resource manager unavailable",
			ctx:  ctx,
			err: &pderr.ErrClientGetResourceGroup{
				ResourceGroupName: "rg1",
				Cause:             "resource group unavailable",
			},
			want: true,
		},
		{
			name: "resource group not found",
			ctx:  ctx,
			err: &pderr.ErrClientGetResourceGroup{
				ResourceGroupName: "rg1",
				Cause:             "resource group does not exist",
			},
			want: false,
		},
		{
			name: "context canceled",
			ctx:  canceledCtx,
			err: &pderr.ErrClientGetResourceGroup{
				ResourceGroupName: "rg1",
				Cause:             "resource group unavailable",
			},
			want: false,
		},
		{
			name: "lookup canceled",
			ctx:  ctx,
			err:  context.Canceled,
			want: false,
		},
		{
			name: "config unavailable",
			ctx:  ctx,
			err:  pderr.ErrClientResourceGroupConfigUnavailable.FastGenByArgs("not configured"),
			want: true,
		},
		{
			name: "generic non retryable",
			ctx:  ctx,
			err:  errors.New("invalid resource group lookup"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldUseDegradedResourceGroupFallback(tt.ctx, tt.err))
		})
	}
}
