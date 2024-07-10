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

package domain

import (
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/require"
)

func TestSessionPool(t *testing.T) {
	f := func() (pools.Resource, error) { return &testResource{}, nil }
	pool := newSessionPool(1, f)
	tr, err := pool.Get()
	require.NoError(t, err)
	tr1, err := pool.Get()
	require.NoError(t, err)
	pool.Put(tr)
	// Capacity is 1, so tr1 is closed.
	pool.Put(tr1)
	require.Equal(t, 1, tr1.(*testResource).status)
	pool.Close()
	pool.Close()
	pool.Put(tr1)

	tr, err = pool.Get()
	require.Error(t, err)
	require.Equal(t, "session pool closed", err.Error())
	require.Nil(t, tr)
}

type testResource struct {
	sessionctx.Context
	status int
}

func (tr *testResource) Close() { tr.status = 1 }
