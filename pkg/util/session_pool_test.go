// Copyright 2024 PingCAP, Inc.
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

package util_test

import (
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestSessionPool(t *testing.T) {
	re := require.New(t)
	f := func() (pools.Resource, error) { return &testResource{}, nil }
	pool := util.NewSessionPool(
		1, f,
		func(r pools.Resource) {
			r.(*testResource).refCount++
		}, func(r pools.Resource) {
			r.(*testResource).refCount--
		},
	)
	tr, err := pool.Get()
	re.NoError(err)
	re.Equal(1, tr.(*testResource).refCount)
	tr1, err := pool.Get()
	re.NoError(err)
	re.Equal(1, tr1.(*testResource).refCount)
	pool.Put(tr)
	re.Equal(0, tr.(*testResource).refCount)
	// Capacity is 1, so tr1 is closed.
	pool.Put(tr1)
	re.Equal(0, tr1.(*testResource).refCount)
	re.Equal(1, tr1.(*testResource).status)
	pool.Close()
	pool.Close()
	pool.Put(tr1)
	re.Equal(-1, tr1.(*testResource).refCount)

	tr, err = pool.Get()
	re.Error(err)
	re.Equal("session pool closed", err.Error())
	re.Nil(tr)
}

type testResource struct {
	sessionctx.Context
	status   int
	refCount int
}

func (tr *testResource) Close() { tr.status = 1 }
