// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checksum

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestBuilderRequestSource(t *testing.T) {
	b := NewExecutorBuilder(nil, 0)
	require.Equal(t, util.RequestSource{}, b.requestSource)

	b.SetExplicitRequestSourceType("aaa")
	require.EqualValues(t, util.RequestSource{ExplicitRequestSourceType: "aaa"}, b.requestSource)

	reqSource := util.RequestSource{RequestSourceInternal: true, RequestSourceType: "type", ExplicitRequestSourceType: "bbb"}
	b.SetRequestSource(reqSource)
	require.EqualValues(t, reqSource, b.requestSource)
}

func TestBuilderTiKVClientReadTimeout(t *testing.T) {
	table := &model.TableInfo{
		ID:    1,
		State: model.StatePublic,
		Indices: []*model.IndexInfo{
			{ID: 1, Name: ast.NewCIStr("idx"), State: model.StatePublic},
		},
	}

	// The default keeps the global copr-req-timeout behavior (0).
	executor, err := NewExecutorBuilder(table, 0).Build()
	require.NoError(t, err)
	require.NoError(t, executor.Each(func(req *kv.Request) error {
		require.Equal(t, uint64(0), req.TiKVClientReadTimeout)
		return nil
	}))

	// Both the table and the index request carry the per-request deadline.
	executor, err = NewExecutorBuilder(table, 0).SetTiKVClientReadTimeout(266000).Build()
	require.NoError(t, err)
	reqCount := 0
	require.NoError(t, executor.Each(func(req *kv.Request) error {
		require.Equal(t, uint64(266000), req.TiKVClientReadTimeout)
		reqCount++
		return nil
	}))
	require.Equal(t, 2, reqCount) // table + index
}
