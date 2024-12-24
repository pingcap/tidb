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

package bindinfo

import (
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func bindingNoDBDigest(t *testing.T, b Binding) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(b.BindSQL, b.Charset, b.Collation)
	require.NoError(t, err)
	_, noDBDigest := norm.NormalizeStmtForBinding(stmt, norm.WithoutDB(true))
	return noDBDigest
}

func TestCrossDBBindingCache(t *testing.T) {
	fbc := newBindCache(nil).(*bindingCache)
	b1 := Binding{BindSQL: "SELECT * FROM db1.t1", SQLDigest: "b1"}
	fDigest1 := bindingNoDBDigest(t, b1)
	b2 := Binding{BindSQL: "SELECT * FROM db2.t1", SQLDigest: "b2"}
	b3 := Binding{BindSQL: "SELECT * FROM db2.t3", SQLDigest: "b3"}
	fDigest3 := bindingNoDBDigest(t, b3)

	// add 3 bindings and b1 and b2 have the same noDBDigest
	require.NoError(t, fbc.SetBinding(b1.SQLDigest, []Binding{b1}))
	require.NoError(t, fbc.SetBinding(b2.SQLDigest, []Binding{b2}))
	require.NoError(t, fbc.SetBinding(b3.SQLDigest, []Binding{b3}))
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest), 2) // b1 and b2 have the same noDBDigest
	require.Equal(t, len(fbc.digestBiMap.NoDBDigest2SQLDigest(fDigest1)), 2)
	require.Equal(t, len(fbc.digestBiMap.NoDBDigest2SQLDigest(fDigest3)), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest), 3)
	_, ok := fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b2.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b3.SQLDigest]
	require.True(t, ok)

	// remove b2
	fbc.RemoveBinding(b2.SQLDigest)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest), 2)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest[fDigest1]), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest[fDigest3]), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest), 2)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b2.SQLDigest]
	require.False(t, ok) // can't find b2 now
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b3.SQLDigest]
	require.True(t, ok)
}

func TestBindCache(t *testing.T) {
	bindings := Bindings{{BindSQL: "SELECT * FROM t1"}}
	kvSize := len("digest1") + int(bindings.size())
	variable.MemQuotaBindingCache.Store(int64(kvSize*3) - 1)
	bindCache := newBindCache(nil)

	err := bindCache.SetBinding("digest1", bindings)
	require.Nil(t, err)
	require.NotNil(t, bindCache.GetBinding("digest1"))

	err = bindCache.SetBinding("digest2", bindings)
	require.Nil(t, err)
	require.NotNil(t, bindCache.GetBinding("digest2"))

	err = bindCache.SetBinding("digest3", bindings)
	require.NotNil(t, err) // exceed the memory limit
	require.NotNil(t, bindCache.GetBinding("digest2"))

	require.Nil(t, bindCache.GetBinding("digest1"))    // digest1 is evicted
	require.NotNil(t, bindCache.GetBinding("digest2")) // digest2 is still in the cache
}
