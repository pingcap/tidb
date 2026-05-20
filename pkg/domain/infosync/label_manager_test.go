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

package infosync

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestFilterLabelRulesByKeyspace(t *testing.T) {
	rules := []*label.Rule{
		{ID: "schema/test/t1"},
		{ID: "keyspace/42/schema/test/t2"},
		{ID: "keyspace/43/schema/test/t3"},
	}

	codecV1 := tikv.NewCodecV1(tikv.ModeTxn)
	require.Equal(t, rules, filterRulesByKeyspace(rules, codecV1))

	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 42})
	require.NoError(t, err)

	if kerneltype.IsClassic() {
		require.Equal(t, rules, filterRulesByKeyspace(rules, codecV2))
		return
	}

	require.Equal(t, []*label.Rule{rules[1]}, filterRulesByKeyspace(rules, codecV2))
}
