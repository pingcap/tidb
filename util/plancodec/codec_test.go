// Copyright 2020 PingCAP, Inc.
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

package plancodec

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

type encodeTaskTypeCase struct {
	IsRoot     bool
	StoreType  kv.StoreType
	EncodedStr string
	DecodedStr string
}

func TestEncodeTaskType(t *testing.T) {
	cases := []encodeTaskTypeCase{
		{true, kv.UnSpecified, "0", "root"},
		{false, kv.TiKV, "1_0", "cop[tikv]"},
		{false, kv.TiFlash, "1_1", "cop[tiflash]"},
		{false, kv.TiDB, "1_2", "cop[tidb]"},
	}
	for _, cas := range cases {
		require.Equal(t, cas.EncodedStr, EncodeTaskType(cas.IsRoot, cas.StoreType))
		str, err := decodeTaskType(cas.EncodedStr)
		require.NoError(t, err)
		require.Equal(t, cas.DecodedStr, str)
	}

	str, err := decodeTaskType("1")
	require.NoError(t, err)
	require.Equal(t, "cop", str)

	_, err = decodeTaskType("1_x")
	require.Error(t, err)
}

func TestDecodeDiscardPlan(t *testing.T) {
	plan, err := DecodePlan(PlanDiscardedEncoded)
	require.NoError(t, err)
	require.Equal(t, planDiscardedDecoded, plan)
}
