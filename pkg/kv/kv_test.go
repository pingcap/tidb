// Copyright 2025 PingCAP, Inc.
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

package kv

import (
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := range length {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func TestResourceGroupTagEncoding(t *testing.T) {
	sqlDigest := parser.NewDigest(nil)
	tag := NewResourceGroupTagBuilder(nil).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	require.Len(t, tag, 2)

	decodedSQLDigest, err := resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Len(t, decodedSQLDigest, 0)
	resTag := &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	require.Nil(t, resTag.KeyspaceName)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = NewResourceGroupTagBuilder(nil).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	require.Len(t, tag, 6)

	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	keyspaceName := []byte("123")
	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = NewResourceGroupTagBuilder(keyspaceName).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
	resTag = &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	require.NotNil(t, resTag.KeyspaceName)
	require.Equal(t, resTag.KeyspaceName, keyspaceName)

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = NewResourceGroupTagBuilder(keyspace.GetKeyspaceNameBytesBySettings()).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
	resTag = &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	if kerneltype.IsNextGen() {
		require.NotNil(t, resTag.KeyspaceName)
		require.Equal(t, resTag.KeyspaceName, keyspace.GetKeyspaceNameBytesBySettings())
	} else {
		require.Nil(t, resTag.KeyspaceName)
	}
}
