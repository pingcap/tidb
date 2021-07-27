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
// See the License for the specific language governing permissions and
// limitations under the License.

package resourcegrouptag

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupTagEncoding(t *testing.T) {
	t.Parallel()

	sqlDigest := parser.NewDigest(nil)
	tag := EncodeResourceGroupTag(sqlDigest, nil)
	require.Len(t, tag, 0)

	decodedSQLDigest, err := DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Len(t, decodedSQLDigest, 0)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	require.Len(t, tag, 4)

	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
}

func TestResourceGroupTagEncodingPB(t *testing.T) {
	t.Parallel()

	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	// Test for protobuf
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	buf, err := resourceTag.Marshal()
	require.NoError(t, err)
	require.Len(t, buf, 68)

	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, digest1, tag.SqlDigest)
	require.Equal(t, digest2, tag.PlanDigest)

	// Test for protobuf sql_digest only
	resourceTag = &tipb.ResourceGroupTag{
		SqlDigest: digest1,
	}
	buf, err = resourceTag.Marshal()
	require.NoError(t, err)
	require.Len(t, buf, 34)

	tag = &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, digest1, tag.SqlDigest)
	require.Nil(t, tag.PlanDigest)
}

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func genDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write(hack.Slice(str))
	return hasher.Sum(nil)
}
