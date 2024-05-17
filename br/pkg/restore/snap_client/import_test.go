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

package snapclient_test

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestGetKeyRangeByMode(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	endFile := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte(""),
	}
	rule := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte("t1"),
				NewKeyPrefix: []byte("t2"),
			},
		},
	}
	// raw kv
	testRawFn := snapclient.GetKeyRangeByMode(snapclient.Raw)
	start, end, err := testRawFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte("t1ccc"), end)

	start, end, err = testRawFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte(""), end)

	// txn kv: the keys must be encoded.
	testTxnFn := snapclient.GetKeyRangeByMode(snapclient.Txn)
	start, end, err = testTxnFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1ccc")), end)

	start, end, err = testTxnFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, []byte(""), end)

	// normal kv: the keys must be encoded.
	testFn := snapclient.GetKeyRangeByMode(snapclient.TiDB)
	start, end, err = testFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2ccc")), end)

	// TODO maybe fix later
	// current restore does not support rewrite empty endkey.
	// because backup guarantees that the end key is not empty.
	// start, end, err = testFn(endFile, rule)
	// require.NoError(t, err)
	// require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	// require.Equal(t, []byte(""), end)
}

func TestGetSSTMetaFromFile(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta, err := snapclient.GetSSTMetaFromFile([]byte{}, file, region, rule, snapclient.RewriteModeLegacy)
	require.Nil(t, err)
	require.Equal(t, "t2abc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}
