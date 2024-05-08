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

package logrestore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

type fakeSplitClient struct {
	split.SplitClient
	regions []*split.RegionInfo
}

func newFakeSplitClient() *fakeSplitClient {
	return &fakeSplitClient{
		regions: make([]*split.RegionInfo, 0),
	}
}

func (f *fakeSplitClient) AppendRegion(startKey, endKey []byte) {
	f.regions = append(f.regions, &split.RegionInfo{
		Region: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	})
}

func (f *fakeSplitClient) ScanRegions(ctx context.Context, startKey, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	result := make([]*split.RegionInfo, 0)
	count := 0
	for _, rng := range f.regions {
		if bytes.Compare(rng.Region.StartKey, endKey) <= 0 && bytes.Compare(rng.Region.EndKey, startKey) > 0 {
			result = append(result, rng)
			count++
		}
		if count >= limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSplitClient) WaitRegionsScattered(context.Context, []*split.RegionInfo) (int, error) {
	return 0, nil
}

func keyWithTablePrefix(tableID int64, key string) []byte {
	rawKey := append(tablecodec.GenTableRecordPrefix(tableID), []byte(key)...)
	return codec.EncodeBytes([]byte{}, rawKey)
}

func TestSplitPoint(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e       g         i
	//            +---+ +---+       +---------+
	//          +-------------+----------+---------+
	// region:  a             f          h         j
	splitHelper := split.NewSplitHelper()
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: split.Value{Size: 100, Number: 100}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "g"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: split.Value{Size: 300, Number: 300}})
	client := newFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "f"))
	client.AppendRegion(keyWithTablePrefix(tableID, "f"), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "j"))
	client.AppendRegion(keyWithTablePrefix(tableID, "j"), keyWithTablePrefix(tableID+1, "a"))

	iter := NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := SplitPoint(ctx, iter, client, func(ctx context.Context, rs *utils.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []split.Valued) error {
		require.Equal(t, u, uint64(0))
		require.Equal(t, o, int64(0))
		require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
		require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "f"))
		require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
		require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
		require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
		require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
		require.Equal(t, len(v), 2)
		return nil
	})
	require.NoError(t, err)
}

func getCharFromNumber(prefix string, i int) string {
	c := '1' + (i % 10)
	b := '1' + (i%100)/10
	a := '1' + i/100
	return fmt.Sprintf("%s%c%c%c", prefix, a, b, c)
}

func TestSplitPoint2(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e f                 i j    k l        n
	//            +---+ +---+ +-----------------+ +----+ +--------+
	//          +---------------+--+.....+----+------------+---------+
	// region:  a               g   >128      h            m         o
	splitHelper := split.NewSplitHelper()
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: split.Value{Size: 100, Number: 100}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "f"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: split.Value{Size: 300, Number: 300}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "j"), EndKey: keyWithTablePrefix(oldTableID, "k")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "l"), EndKey: keyWithTablePrefix(oldTableID, "n")}, Value: split.Value{Size: 200, Number: 200}})
	client := newFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "g"))
	client.AppendRegion(keyWithTablePrefix(tableID, "g"), keyWithTablePrefix(tableID, getCharFromNumber("g", 0)))
	for i := 0; i < 256; i++ {
		client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", i)), keyWithTablePrefix(tableID, getCharFromNumber("g", i+1)))
	}
	client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", 256)), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "m"))
	client.AppendRegion(keyWithTablePrefix(tableID, "m"), keyWithTablePrefix(tableID, "o"))
	client.AppendRegion(keyWithTablePrefix(tableID, "o"), keyWithTablePrefix(tableID+1, "a"))

	firstSplit := true
	iter := NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := SplitPoint(ctx, iter, client, func(ctx context.Context, rs *utils.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []split.Valued) error {
		if firstSplit {
			require.Equal(t, u, uint64(0))
			require.Equal(t, o, int64(0))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "g"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
			require.EqualValues(t, v[2].Key.StartKey, keyWithTablePrefix(tableID, "f"))
			require.EqualValues(t, v[2].Key.EndKey, keyWithTablePrefix(tableID, "g"))
			require.Equal(t, v[2].Value.Size, uint64(1))
			require.Equal(t, v[2].Value.Number, int64(1))
			require.Equal(t, len(v), 3)
			firstSplit = false
		} else {
			require.Equal(t, u, uint64(1))
			require.Equal(t, o, int64(1))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "h"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "m"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "j"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "k"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "l"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "m"))
			require.Equal(t, v[1].Value.Size, uint64(100))
			require.Equal(t, v[1].Value.Number, int64(100))
			require.Equal(t, len(v), 2)
		}
		return nil
	})
	require.NoError(t, err)
}

type mockLogIter struct {
	next int
}

func (m *mockLogIter) TryNext(ctx context.Context) iter.IterResult[*LogDataFileInfo] {
	if m.next > 10000 {
		return iter.Done[*LogDataFileInfo]()
	}
	m.next += 1
	return iter.Emit(&LogDataFileInfo{
		DataFileInfo: &backuppb.DataFileInfo{
			StartKey: []byte(fmt.Sprintf("a%d", m.next)),
			EndKey:   []byte("b"),
			Length:   1024, // 1 KB
		},
	})
}

func TestLogFilesIterWithSplitHelper(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	rewriteRules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}
	rewriteRulesMap := map[int64]*utils.RewriteRules{
		oldTableID: rewriteRules,
	}
	mockIter := &mockLogIter{}
	ctx := context.Background()
	logIter := NewLogFilesIterWithSplitHelper(mockIter, rewriteRulesMap, newFakeSplitClient(), 144*1024*1024, 1440000)
	next := 0
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		require.NoError(t, r.Err)
		next += 1
		require.Equal(t, []byte(fmt.Sprintf("a%d", next)), r.Item.StartKey)
	}
}
