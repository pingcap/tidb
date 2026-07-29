// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"context"
	"hash/crc32"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type codecStorage struct {
	tidbkv.Storage
	codec tikv.Codec
}

func (s *codecStorage) GetCodec() tikv.Codec {
	return s.codec
}

type notifyingCodec struct {
	tikv.Codec
	decoded chan struct{}
}

func (c *notifyingCodec) DecodeKey(key []byte) ([]byte, error) {
	decodedKey, err := c.Codec.DecodeKey(key)
	c.decoded <- struct{}{}
	return decodedKey, err
}

func requireKVPairChannelClosed(t *testing.T, ch <-chan *simplesst.KVPair) {
	t.Helper()
	select {
	case _, ok := <-ch:
		require.False(t, ok)
	default:
		require.FailNow(t, "KV-pair channel was not closed")
	}
}

func drainClosedKVPairChannel(
	t *testing.T,
	ch <-chan *simplesst.KVPair,
	visit func(*simplesst.KVPair),
) {
	t.Helper()
	for {
		select {
		case pair, ok := <-ch:
			if !ok {
				return
			}
			visit(pair)
		default:
			require.FailNow(t, "KV-pair channel was not closed")
		}
	}
}

func makeUniqueIndexKVPair(
	t *testing.T,
	store *codecStorage,
	indexValue int64,
	handle tidbkv.Handle,
) *simplesst.KVPair {
	t.Helper()
	encodedValue, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(indexValue))
	require.NoError(t, err)
	key := tablecodec.EncodeIndexSeekKey(1, 2, encodedValue)
	return &simplesst.KVPair{
		Key:   store.GetCodec().EncodeKey(key),
		Value: tablecodec.EncodeHandleInUniqueIndexValue(handle, false),
	}
}

func TestCollectConflictsKVGroupIndexInfo(t *testing.T) {
	var tableImporter *importer.TableImporter

	indexInfo, err := getKVGroupIndexInfo(tableImporter, globalsort.DataKVGroup)
	require.NoError(t, err)
	require.Nil(t, indexInfo)

	_, err = getKVGroupIndexInfo(tableImporter, "not-an-index-id")
	require.Error(t, err)

	tableInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t")}
	mockTable := tables.MockTableFromMeta(tableInfo)
	require.NotNil(t, mockTable)
	targetIdx := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("mv"), MVIndex: true}
	tableInfo.Indices = []*model.IndexInfo{targetIdx}
	tableImporter = &importer.TableImporter{
		LoadDataController: &importer.LoadDataController{Table: mockTable},
	}

	indexInfo, err = getKVGroupIndexInfo(tableImporter, globalsort.IndexID2KVGroup(targetIdx.ID))
	require.NoError(t, err)
	require.Same(t, targetIdx, indexInfo)

	_, err = getKVGroupIndexInfo(tableImporter, globalsort.IndexID2KVGroup(3))
	require.EqualError(t, err, `index 3 from KV group "3" not found in table t`)
}

func TestDispatchMVIndexKVPairs(t *testing.T) {
	targetIdx := &model.IndexInfo{
		ID:      2,
		MVIndex: true,
		Columns: []*model.IndexColumn{{}},
	}
	t.Run("channel selection", func(t *testing.T) {
		pairCh := make(chan *simplesst.KVPair)
		testCases := []struct {
			name         string
			concurrency  int
			targetIdx    *model.IndexInfo
			needDispatch bool
		}{
			{name: "MV index", concurrency: 4, targetIdx: targetIdx, needDispatch: true},
			{name: "single MV index handler", concurrency: 1, targetIdx: targetIdx},
			{name: "normal index", concurrency: 4, targetIdx: &model.IndexInfo{}},
			{name: "data KV", concurrency: 4},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				handlerChs, needDispatch := createConflictHandlerChannels(
					pairCh,
					testCase.concurrency,
					testCase.targetIdx,
				)
				require.Equal(t, testCase.needDispatch, needDispatch)
				require.Len(t, handlerChs, testCase.concurrency)
				for i, handlerCh := range handlerChs {
					if testCase.needDispatch {
						require.NotEqual(t, pairCh, handlerCh)
						require.Equal(t, conflictedkv.BufferedHandleLimit, cap(handlerCh))
						for j := range i {
							require.NotEqual(t, handlerChs[j], handlerCh)
						}
					} else {
						require.Equal(t, pairCh, handlerCh)
					}
				}
			})
		}
	})

	commonHandleBytes, err := codec.EncodeKey(time.UTC, nil, types.NewStringDatum("common-handle"))
	require.NoError(t, err)
	commonHandle, err := tidbkv.NewCommonHandle(commonHandleBytes)
	require.NoError(t, err)
	handles := []tidbkv.Handle{tidbkv.IntHandle(1), tidbkv.IntHandle(2), commonHandle}

	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 1})
	require.NoError(t, err)
	for name, tikvCodec := range map[string]tikv.Codec{
		"api v1": tikv.NewCodecV1(tikv.ModeTxn),
		"api v2": codecV2,
	} {
		t.Run(name, func(t *testing.T) {
			store := &codecStorage{codec: tikvCodec}
			pairCh := make(chan *simplesst.KVPair, len(handles)*2)
			for i, handle := range handles {
				pairCh <- makeUniqueIndexKVPair(t, store, int64(i*2+1), handle)
				pairCh <- makeUniqueIndexKVPair(t, store, int64(i*2+2), handle)
			}
			close(pairCh)

			const handlerCount = 4
			handlerChs := make([]chan *simplesst.KVPair, handlerCount)
			for i := range handlerChs {
				handlerChs[i] = make(chan *simplesst.KVPair, len(handles)*2)
			}
			require.NoError(t, dispatchMVIndexKVPairs(
				context.Background(),
				store,
				pairCh,
				handlerChs,
				targetIdx,
			))

			routes := make(map[string][]int, len(handles))
			for handlerIdx, handlerCh := range handlerChs {
				drainClosedKVPairChannel(t, handlerCh, func(pair *simplesst.KVPair) {
					key, err := store.GetCodec().DecodeKey(pair.Key)
					require.NoError(t, err)
					handle, err := tablecodec.DecodeIndexHandle(key, pair.Value, len(targetIdx.Columns))
					require.NoError(t, err)
					handleKey := string(handle.Encoded())
					routes[handleKey] = append(routes[handleKey], handlerIdx)
				})
			}

			require.Len(t, routes, len(handles))
			for _, handle := range handles {
				expectedHandler := int(crc32.ChecksumIEEE(handle.Encoded()) % handlerCount)
				require.Equal(t, []int{expectedHandler, expectedHandler}, routes[string(handle.Encoded())])
			}
		})
	}
}

func TestDispatchMVIndexKVPairsErrorsAndCancellation(t *testing.T) {
	targetIdx := &model.IndexInfo{
		ID:      2,
		MVIndex: true,
		Columns: []*model.IndexColumn{{}},
	}

	t.Run("decode key error", func(t *testing.T) {
		codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 1})
		require.NoError(t, err)
		store := &codecStorage{codec: codecV2}
		pairCh := make(chan *simplesst.KVPair, 1)
		pairCh <- &simplesst.KVPair{Key: []byte("key")}
		close(pairCh)
		handlerChs := []chan *simplesst.KVPair{make(chan *simplesst.KVPair, 1)}

		err = dispatchMVIndexKVPairs(context.Background(), store, pairCh, handlerChs, targetIdx)
		require.Error(t, err)
		requireKVPairChannelClosed(t, handlerChs[0])
	})

	t.Run("decode index handle error", func(t *testing.T) {
		store := &codecStorage{codec: tikv.NewCodecV1(tikv.ModeTxn)}
		pairCh := make(chan *simplesst.KVPair, 1)
		badKey := tablecodec.EncodeIndexSeekKey(1, 2, []byte{0xff})
		pairCh <- &simplesst.KVPair{
			Key:   store.GetCodec().EncodeKey(badKey),
			Value: tablecodec.EncodeHandleInUniqueIndexValue(tidbkv.IntHandle(1), false),
		}
		close(pairCh)
		handlerChs := []chan *simplesst.KVPair{make(chan *simplesst.KVPair, 1)}

		err := dispatchMVIndexKVPairs(context.Background(), store, pairCh, handlerChs, targetIdx)
		require.Error(t, err)
		requireKVPairChannelClosed(t, handlerChs[0])
	})

	t.Run("canceled context", func(t *testing.T) {
		store := &codecStorage{codec: tikv.NewCodecV1(tikv.ModeTxn)}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		handlerChs := []chan *simplesst.KVPair{make(chan *simplesst.KVPair, 1)}

		err := dispatchMVIndexKVPairs(
			ctx,
			store,
			make(chan *simplesst.KVPair),
			handlerChs,
			targetIdx,
		)
		require.ErrorIs(t, err, context.Canceled)
		requireKVPairChannelClosed(t, handlerChs[0])
	})

	t.Run("canceled while sending", func(t *testing.T) {
		pairCount := conflictedkv.BufferedHandleLimit + 1
		decoded := make(chan struct{}, pairCount)
		store := &codecStorage{codec: &notifyingCodec{
			Codec:   tikv.NewCodecV1(tikv.ModeTxn),
			decoded: decoded,
		}}
		pairCh := make(chan *simplesst.KVPair, pairCount)
		for i := range pairCount {
			pairCh <- makeUniqueIndexKVPair(t, store, int64(i), tidbkv.IntHandle(1))
		}
		close(pairCh)
		handlerChs, needDispatch := createConflictHandlerChannels(pairCh, 2, targetIdx)
		require.True(t, needDispatch)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() {
			errCh <- dispatchMVIndexKVPairs(ctx, store, pairCh, handlerChs, targetIdx)
		}()

		for range pairCount {
			select {
			case <-decoded:
			case <-time.After(5 * time.Second):
				t.Fatal("dispatcher did not reach the collector send")
			}
		}
		cancel()
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("dispatcher did not exit after cancellation")
		}
		for _, handlerCh := range handlerChs {
			drainClosedKVPairChannel(t, handlerCh, func(*simplesst.KVPair) {})
		}
	})
}
