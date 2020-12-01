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
// See the License for the specific language governing permissions and
// limitations under the License.

package unistore

import (
	"bytes"
	"context"
	"sync"

	"github.com/ngaut/unistore/lockstore"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type rawHandler struct {
	mu    sync.RWMutex
	store *lockstore.MemStore
}

func newRawHandler() *rawHandler {
	return &rawHandler{
		store: lockstore.NewMemStore(4096),
	}
}

func (h *rawHandler) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	val := h.store.Get(req.Key, nil)
	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: len(val) == 0,
	}, nil
}

func (h *rawHandler) RawBatchGet(_ context.Context, req *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	pairs := make([]*kvrpcpb.KvPair, len(req.Keys))
	for i, key := range req.Keys {
		pairs[i] = &kvrpcpb.KvPair{
			Key:   key,
			Value: h.store.Get(key, nil),
		}
	}
	return &kvrpcpb.RawBatchGetResponse{Pairs: pairs}, nil
}

func (h *rawHandler) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.store.Put(req.Key, req.Value)
	return &kvrpcpb.RawPutResponse{}, nil
}

func (h *rawHandler) RawBatchPut(_ context.Context, req *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, pair := range req.Pairs {
		h.store.Put(pair.Key, pair.Value)
	}
	return &kvrpcpb.RawBatchPutResponse{}, nil
}

func (h *rawHandler) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.store.Delete(req.Key)
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (h *rawHandler) RawBatchDelete(_ context.Context, req *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, key := range req.Keys {
		h.store.Delete(key)
	}
	return &kvrpcpb.RawBatchDeleteResponse{}, nil
}

func (h *rawHandler) RawDeleteRange(_ context.Context, req *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	it := h.store.NewIterator()
	var keys [][]byte
	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), req.EndKey) >= 0 {
			break
		}
		keys = append(keys, safeCopy(it.Key()))
	}
	for _, key := range keys {
		h.store.Delete(key)
	}
	return &kvrpcpb.RawDeleteRangeResponse{}, nil
}

func (h *rawHandler) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	it := h.store.NewIterator()
	var pairs []*kvrpcpb.KvPair
	if !req.Reverse {
		for it.Seek(req.StartKey); it.Valid(); it.Next() {
			if len(pairs) >= int(req.Limit) {
				break
			}
			if len(req.EndKey) > 0 && bytes.Compare(it.Key(), req.EndKey) >= 0 {
				break
			}
			pairs = h.appendPair(pairs, it)
		}
	} else {
		for it.SeekForPrev(req.StartKey); it.Valid(); it.Prev() {
			if bytes.Equal(it.Key(), req.StartKey) {
				continue
			}
			if len(pairs) >= int(req.Limit) {
				break
			}
			if bytes.Compare(it.Key(), req.EndKey) < 0 {
				break
			}
			pairs = h.appendPair(pairs, it)
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: pairs}, nil
}

func (h *rawHandler) appendPair(pairs []*kvrpcpb.KvPair, it *lockstore.Iterator) []*kvrpcpb.KvPair {
	pair := &kvrpcpb.KvPair{
		Key:   safeCopy(it.Key()),
		Value: safeCopy(it.Value()),
	}
	return append(pairs, pair)
}

func safeCopy(val []byte) []byte {
	return append([]byte{}, val...)
}
