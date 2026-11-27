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

package kvsource

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// metadataSnapshot retains whole service shards: even a point scan may cost
// as much as reading a shard, so metadata accessors must share the first scan.
// Cached reads remain valid after the context used to load shards is canceled.
type metadataSnapshot struct {
	start  kv.Key
	end    kv.Key
	shards []metadataShard
}

type metadataShard struct {
	start kv.Key
	end   kv.Key
	load  func() ([]metadataPair, error)
}

type metadataPair struct {
	key   kv.Key
	value []byte
}

var _ kv.Snapshot = (*metadataSnapshot)(nil)

// NewMetadataSnapshot caches whole service shards for TiDB metadata readers.
func NewMetadataSnapshot(ctx context.Context, scanner Scanner) (kv.Snapshot, error) {
	start := kv.Key("m")
	end := start.PrefixNext()
	ranges, err := scanner.Shards(ctx, start, end)
	if err != nil {
		return nil, err
	}
	r := &metadataSnapshot{
		start:  start,
		end:    end,
		shards: make([]metadataShard, 0, len(ranges)),
	}
	for _, shard := range ranges {
		rangeToScan := dumpservice.Range{Start: slices.Clone(shard.Start), End: slices.Clone(shard.End)}
		r.shards = append(r.shards, metadataShard{
			start: rangeToScan.Start,
			end:   rangeToScan.End,
			load: sync.OnceValues(func() ([]metadataPair, error) {
				return loadMetadataShard(ctx, scanner, rangeToScan)
			}),
		})
	}
	return r, nil
}

func loadMetadataShard(ctx context.Context, scanner Scanner, shard dumpservice.Range) ([]metadataPair, error) {
	var pairs []metadataPair
	err := scanRange(ctx, scanner, shard.Start, shard.End, func(key, value []byte) error {
		if bytes.Compare(key, shard.Start) < 0 || bytes.Compare(key, shard.End) >= 0 {
			return errors.Errorf("metadata key %x is outside shard [%x, %x)", key, shard.Start, shard.End)
		}
		pairs = append(pairs, metadataPair{key: slices.Clone(key), value: slices.Clone(value)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// The protocol guarantees the KV set, but allows arbitrary scan order.
	slices.SortFunc(pairs, func(a, b metadataPair) int { return bytes.Compare(a.key, b.key) })
	for index := 1; index < len(pairs); index++ {
		if bytes.Equal(pairs[index-1].key, pairs[index].key) {
			return nil, errors.Errorf("duplicate metadata key %x", pairs[index].key)
		}
	}
	return pairs, nil
}

func (r *metadataSnapshot) Get(ctx context.Context, key kv.Key, options ...kv.GetOption) (kv.ValueEntry, error) {
	if len(options) != 0 {
		return kv.ValueEntry{}, errors.New("dump service metadata does not support Get options")
	}
	if err := ctx.Err(); err != nil {
		return kv.ValueEntry{}, err
	}
	if bytes.Compare(key, r.start) < 0 || bytes.Compare(key, r.end) >= 0 {
		return kv.ValueEntry{}, kv.ErrNotExist
	}
	index := sort.Search(len(r.shards), func(i int) bool { return bytes.Compare(r.shards[i].end, key) > 0 })
	if index == len(r.shards) {
		return kv.ValueEntry{}, kv.ErrNotExist
	}
	pairs, err := r.shards[index].load()
	if err != nil {
		return kv.ValueEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		return kv.ValueEntry{}, err
	}
	position, found := slices.BinarySearchFunc(pairs, key, compareMetadataKey)
	if !found {
		return kv.ValueEntry{}, kv.ErrNotExist
	}
	return kv.NewValueEntry(pairs[position].value, 0), nil
}

func (r *metadataSnapshot) BatchGet(
	ctx context.Context, keys []kv.Key, options ...kv.BatchGetOption,
) (map[string]kv.ValueEntry, error) {
	values := make(map[string]kv.ValueEntry, len(keys))
	getOptions := kv.BatchGetToGetOptions(options)
	for _, key := range keys {
		value, err := r.Get(ctx, key, getOptions...)
		if kv.IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[string(key)] = value
	}
	return values, nil
}

// Metadata request-source options have no effect on the service transport.
func (*metadataSnapshot) SetOption(int, any) {}

func compareMetadataKey(pair metadataPair, key kv.Key) int {
	return bytes.Compare(pair.key, key)
}

func (r *metadataSnapshot) Iter(start, end kv.Key) (kv.Iterator, error) {
	return r.iter(start, end, false)
}

func (r *metadataSnapshot) IterReverse(upper, lower kv.Key) (kv.Iterator, error) {
	return r.iter(lower, upper, true)
}

func (r *metadataSnapshot) iter(start, end kv.Key, reverse bool) (kv.Iterator, error) {
	if start == nil || bytes.Compare(start, r.start) < 0 {
		start = r.start
	}
	if end == nil || bytes.Compare(end, r.end) > 0 {
		end = r.end
	}
	if bytes.Compare(start, end) >= 0 {
		return &kv.EmptyIterator{}, nil
	}
	first := sort.Search(len(r.shards), func(i int) bool { return bytes.Compare(r.shards[i].end, start) > 0 })
	var pairs []metadataPair
	for index := first; index < len(r.shards) && bytes.Compare(r.shards[index].start, end) < 0; index++ {
		shardPairs, err := r.shards[index].load()
		if err != nil {
			return nil, err
		}
		left, _ := slices.BinarySearchFunc(shardPairs, start, compareMetadataKey)
		right, _ := slices.BinarySearchFunc(shardPairs, end, compareMetadataKey)
		pairs = append(pairs, shardPairs[left:right]...)
	}
	iter := &metadataIter{pairs: pairs, step: 1}
	if reverse {
		iter.position = len(pairs) - 1
		iter.step = -1
	}
	return iter, nil
}

type metadataIter struct {
	pairs    []metadataPair
	position int
	step     int
}

func (i *metadataIter) Valid() bool   { return i.position >= 0 && i.position < len(i.pairs) }
func (i *metadataIter) Key() kv.Key   { return i.pairs[i.position].key }
func (i *metadataIter) Value() []byte { return i.pairs[i.position].value }

func (i *metadataIter) Next() error {
	if !i.Valid() {
		return errors.New("metadata iterator is invalid")
	}
	i.position += i.step
	return nil
}

func (i *metadataIter) Close() { i.pairs = nil }

// metadataAllocators supplies only the allocators consulted by SHOW CREATE
// TABLE. Base decoding uses the same metadata accessors as the live allocator.
func metadataAllocators(reader tidbmeta.Reader, databaseID int64, table *model.TableInfo) (autoid.Allocators, error) {
	allocators := autoid.NewAllocators(table.SepAutoInc())
	accessors := reader.GetAutoIDAccessors(databaseID, table.ID)
	if table.GetAutoIncrementColInfo() != nil {
		allocType := autoid.RowIDAllocType
		accessor := accessors.RowID()
		if table.SepAutoInc() {
			allocType = autoid.AutoIncrementType
			accessor = accessors.IncrementID(table.Version)
		}
		base, err := accessor.Get()
		if err != nil {
			return autoid.Allocators{}, err
		}
		allocators = allocators.Append(&metadataAllocator{
			base: base, allocType: allocType, unsigned: table.IsAutoIncColUnsigned(),
		})
	}
	if table.ContainsAutoRandomBits() {
		base, err := accessors.RandomID().Get()
		if err != nil {
			return autoid.Allocators{}, err
		}
		allocators = allocators.Append(&metadataAllocator{
			base: base, allocType: autoid.AutoRandomType, unsigned: table.IsAutoRandomBitColUnsigned(),
		})
	}
	return allocators, nil
}

type metadataAllocator struct {
	base      int64
	allocType autoid.AllocatorType
	unsigned  bool
}

var _ autoid.Allocator = (*metadataAllocator)(nil)

func (a *metadataAllocator) Base() int64                   { return a.base }
func (a *metadataAllocator) End() int64                    { return a.base }
func (a *metadataAllocator) GetType() autoid.AllocatorType { return a.allocType }

func (a *metadataAllocator) NextGlobalAutoID() (int64, error) {
	if a.unsigned {
		return int64(uint64(a.base) + 1), nil
	}
	return a.base + 1, nil
}

var errMetadataAllocatorReadOnly = errors.New("dump service metadata allocator is read-only")

func (*metadataAllocator) Alloc(context.Context, uint64, int64, int64) (minv, maxv int64, err error) {
	return 0, 0, errMetadataAllocatorReadOnly
}

func (*metadataAllocator) AllocSeqCache() (minv, maxv, round int64, err error) {
	return 0, 0, 0, errMetadataAllocatorReadOnly
}

func (*metadataAllocator) Rebase(context.Context, int64, bool) error {
	return errMetadataAllocatorReadOnly
}

func (*metadataAllocator) ForceRebase(int64) error {
	return errMetadataAllocatorReadOnly
}

func (*metadataAllocator) RebaseSeq(int64) (int64, bool, error) {
	return 0, false, errMetadataAllocatorReadOnly
}

func (*metadataAllocator) Transfer(int64, int64) error {
	return errMetadataAllocatorReadOnly
}
