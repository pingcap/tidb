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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/pebble"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/multierr"
)

type pebbleIter struct {
	*pebble.Iterator
}

func (p pebbleIter) Seek(key []byte) bool {
	return p.SeekGE(key)
}

func (p pebbleIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ kv.Iter = pebbleIter{}

const maxDuplicateBatchSize = 4 << 20

type dupDetectIter struct {
	ctx       context.Context
	iter      *pebble.Iterator
	curKey    []byte
	curRawKey []byte
	curVal    []byte
	nextKey   []byte
	err       error

	keyAdapter     KeyAdapter
	writeBatch     *pebble.Batch
	writeBatchSize int64
	logger         log.Logger
}

func (d *dupDetectIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, 0)
	if d.err != nil || !d.iter.SeekGE(rawKey) {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *dupDetectIter) First() bool {
	if d.err != nil || !d.iter.First() {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *dupDetectIter) Last() bool {
	if d.err != nil || !d.iter.Last() {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *dupDetectIter) fill() {
	d.curKey, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
	d.curVal = append(d.curVal[:0], d.iter.Value()...)
}

func (d *dupDetectIter) flush() {
	d.err = d.writeBatch.Commit(pebble.Sync)
	d.writeBatch.Reset()
	d.writeBatchSize = 0
}

func (d *dupDetectIter) record(rawKey, key, val []byte) {
	d.logger.Debug("[detect-dupe] local duplicate key detected",
		logutil.Key("key", key),
		logutil.Key("value", val),
		logutil.Key("rawKey", rawKey))
	d.err = d.writeBatch.Set(rawKey, val, nil)
	if d.err != nil {
		return
	}
	d.writeBatchSize += int64(len(rawKey) + len(val))
	if d.writeBatchSize >= maxDuplicateBatchSize {
		d.flush()
	}
}

func (d *dupDetectIter) Next() bool {
	recordFirst := false
	for d.err == nil && d.ctx.Err() == nil && d.iter.Next() {
		d.nextKey, d.err = d.keyAdapter.Decode(d.nextKey[:0], d.iter.Key())
		if d.err != nil {
			return false
		}
		if !bytes.Equal(d.nextKey, d.curKey) {
			d.curKey, d.nextKey = d.nextKey, d.curKey[:0]
			d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
			d.curVal = append(d.curVal[:0], d.iter.Value()...)
			return true
		}
		if !recordFirst {
			d.record(d.curRawKey, d.curKey, d.curVal)
			recordFirst = true
		}
		d.record(d.iter.Key(), d.nextKey, d.iter.Value())
	}
	if d.err == nil {
		d.err = d.ctx.Err()
	}
	return false
}

func (d *dupDetectIter) Key() []byte {
	return d.curKey
}

func (d *dupDetectIter) Value() []byte {
	return d.curVal
}

func (d *dupDetectIter) Valid() bool {
	return d.err == nil && d.iter.Valid()
}

func (d *dupDetectIter) Error() error {
	return multierr.Combine(d.iter.Error(), d.err)
}

func (d *dupDetectIter) Close() error {
	if d.err == nil {
		d.flush()
	}
	d.writeBatch.Close()
	return d.iter.Close()
}

func (d *dupDetectIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ kv.Iter = &dupDetectIter{}

func newDupDetectIter(ctx context.Context, db *pebble.DB, keyAdapter KeyAdapter,
	opts *pebble.IterOptions, dupDB *pebble.DB, logger log.Logger) *dupDetectIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, math.MinInt64)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, math.MinInt64)
	}
	return &dupDetectIter{
		ctx:        ctx,
		iter:       db.NewIter(newOpts),
		keyAdapter: keyAdapter,
		writeBatch: dupDB.NewBatch(),
		logger:     logger,
	}
}

type dupDBIter struct {
	iter       *pebble.Iterator
	keyAdapter KeyAdapter
	curKey     []byte
	err        error
}

func (d *dupDBIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, 0)
	if d.err != nil || !d.iter.SeekGE(rawKey) {
		return false
	}
	d.curKey, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	return d.err == nil
}

func (d *dupDBIter) Error() error {
	if d.err != nil {
		return d.err
	}
	return d.iter.Error()
}

func (d *dupDBIter) First() bool {
	if d.err != nil || !d.iter.First() {
		return false
	}
	d.curKey, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	return d.err == nil
}

func (d *dupDBIter) Last() bool {
	if d.err != nil || !d.iter.Last() {
		return false
	}
	d.curKey, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	return d.err == nil
}

func (d *dupDBIter) Valid() bool {
	return d.err == nil && d.iter.Valid()
}

func (d *dupDBIter) Next() bool {
	if d.err != nil || !d.iter.Next() {
		return false
	}
	d.curKey, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	return d.err == nil
}

func (d *dupDBIter) Key() []byte {
	return d.curKey
}

func (d *dupDBIter) Value() []byte {
	return d.iter.Value()
}

func (d *dupDBIter) Close() error {
	return d.iter.Close()
}

func (d *dupDBIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ kv.Iter = &dupDBIter{}

func newDupDBIter(dupDB *pebble.DB, keyAdapter KeyAdapter, opts *pebble.IterOptions) *dupDBIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, math.MinInt64)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, math.MinInt64)
	}
	return &dupDBIter{
		iter:       dupDB.NewIter(newOpts),
		keyAdapter: keyAdapter,
	}
}
