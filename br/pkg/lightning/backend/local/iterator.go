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

	"github.com/cockroachdb/pebble"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/util/codec"
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

type duplicateIter struct {
	ctx       context.Context
	iter      *pebble.Iterator
	curKey    []byte
	curRawKey []byte
	curVal    []byte
	nextKey   []byte
	err       error

	engine         *Engine
	keyAdapter     KeyAdapter
	writeBatch     *pebble.Batch
	writeBatchSize int64
	logger         log.Logger
}

func (d *duplicateIter) Seek(key []byte) bool {
	encodedKey := d.keyAdapter.Encode(nil, key, 0, 0)
	if d.err != nil || !d.iter.SeekGE(encodedKey) {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *duplicateIter) First() bool {
	if d.err != nil || !d.iter.First() {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *duplicateIter) Last() bool {
	if d.err != nil || !d.iter.Last() {
		return false
	}
	d.fill()
	return d.err == nil
}

func (d *duplicateIter) fill() {
	d.curKey, _, _, d.err = d.keyAdapter.Decode(d.curKey[:0], d.iter.Key())
	d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
	d.curVal = append(d.curVal[:0], d.iter.Value()...)
}

func (d *duplicateIter) flush() {
	d.err = d.writeBatch.Commit(pebble.Sync)
	d.writeBatch.Reset()
	d.writeBatchSize = 0
}

func (d *duplicateIter) record(key []byte, val []byte) {
	d.engine.Duplicates.Inc()
	d.err = d.writeBatch.Set(key, val, nil)
	if d.err != nil {
		return
	}
	d.writeBatchSize += int64(len(key) + len(val))
	if d.writeBatchSize >= maxDuplicateBatchSize {
		d.flush()
	}
}

func (d *duplicateIter) Next() bool {
	recordFirst := false
	for d.err == nil && d.ctx.Err() == nil && d.iter.Next() {
		d.nextKey, _, _, d.err = d.keyAdapter.Decode(d.nextKey[:0], d.iter.Key())
		if d.err != nil {
			return false
		}
		if !bytes.Equal(d.nextKey, d.curKey) {
			d.curKey, d.nextKey = d.nextKey, d.curKey[:0]
			d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
			d.curVal = append(d.curVal[:0], d.iter.Value()...)
			return true
		}
		d.logger.Debug("[detect-dupe] local duplicate key detected",
			logutil.Key("key", d.curKey),
			logutil.Key("prevValue", d.curVal),
			logutil.Key("value", d.iter.Value()))
		if !recordFirst {
			d.record(d.curRawKey, d.curVal)
			recordFirst = true
		}
		d.record(d.iter.Key(), d.iter.Value())
	}
	if d.err == nil {
		d.err = d.ctx.Err()
	}
	return false
}

func (d *duplicateIter) Key() []byte {
	return d.curKey
}

func (d *duplicateIter) Value() []byte {
	return d.curVal
}

func (d *duplicateIter) Valid() bool {
	return d.err == nil && d.iter.Valid()
}

func (d *duplicateIter) Error() error {
	return multierr.Combine(d.iter.Error(), d.err)
}

func (d *duplicateIter) Close() error {
	if d.err == nil {
		d.flush()
	}
	d.writeBatch.Close()
	return d.iter.Close()
}

func (d *duplicateIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ kv.Iter = &duplicateIter{}

func newDuplicateIter(ctx context.Context, engine *Engine, opts *pebble.IterOptions) kv.Iter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = codec.EncodeBytes(nil, opts.LowerBound)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = codec.EncodeBytes(nil, opts.UpperBound)
	}
	logger := log.With(
		zap.String("table", common.UniqueTable(engine.tableInfo.DB, engine.tableInfo.Name)),
		zap.Int64("tableID", engine.tableInfo.ID),
		zap.Stringer("engineUUID", engine.UUID))
	return &duplicateIter{
		ctx:        ctx,
		iter:       engine.db.NewIter(newOpts),
		engine:     engine,
		keyAdapter: engine.keyAdapter,
		writeBatch: engine.duplicateDB.NewBatch(),
		logger:     logger,
	}
}

func newKVIter(ctx context.Context, engine *Engine, opts *pebble.IterOptions) kv.Iter {
	if bytes.Compare(opts.LowerBound, normalIterStartKey) < 0 {
		newOpts := *opts
		newOpts.LowerBound = normalIterStartKey
		opts = &newOpts
	}
	if !engine.duplicateDetection {
		return pebbleIter{Iterator: engine.db.NewIter(opts)}
	}
	return newDuplicateIter(ctx, engine, opts)
}
