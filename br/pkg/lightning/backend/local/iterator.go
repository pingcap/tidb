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
	"github.com/cockroachdb/pebble"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/multierr"
)

// Iter abstract iterator method for Ingester.
type Iter interface {
	common.ForwardIter
	// Seek seek to specify position.
	// if key not found, seeks next key position in iter.
	Seek(key []byte) bool
	// Last moves this iter to the last key.
	Last() bool
	// OpType represents operations of pair. currently we have two types.
	// 1. Put
	// 2. Delete
	OpType() sst.Pair_OP
}

type pebbleIter struct {
	*pebble.Iterator
}

func (p pebbleIter) Seek(key []byte) bool {
	return p.SeekGE(key)
}

func (pebbleIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ Iter = pebbleIter{}

type dupDetectIter struct {
	keyAdapter  common.KeyAdapter
	iter        *pebble.Iterator
	dupDetector *common.DupDetector
	err         error

	curKey, curVal []byte
	logger         log.Logger
}

func (d *dupDetectIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, common.ZeroRowID)
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
	d.curKey, d.curVal, d.err = d.dupDetector.Init(d.iter)
}

func (d *dupDetectIter) Next() bool {
	if d.err != nil {
		return false
	}
	key, val, ok, err := d.dupDetector.Next(d.iter)
	if err != nil {
		d.err = err
		return false
	}
	if !ok {
		return false
	}
	d.curKey, d.curVal = key, val
	return true
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
	firstErr := d.dupDetector.Close()
	err := d.iter.Close()
	if firstErr != nil {
		return firstErr
	}
	return err
}

func (*dupDetectIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ Iter = &dupDetectIter{}

func newDupDetectIter(
	db *pebble.DB,
	keyAdapter common.KeyAdapter,
	opts *pebble.IterOptions,
	dupDB *pebble.DB,
	logger log.Logger,
	dupOpt common.DupDetectOpt,
) *dupDetectIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, common.MinRowID)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, common.MinRowID)
	}

	detector := common.NewDupDetector(keyAdapter, dupDB.NewBatch(), logger, dupOpt)
	return &dupDetectIter{
		keyAdapter:  keyAdapter,
		iter:        db.NewIter(newOpts),
		dupDetector: detector,
		logger:      logger,
	}
}

type dupDBIter struct {
	iter       *pebble.Iterator
	keyAdapter common.KeyAdapter
	curKey     []byte
	err        error
}

func (d *dupDBIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, common.ZeroRowID)
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

func (*dupDBIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

var _ Iter = &dupDBIter{}

func newDupDBIter(dupDB *pebble.DB, keyAdapter common.KeyAdapter, opts *pebble.IterOptions) *dupDBIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, common.MinRowID)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, common.MinRowID)
	}
	return &dupDBIter{
		iter:       dupDB.NewIter(newOpts),
		keyAdapter: keyAdapter,
	}
}
