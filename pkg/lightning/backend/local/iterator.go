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
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/multierr"
)

// IngestLocalEngineIter abstract iterator method for iterator.
type IngestLocalEngineIter interface {
	common.ForwardIter
	// Last moves this iter to the last key.
	Last() bool
}

type pebbleIter struct {
	*pebble.Iterator
	buf *membuf.Buffer
}

func (p *pebbleIter) ReleaseBuf() {
	p.buf.Reset()
}

func (p *pebbleIter) Close() error {
	// only happens for GetFirstAndLastKey
	if p.buf != nil {
		p.buf.Destroy()
	}
	return p.Iterator.Close()
}

func (p *pebbleIter) Key() []byte {
	// only happens for GetFirstAndLastKey
	if p.buf == nil {
		return p.Iterator.Key()
	}
	return p.buf.AddBytes(p.Iterator.Key())
}

func (p *pebbleIter) Value() []byte {
	return p.buf.AddBytes(p.Iterator.Value())
}

var _ IngestLocalEngineIter = &pebbleIter{}

type dupDetectIter struct {
	keyAdapter  common.KeyAdapter
	iter        *pebble.Iterator
	dupDetector *common.DupDetector
	err         error

	curKey, curVal []byte
	buf            *membuf.Buffer
	logger         log.Logger
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
	// only happens for GetFirstAndLastKey
	if d.buf == nil {
		return d.curKey
	}
	return d.buf.AddBytes(d.curKey)
}

func (d *dupDetectIter) Value() []byte {
	return d.buf.AddBytes(d.curVal)
}

func (d *dupDetectIter) Valid() bool {
	return d.err == nil && d.iter.Valid()
}

func (d *dupDetectIter) Error() error {
	return multierr.Combine(d.iter.Error(), d.err)
}

func (d *dupDetectIter) Close() error {
	// only happens for GetFirstAndLastKey
	if d.buf != nil {
		d.buf.Destroy()
	}
	firstErr := d.dupDetector.Close()
	err := d.iter.Close()
	if firstErr != nil {
		return firstErr
	}
	return err
}

func (d *dupDetectIter) ReleaseBuf() {
	d.buf.Reset()
}

var _ IngestLocalEngineIter = &dupDetectIter{}

func newDupDetectIter(
	db *pebble.DB,
	keyAdapter common.KeyAdapter,
	opts *pebble.IterOptions,
	dupDB *pebble.DB,
	logger log.Logger,
	dupOpt common.DupDetectOpt,
	buf *membuf.Buffer,
) *dupDetectIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, common.MinRowID)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, common.MinRowID)
	}

	detector := common.NewDupDetector(keyAdapter, dupDB.NewBatch(), logger, dupOpt)
	iter, err := db.NewIter(newOpts)
	if err != nil {
		panic("fail to create iterator")
	}
	return &dupDetectIter{
		keyAdapter:  keyAdapter,
		iter:        iter,
		buf:         buf,
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

// Iter describes an iterator.
type Iter interface {
	// Valid check this iter reach the end.
	Valid() bool
	// Next moves this iter forward.
	Next() bool
	// Key returns current position pair's key. The key is accessible after more
	// Next() or Key() invocations but is invalidated by Close() or ReleaseBuf().
	Key() []byte
	// Value returns current position pair's Value. The value is accessible after
	// more Next() or Value() invocations but is invalidated by Close() or
	// ReleaseBuf().
	Value() []byte
	// Close close this iter.
	Close() error
	// Error return current error on this iter.
	Error() error
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
	iter, err := dupDB.NewIter(newOpts)
	if err != nil {
		panic("fail to create iterator")
	}
	return &dupDBIter{
		iter:       iter,
		keyAdapter: keyAdapter,
	}
}
