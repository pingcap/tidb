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

	"github.com/cockroachdb/pebble"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// Iter abstract iterator method for Ingester.
type Iter interface {
	ForwardIter
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

const maxDuplicateBatchSize = 4 << 20

// DupDetector extract the decoded key and value from the iter which may contain
// duplicate keys and store the keys encoded by KeyAdapter. The duplicate keys
// and values will be saved in dupDB.
type DupDetector struct {
	keyAdapter      KeyAdapter
	dupDBWriteBatch *pebble.Batch
	curBatchSize    int

	curKey    []byte
	curRawKey []byte
	curVal    []byte
	nextKey   []byte

	logger log.Logger
	option DupDetectOpt
}

// NewDupDetector creates a new DupDetector.
// dupDBWriteBatch will be closed when DupDetector is closed.
func NewDupDetector(
	keyAdaptor KeyAdapter,
	dupDBWriteBatch *pebble.Batch,
	logger log.Logger,
	option DupDetectOpt,
) *DupDetector {
	return &DupDetector{
		keyAdapter:      keyAdaptor,
		dupDBWriteBatch: dupDBWriteBatch,
		logger:          logger,
		option:          option,
	}
}

// KVIter is a slim interface that DupDetector needs.
type KVIter interface {
	Next() bool
	Key() []byte
	Value() []byte
}

// Init initializes the status of DupDetector by reading the current Key and
// Value of given iter.
func (d *DupDetector) Init(iter KVIter) (key []byte, val []byte, err error) {
	d.curKey, err = d.keyAdapter.Decode(d.curKey[:0], iter.Key())
	if err != nil {
		return nil, nil, err
	}
	d.curRawKey = append(d.curRawKey[:0], iter.Key()...)
	d.curVal = append(d.curVal[:0], iter.Value()...)
	return d.curKey, d.curVal, nil
}

// Next reads the next key and value from given iter. If it meets duplicate key,
// it will record the duplicate key and value in dupDB and skip it.
func (d *DupDetector) Next(iter KVIter) (key []byte, value []byte, ok bool, err error) {
	recordFirst := false
	for iter.Next() {
		encodedKey, val := iter.Key(), iter.Value()
		d.nextKey, err = d.keyAdapter.Decode(d.nextKey[:0], encodedKey)
		if err != nil {
			return nil, nil, false, err
		}
		if !bytes.Equal(d.nextKey, d.curKey) {
			d.curKey, d.nextKey = d.nextKey, d.curKey[:0]
			d.curRawKey = append(d.curRawKey[:0], encodedKey...)
			d.curVal = append(d.curVal[:0], val...)
			return d.curKey, d.curVal, true, nil
		}
		if d.option.ReportErrOnDup {
			dupKey := make([]byte, len(d.curKey))
			dupVal := make([]byte, len(val))
			copy(dupKey, d.curKey)
			copy(dupVal, d.curVal)
			return nil, nil, false, common.ErrFoundDuplicateKeys.FastGenByArgs(dupKey, dupVal)
		}
		if !recordFirst {
			if err = d.record(d.curRawKey, d.curKey, d.curVal); err != nil {
				return nil, nil, false, err
			}
			recordFirst = true
		}
		if err = d.record(encodedKey, d.nextKey, val); err != nil {
			return nil, nil, false, err
		}
	}
	return nil, nil, false, nil
}

func (d *DupDetector) record(rawKey, key, val []byte) error {
	d.logger.Debug("local duplicate key detected", zap.String("category", "detect-dupe"),
		logutil.Key("key", key),
		logutil.Key("value", val),
		logutil.Key("rawKey", rawKey))
	if err := d.dupDBWriteBatch.Set(rawKey, val, nil); err != nil {
		return err
	}
	d.curBatchSize += len(rawKey) + len(val)
	if d.curBatchSize >= maxDuplicateBatchSize {
		return d.flush()
	}
	return nil
}

func (d *DupDetector) flush() error {
	if err := d.dupDBWriteBatch.Commit(pebble.Sync); err != nil {
		return err
	}
	d.dupDBWriteBatch.Reset()
	d.curBatchSize = 0
	return nil
}

// Close closes the DupDetector.
func (d *DupDetector) Close() error {
	if err := d.flush(); err != nil {
		return err
	}
	return d.dupDBWriteBatch.Close()
}

type dupDetectIter struct {
	keyAdapter  KeyAdapter
	iter        *pebble.Iterator
	dupDetector *DupDetector
	err         error

	curKey, curVal []byte
	logger         log.Logger
}

// DupDetectOpt is the option for duplicate detection.
type DupDetectOpt struct {
	ReportErrOnDup bool
}

func (d *dupDetectIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, ZeroRowID)
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
	keyAdapter KeyAdapter,
	opts *pebble.IterOptions,
	dupDB *pebble.DB,
	logger log.Logger,
	dupOpt DupDetectOpt,
) *dupDetectIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, MinRowID)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, MinRowID)
	}

	detector := NewDupDetector(keyAdapter, dupDB.NewBatch(), logger, dupOpt)
	return &dupDetectIter{
		keyAdapter:  keyAdapter,
		iter:        db.NewIter(newOpts),
		dupDetector: detector,
		logger:      logger,
	}
}

type dupDBIter struct {
	iter       *pebble.Iterator
	keyAdapter KeyAdapter
	curKey     []byte
	err        error
}

func (d *dupDBIter) Seek(key []byte) bool {
	rawKey := d.keyAdapter.Encode(nil, key, ZeroRowID)
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

func newDupDBIter(dupDB *pebble.DB, keyAdapter KeyAdapter, opts *pebble.IterOptions) *dupDBIter {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = keyAdapter.Encode(nil, opts.LowerBound, MinRowID)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = keyAdapter.Encode(nil, opts.UpperBound, MinRowID)
	}
	return &dupDBIter{
		iter:       dupDB.NewIter(newOpts),
		keyAdapter: keyAdapter,
	}
}
