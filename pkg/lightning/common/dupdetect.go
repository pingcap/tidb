// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

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
			dupVal := make([]byte, len(d.curVal))
			copy(dupKey, d.curKey)
			copy(dupVal, d.curVal)
			return nil, nil, false, ErrFoundDuplicateKeys.FastGenByArgs(dupKey, dupVal)
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

// DupDetectOpt is the option for duplicate detection.
type DupDetectOpt struct {
	ReportErrOnDup bool
}
