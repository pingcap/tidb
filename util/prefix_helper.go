// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package util

import (
	"bytes"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

// ScanMetaWithPrefix scans metadata with the prefix.
func ScanMetaWithPrefix(txn kv.Transaction, prefix string, filter func([]byte, []byte) bool) error {
	iter, err := txn.Seek([]byte(prefix))
	if err != nil {
		return errors.Trace(err)
	}
	defer iter.Close()

	for {
		if err != nil {
			return errors.Trace(err)
		}

		if iter.Valid() && strings.HasPrefix(iter.Key(), prefix) {
			if !filter([]byte(iter.Key()), iter.Value()) {
				break
			}
			err = iter.Next()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			break
		}
	}

	return nil
}

// DelKeyWithPrefix deletes keys with prefix.
func DelKeyWithPrefix(ctx context.Context, prefix string) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}

	var keys []string
	iter, err := txn.Seek([]byte(prefix))
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for {
		if err != nil {
			return errors.Trace(err)
		}

		if iter.Valid() && strings.HasPrefix(iter.Key(), prefix) {
			keys = append(keys, iter.Key())
			err = iter.Next()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			break
		}
	}

	for _, key := range keys {
		err := txn.Delete([]byte(key))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// EncodeRecordKey encodes the string value to a byte slice.
func EncodeRecordKey(tablePrefix string, h int64, columnID int64) []byte {
	var (
		buf []byte
		err error
	)

	if columnID == 0 { // Ignore columnID.
		buf, err = kv.EncodeValue(tablePrefix, h)
	} else {
		buf, err = kv.EncodeValue(tablePrefix, h, columnID)
	}
	if err != nil {
		log.Fatal("should never happend")
	}
	return buf
}

// DecodeHandleFromRowKey decodes the string form a row key and returns an int64.
func DecodeHandleFromRowKey(rk string) (int64, error) {
	vals, err := kv.DecodeValue([]byte(rk))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return vals[1].(int64), nil
}

// RowKeyPrefixFilter returns a function which checks whether currentKey has decoded rowKeyPrefix as prefix.
func RowKeyPrefixFilter(rowKeyPrefix []byte) kv.FnKeyCmp {
	return func(currentKey kv.Key) bool {
		// Next until key without prefix of this record.
		raw, err := codec.StripEnd(rowKeyPrefix)
		if err != nil {
			return false
		}
		return !bytes.HasPrefix(currentKey, raw)
	}
}
