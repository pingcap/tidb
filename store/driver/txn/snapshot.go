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
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"
)

type tikvSnapshot struct {
	*tikv.KVSnapshot
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	data, err := s.KVSnapshot.BatchGet(ctx, keys)
	return data, extractUnExtractKeyErr(err)
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	data, err := s.KVSnapshot.Get(ctx, k)
	return data, extractUnExtractKeyErr(err)
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.Iter(k, upperBound)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.IterReverse(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
}

func extractUnExtractKeyErr(err error) error {
	if err == nil {
		return nil
	}
	e, ok := errors.Cause(err).(*tikvstore.UnExtractKeyErr)
	if !ok {
		return errors.Trace(err)
	}

	if e.Conflict != nil {
		return newWriteConflictError(e.Conflict)
	}
	if e.Retryable != "" {
		notFoundDetail := prettyLockNotFoundKey(e.GetRetryable())
		return kv.ErrTxnRetryable.GenWithStackByArgs(e.GetRetryable() + " " + notFoundDetail)
	}
	// unexpected error
	return e
}

func newWriteConflictError(conflict *kvrpcpb.WriteConflict) error {
	var buf bytes.Buffer
	prettyWriteKey(&buf, conflict.Key)
	buf.WriteString(" primary=")
	prettyWriteKey(&buf, conflict.Primary)
	return kv.ErrWriteConflict.FastGenByArgs(conflict.StartTs, conflict.ConflictTs, conflict.ConflictCommitTs, buf.String())
}

func prettyWriteKey(buf *bytes.Buffer, key []byte) {
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		_, err1 := fmt.Fprintf(buf, "{tableID=%d, indexID=%d, indexValues={", tableID, indexID)
		if err1 != nil {
			logutil.BgLogger().Error("error", zap.Error(err1))
		}
		for _, v := range indexValues {
			_, err2 := fmt.Fprintf(buf, "%s, ", v)
			if err2 != nil {
				logutil.BgLogger().Error("error", zap.Error(err2))
			}
		}
		buf.WriteString("}}")
		return
	}

	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{tableID=%d, handle=%d}", tableID, handle)
		if err3 != nil {
			logutil.BgLogger().Error("error", zap.Error(err3))
		}
		return
	}

	mKey, mField, err := tablecodec.DecodeMetaKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{metaKey=true, key=%s, field=%s}", string(mKey), string(mField))
		if err3 != nil {
			logutil.Logger(context.Background()).Error("error", zap.Error(err3))
		}
		return
	}

	_, err4 := fmt.Fprintf(buf, "%#v", key)
	if err4 != nil {
		logutil.BgLogger().Error("error", zap.Error(err4))
	}
}

func prettyLockNotFoundKey(rawRetry string) string {
	if !strings.Contains(rawRetry, "TxnLockNotFound") {
		return ""
	}
	start := strings.Index(rawRetry, "[")
	if start == -1 {
		return ""
	}
	rawRetry = rawRetry[start:]
	end := strings.Index(rawRetry, "]")
	if end == -1 {
		return ""
	}
	rawRetry = rawRetry[:end+1]
	var key []byte
	err := json.Unmarshal([]byte(rawRetry), &key)
	if err != nil {
		return ""
	}
	var buf bytes.Buffer
	prettyWriteKey(&buf, key)
	return buf.String()
}
