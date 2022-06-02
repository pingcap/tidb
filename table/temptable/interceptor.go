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

package temptable

import (
	"bytes"
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/tablecodec"
	"golang.org/x/exp/maps"
)

var (
	tableWithIDPrefixLen = len(tablecodec.EncodeTablePrefix(1))
)

// TemporaryTableSnapshotInterceptor implements kv.SnapshotInterceptor
type TemporaryTableSnapshotInterceptor struct {
	is          infoschema.InfoSchema
	sessionData kv.Retriever
}

// SessionSnapshotInterceptor creates a new snapshot interceptor for temporary table data fetch
func SessionSnapshotInterceptor(sctx sessionctx.Context) kv.SnapshotInterceptor {
	return NewTemporaryTableSnapshotInterceptor(
		sctx.GetInfoSchema().(infoschema.InfoSchema),
		getSessionData(sctx),
	)
}

// NewTemporaryTableSnapshotInterceptor creates a new TemporaryTableSnapshotInterceptor
func NewTemporaryTableSnapshotInterceptor(is infoschema.InfoSchema, sessionData kv.Retriever) *TemporaryTableSnapshotInterceptor {
	return &TemporaryTableSnapshotInterceptor{
		is:          is,
		sessionData: sessionData,
	}
}

// OnGet intercepts Get operation for Snapshot
func (i *TemporaryTableSnapshotInterceptor) OnGet(ctx context.Context, snap kv.Snapshot, k kv.Key) ([]byte, error) {
	if tblID, ok := getKeyAccessedTableID(k); ok {
		if tblInfo, ok := i.temporaryTableInfoByID(tblID); ok {
			return getSessionKey(ctx, tblInfo, i.sessionData, k)
		}
	}

	return snap.Get(ctx, k)
}

func getSessionKey(ctx context.Context, tblInfo *model.TableInfo, sessionData kv.Retriever, k kv.Key) ([]byte, error) {
	if tblInfo.TempTableType == model.TempTableNone {
		return nil, errors.New("Cannot get normal table key from session")
	}

	if sessionData == nil || tblInfo.TempTableType == model.TempTableGlobal {
		return nil, kv.ErrNotExist
	}

	val, err := sessionData.Get(ctx, k)
	if err == nil && len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, err
}

// OnBatchGet intercepts BatchGet operation for Snapshot
func (i *TemporaryTableSnapshotInterceptor) OnBatchGet(ctx context.Context, snap kv.Snapshot, keys []kv.Key) (map[string][]byte, error) {
	keys, result, err := i.batchGetTemporaryTableKeys(ctx, keys)
	if err != nil {
		return nil, err
	}

	if len(keys) > 0 {
		snapResult, err := snap.BatchGet(ctx, keys)
		if err != nil {
			return nil, err
		}

		if len(snapResult) > 0 {
			maps.Copy(snapResult, result)
			result = snapResult
		}
	}

	if result == nil {
		result = make(map[string][]byte)
	}
	return result, nil
}

func (i *TemporaryTableSnapshotInterceptor) batchGetTemporaryTableKeys(ctx context.Context, keys []kv.Key) (snapKeys []kv.Key, result map[string][]byte, err error) {
	for _, k := range keys {
		tblID, ok := getKeyAccessedTableID(k)
		if !ok {
			snapKeys = append(snapKeys, k)
			continue
		}

		tblInfo, ok := i.temporaryTableInfoByID(tblID)
		if !ok {
			snapKeys = append(snapKeys, k)
			continue
		}

		val, err := getSessionKey(ctx, tblInfo, i.sessionData, k)
		if kv.ErrNotExist.Equal(err) {
			continue
		}

		if err != nil {
			return nil, nil, err
		}

		if result == nil {
			result = make(map[string][]byte)
		}

		result[string(k)] = val
	}

	return snapKeys, result, err
}

// OnIter intercepts Iter operation for Snapshot
func (i *TemporaryTableSnapshotInterceptor) OnIter(snap kv.Snapshot, k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	if notTableRange(k, upperBound) {
		return snap.Iter(k, upperBound)
	}

	if tblID, ok := getRangeAccessedTableID(k, upperBound); ok {
		return i.iterTable(tblID, snap, k, upperBound)
	}

	return createUnionIter(i.sessionData, snap, k, upperBound, false)
}

// OnIterReverse intercepts IterReverse operation for Snapshot
func (i *TemporaryTableSnapshotInterceptor) OnIterReverse(snap kv.Snapshot, k kv.Key) (kv.Iterator, error) {
	if notTableRange(nil, k) {
		// scan range has no intersect with table data
		return snap.IterReverse(k)
	}

	// because lower bound is nil, so the range cannot be located in one table
	return createUnionIter(i.sessionData, snap, nil, k, true)
}

func (i *TemporaryTableSnapshotInterceptor) iterTable(tblID int64, snap kv.Snapshot, k, upperBound kv.Key) (kv.Iterator, error) {
	tblInfo, ok := i.temporaryTableInfoByID(tblID)
	if !ok {
		return snap.Iter(k, upperBound)
	}

	if tblInfo.TempTableType == model.TempTableGlobal || i.sessionData == nil {
		return &kv.EmptyIterator{}, nil
	}

	// still need union iter to filter out empty value in session data
	return createUnionIter(i.sessionData, nil, k, upperBound, false)
}

func (i *TemporaryTableSnapshotInterceptor) temporaryTableInfoByID(tblID int64) (*model.TableInfo, bool) {
	if tbl, ok := i.is.TableByID(tblID); ok {
		tblInfo := tbl.Meta()
		if tblInfo.TempTableType != model.TempTableNone {
			return tblInfo, true
		}
	}

	return nil, false
}

func createUnionIter(sessionData kv.Retriever, snap kv.Snapshot, k, upperBound kv.Key, reverse bool) (iter kv.Iterator, err error) {
	if reverse && k != nil {
		return nil, errors.New("k should be nil for iter reverse")
	}

	var snapIter kv.Iterator
	if snap == nil {
		snapIter = &kv.EmptyIterator{}
	} else {
		if reverse {
			snapIter, err = snap.IterReverse(upperBound)
		} else {
			snapIter, err = snap.Iter(k, upperBound)
		}
	}

	if err != nil {
		return nil, err
	}

	if sessionData == nil {
		return snapIter, nil
	}

	var sessionIter kv.Iterator
	if reverse {
		sessionIter, err = sessionData.IterReverse(upperBound)
	} else {
		sessionIter, err = sessionData.Iter(k, upperBound)
	}

	if err != nil {
		snapIter.Close()
		return nil, err
	}

	iter, err = txn.NewUnionIter(sessionIter, snapIter, reverse)
	if err != nil {
		snapIter.Close()
		sessionIter.Close()
	}
	return iter, err
}

func getRangeAccessedTableID(startKey, endKey kv.Key) (int64, bool) {
	tblID, ok := getKeyAccessedTableID(startKey)
	if !ok {
		return 0, false
	}

	tblStart := tablecodec.EncodeTablePrefix(tblID)
	tblEnd := tablecodec.EncodeTablePrefix(tblID + 1)
	if bytes.HasPrefix(endKey, tblStart) || bytes.Equal(endKey, tblEnd) {
		return tblID, true
	}

	return 0, false
}

func getKeyAccessedTableID(k kv.Key) (int64, bool) {
	if bytes.HasPrefix(k, tablecodec.TablePrefix()) && len(k) >= tableWithIDPrefixLen {
		if tbID := tablecodec.DecodeTableID(k); tbID > 0 && tbID != math.MaxInt64 {
			return tbID, true
		}
	}

	return 0, false
}

func notTableRange(k, upperBound kv.Key) bool {
	tblPrefix := tablecodec.TablePrefix()
	return bytes.Compare(k, tblPrefix) > 0 && !bytes.HasPrefix(k, tblPrefix) ||
		len(upperBound) > 0 && bytes.Compare(upperBound, tblPrefix) < 0
}
