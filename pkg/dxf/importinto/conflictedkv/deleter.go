// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictedkv

import (
	"context"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	dxfhandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"go.uber.org/zap"
)

const (
	// (1+2+4+8)*0.1s + (10-4)*1s = 7.5s
	storeOpMinBackoff  = 100 * time.Millisecond
	storeOpMaxBackoff  = time.Second
	storeOpMaxRetryCnt = 10
)

var (
	// BufferedKeySizeLimit is the max size of keys buffered before delete.
	// exported for test.
	// we define those limit to be within how client define big transaction, see
	// https://github.com/tikv/client-go/blob/3150e385e39fbbb324fe975d68abe4fdf5dbd6ba/txnkv/transaction/2pc.go#L695-L696
	BufferedKeySizeLimit = 2 * units.MiB
	// BufferedKeyCountLimit is the max number of keys buffered before delete.
	BufferedKeyCountLimit = 9600
)

// Deleter deletes KVs related to conflicted KVs from TiKV.
type Deleter struct {
	handler  Handler
	keysCh   chan []tidbkv.Key
	store    tidbkv.Storage
	logger   *zap.Logger
	snapshot *LazyRefreshedSnapshot

	// we delete keys in batch
	bufferedKeys []tidbkv.Key
	bufSize      int
}

// NewDeleter creates a new conflict KV Deleter.
func NewDeleter(
	targetTbl table.Table,
	logger *zap.Logger,
	store tidbkv.Storage,
	kvGroup string,
	encoder *importer.TableKVEncoder,
) *Deleter {
	deleter := &Deleter{
		keysCh:   make(chan []tidbkv.Key),
		store:    store,
		logger:   logger,
		snapshot: NewLazyRefreshedSnapshot(store),
	}
	base := NewBaseHandler(targetTbl, kvGroup, encoder, deleter, logger)
	var h Handler
	if kvGroup == external.DataKVGroup {
		h = NewDataKVHandler(base)
	} else {
		h = NewIndexKVHandler(base, NewLazyRefreshedSnapshot(store), nil)
	}
	deleter.handler = h
	return deleter
}

// Run starts the deleter.
func (d *Deleter) Run(ctx context.Context, ch chan *external.KVPair) error {
	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)

	eg.Go(func() error {
		return d.deleteLoop(egCtx)
	})

	eg.Go(func() (err error) {
		defer func() {
			err2 := d.handler.Close(egCtx)
			if err == nil {
				err = err2
			}
			err2 = d.sendKeysToDelete(egCtx)
			if err == nil {
				err = err2
			}
			close(d.keysCh)
		}()

		if err = d.handler.PreRun(); err != nil {
			return err
		}
		return d.handler.Run(egCtx, ch)
	})

	return eg.Wait()
}

func (d *Deleter) deleteLoop(ctx context.Context) error {
	for keys := range d.keysCh {
		if err := d.deleteKeysWithRetry(ctx, keys); err != nil {
			return err
		}
	}
	return nil
}

func (d *Deleter) deleteKeysWithRetry(ctx context.Context, keys []tidbkv.Key) error {
	if len(keys) == 0 {
		return nil
	}
	backoffer := backoff.NewExponential(storeOpMinBackoff, 2, storeOpMaxBackoff)
	return dxfhandle.RunWithRetry(ctx, storeOpMaxRetryCnt, backoffer, d.logger, func(ctx context.Context) (bool, error) {
		err := d.deleteBufferedKeys(ctx, keys)
		if err != nil {
			return common.IsRetryableError(err), err
		}
		return true, nil
	})
}

func (d *Deleter) deleteBufferedKeys(ctx context.Context, keys []tidbkv.Key) error {
	txn, err := d.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				d.logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	for _, k := range keys {
		if err = txn.Delete(k); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// HandleEncodedRow implements the EncodedRowHandler interface.
func (d *Deleter) HandleEncodedRow(ctx context.Context, _ tidbkv.Handle, _ []types.Datum, kvPairs *kv.Pairs) error {
	return d.gatherAndDeleteKeysWithRetry(ctx, kvPairs.Pairs)
}

func (d *Deleter) gatherAndDeleteKeysWithRetry(ctx context.Context, pairs []common.KvPair) error {
	backoffer := backoff.NewExponential(storeOpMinBackoff, 2, storeOpMaxBackoff)
	if err := dxfhandle.RunWithRetry(ctx, storeOpMaxRetryCnt, backoffer, d.logger, func(ctx context.Context) (bool, error) {
		err := d.gatherKeysToDelete(ctx, pairs)
		if err != nil {
			return common.IsRetryableError(err), err
		}
		return true, nil
	}); err != nil {
		return err
	}

	if d.bufSize >= BufferedKeySizeLimit || len(d.bufferedKeys) >= BufferedKeyCountLimit {
		return d.sendKeysToDelete(ctx)
	}
	return nil
}

// we are deleting keys related to a single row in one transaction, and a normal
// 'insert SQL' will also generate this mount of data, so we shouldn't meet the
// 'transaction too large' issue in normal case.
// as all duplicate KVs are either removed or recorded during importing, and we
// only delete existing KVs, so there will be no overlap in the KVs to be deleted
// for any 2 conflict KVs in a single KV group, it's safe to resolve a single KV
// group in multiple routines, and we can use a relatively stale snapshot to check
// existence of the KVs to be deleted to avoid the overhead to refresh the TS
// every time.
func (d *Deleter) gatherKeysToDelete(ctx context.Context, pairs []common.KvPair) (err error) {
	if err = d.snapshot.refreshAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	allKeys := make([]tidbkv.Key, 0, len(pairs))
	for _, p := range pairs {
		allKeys = append(allKeys, p.Key)
	}
	res, err := d.snapshot.BatchGet(ctx, allKeys)
	if err != nil {
		return errors.Trace(err)
	}
	if len(res) == 0 {
		return nil
	}

	for k := range res {
		d.bufferedKeys = append(d.bufferedKeys, []byte(k))
		d.bufSize += len(k)
	}

	return nil
}

func (d *Deleter) sendKeysToDelete(ctx context.Context) error {
	if len(d.bufferedKeys) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.keysCh <- d.bufferedKeys:
		d.bufferedKeys = make([]tidbkv.Key, 0, len(d.bufferedKeys))
		d.bufSize = 0
		return nil
	}
}
