// Copyright 2026 PingCAP, Inc.
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
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"golang.org/x/sync/errgroup"
)

type tikvScannerProvider interface {
	getTiKVScanner(ctx context.Context, start, end []byte) (*txnsnapshot.Scanner, error)
}

func (em *engineManager) precheckConflictForImportEngineRanges(
	ctx context.Context,
	engineUUID uuid.UUID,
	ranges []tidbkv.KeyRange,
) error {
	engine := em.rLockEngine(engineUUID)
	if engine == nil {
		return errors.Errorf("engine '%s' not found", engineUUID)
	}
	defer engine.rUnlock()
	if !engine.duplicateDetection || len(ranges) == 0 {
		return nil
	}
	scannerProvider, ok := em.StoreHelper.(tikvScannerProvider)
	if !ok {
		return errors.New("tikv scanner is not available")
	}
	sizeProps, err := getSizePropertiesFn(engine.logger, engine.getDB(), engine.keyAdapter)
	if err != nil {
		return errors.Trace(err)
	}

	subRangeCh := make(chan tidbkv.KeyRange, em.PrecheckConcurrency)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(subRangeCh)
		splitRangeByLocalRangeProps(egCtx, ranges, sizeProps, subRangeCh)
		return nil
	})
	for range em.PrecheckConcurrency {
		eg.Go(func() error {
			return em.conflictWorker(egCtx, engine, scannerProvider, subRangeCh)
		})
	}
	return eg.Wait()
}

func splitRangeByLocalRangeProps(ctx context.Context, allRanges []tidbkv.KeyRange, sizeProps *sizeProperties, ch chan<- tidbkv.KeyRange) {
	for _, keyRange := range allRanges {
		curStart := keyRange.StartKey
		sizeProps.iter(func(p *rangeProperty) bool {
			if bytes.Compare(p.Key, curStart) <= 0 {
				return true
			}
			if len(keyRange.EndKey) > 0 && bytes.Compare(p.Key, keyRange.EndKey) >= 0 {
				return false
			}
			select {
			case ch <- tidbkv.KeyRange{StartKey: curStart, EndKey: p.Key}:
				curStart = p.Key
				return true
			case <-ctx.Done():
				return false
			}
		})
		if len(keyRange.EndKey) == 0 || bytes.Compare(curStart, keyRange.EndKey) < 0 {
			select {
			case ch <- tidbkv.KeyRange{StartKey: curStart, EndKey: keyRange.EndKey}:
			case <-ctx.Done():
			}
		}
	}
}

func (em *engineManager) conflictWorker(
	ctx context.Context,
	engine *Engine,
	scannerProvider tikvScannerProvider,
	ch <-chan tidbkv.KeyRange,
) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case subRange, ok := <-ch:
			if !ok {
				return context.Cause(ctx)
			}
			err := em.precheckConflictForImportEngineSubRange(ctx, engine, scannerProvider, subRange)
			if err != nil {
				return err
			}
		}
	}
}

func (em *engineManager) precheckConflictForImportEngineSubRange(
	ctx context.Context,
	engine *Engine,
	scannerProvider tikvScannerProvider,
	subRange tidbkv.KeyRange,
) (err error) {
	localIter := engine.newKVIter(ctx, &pebble.IterOptions{
		LowerBound: subRange.StartKey,
		UpperBound: subRange.EndKey,
	}, em.bufferPool.NewBuffer())
	defer func() {
		if closeErr := localIter.Close(); closeErr != nil && err == nil {
			err = errors.Trace(closeErr)
		}
	}()
	if !localIter.Last() {
		if localIter.Error() != nil {
			return errors.Trace(localIter.Error())
		}
		return nil
	}
	tikvCodec := em.GetTiKVCodec()
	remoteEndKey, err := tikvCodec.DecodeKey(localIter.Key())
	if err != nil {
		return errors.Trace(err)
	}
	remoteEndKey = nextKey(remoteEndKey)
	localIter.ReleaseBuf()
	if !localIter.First() {
		if localIter.Error() != nil {
			return errors.Trace(localIter.Error())
		}
		return errors.Errorf("unexpected invalid iterator state")
	}
	remoteStartKey := subRange.StartKey
	if len(remoteStartKey) > 0 {
		remoteStartKey, err = tikvCodec.DecodeKey(remoteStartKey)
		if err != nil {
			return errors.Trace(err)
		}
	}
	remoteIter, err := scannerProvider.getTiKVScanner(ctx, remoteStartKey, remoteEndKey)
	if err != nil {
		return errors.Trace(err)
	}
	defer remoteIter.Close()

	for localIter.Valid() && remoteIter.Valid() {
		localKey, err := tikvCodec.DecodeKey(localIter.Key())
		if err != nil {
			return errors.Trace(err)
		}
		cmp := bytes.Compare(localKey, remoteIter.Key())
		switch {
		case cmp < 0:
			localIter.ReleaseBuf()
			if !localIter.Next() {
				return errors.Trace(localIter.Error())
			}
		case cmp > 0:
			localIter.ReleaseBuf()
			if err := remoteIter.Next(); err != nil {
				return errors.Trace(err)
			}
		default:
			localVal := localIter.Value()
			return common.ErrFoundDuplicateKeys.FastGenByArgs(bytes.Clone(localKey), bytes.Clone(localVal))
		}
	}

	// The reason we need to continue iterating through the existing data until it finishes here is that localIter.Next() internally checks for duplicate adjacent elements within the incremental data. Therefore, keeping this empty for-loop is necessary.
	if localIter.Valid() {
		localIter.ReleaseBuf()
		for localIter.Next() {
		}
		if localIter.Error() != nil {
			return errors.Trace(localIter.Error())
		}
	}
	return nil
}
