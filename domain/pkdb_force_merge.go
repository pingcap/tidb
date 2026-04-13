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

package domain

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	mergeEmptyRegionsOwnerKey       = "/tidb/merge-empty-regions/owner"
	mergeEmptyRegionsOwnerPrompt    = "merge-empty-regions"
	mergeEmptyRegionsInitMinTableID = int64(1)
)

var mergeEmptyRegionsInterval = 10 * time.Minute

func (do *Domain) mergeEmptyRegionsLoop(ctx context.Context) {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("mergeEmptyRegionsLoop exited.")
		util.Recover(metrics.LabelDomain, "mergeEmptyRegionsLoop", nil, false)
	}()

	owner := do.newOwnerManager(mergeEmptyRegionsOwnerPrompt, mergeEmptyRegionsOwnerKey)
	ticker := time.NewTicker(mergeEmptyRegionsInterval)
	defer func() {
		ticker.Stop()
		owner.Cancel()
	}()

	for {
		if variable.EnableDropTableForceMerge.Load() && owner.IsOwner() {
			if err := do.doMergeEmptyRegions(ctx); err != nil {
				logutil.BgLogger().Error("merge-empty-regions scan failed", zap.Error(err))
			}
		}

		select {
		case <-ticker.C:
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) doMergeEmptyRegions(ctx context.Context) error {
	minTableID, err := do.loadOrInitMergeEmptyRegionsMinTableID()
	if err != nil {
		return errors.Trace(err)
	}

	maxTableID, ranges := ddl.GetMergeEmptyRegionsKeyRanges(do.InfoSchema(), minTableID)
	if maxTableID == 0 || minTableID >= maxTableID {
		return nil
	}

	if len(ranges) > 0 {
		if err := infosync.AddForceMergeRanges(ctx, ranges); err != nil {
			return errors.Trace(err)
		}
	}
	if err := do.storeMergeEmptyRegionsMinTableID(maxTableID); err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("finished merge-empty-regions scan",
		zap.Int64("startTableID", minTableID),
		zap.Int64("nextTableID", maxTableID),
		zap.Int("rangeCount", len(ranges)))
	return nil
}

func (do *Domain) loadOrInitMergeEmptyRegionsMinTableID() (int64, error) {
	minTableID := mergeEmptyRegionsInitMinTableID
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), do.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		tableID, ok, err := t.GetMergeEmptyRegionsMinTableID()
		if err != nil {
			return errors.Trace(err)
		}
		if !ok || tableID < mergeEmptyRegionsInitMinTableID {
			tableID = mergeEmptyRegionsInitMinTableID
			if err := t.SetMergeEmptyRegionsMinTableID(tableID); err != nil {
				return errors.Trace(err)
			}
		}
		minTableID = tableID
		return nil
	})
	return minTableID, errors.Trace(err)
}

func (do *Domain) storeMergeEmptyRegionsMinTableID(tableID int64) error {
	if tableID < mergeEmptyRegionsInitMinTableID {
		tableID = mergeEmptyRegionsInitMinTableID
	}
	return errors.Trace(kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), do.store, true, func(ctx context.Context, txn kv.Transaction) error {
		return errors.Trace(meta.NewMeta(txn).SetMergeEmptyRegionsMinTableID(tableID))
	}))
}

// ResetMergeEmptyRegionsMinTableID resets the background merge-empty-regions
// scan checkpoint to the initial table ID.
func (do *Domain) ResetMergeEmptyRegionsMinTableID() error {
	return errors.Trace(do.storeMergeEmptyRegionsMinTableID(mergeEmptyRegionsInitMinTableID))
}
