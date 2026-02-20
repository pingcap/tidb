// Copyright 2020 PingCAP, Inc.
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

package ddl

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotKeys(ctx *ReorgContext, store kv.Storage, priority int, keyPrefix kv.Key, version uint64,
	startKey kv.Key, endKey kv.Key, fn recordIterFunc) error {
	isRecord := tablecodec.IsRecordKey(keyPrefix.Next())
	var firstKey kv.Key
	if startKey == nil {
		firstKey = keyPrefix
	} else {
		firstKey = startKey
	}

	var upperBound kv.Key
	if endKey == nil {
		upperBound = keyPrefix.PrefixNext()
	} else {
		upperBound = endKey.PrefixNext()
	}

	ver := kv.Version{Ver: version}
	snap := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.ResourceGroupName, ctx.resourceGroupName)

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(keyPrefix) {
			break
		}

		var handle kv.Handle
		if isRecord {
			handle, err = tablecodec.DecodeRowKey(it.Key())
			if err != nil {
				return errors.Trace(err)
			}
		}

		more, err := fn(handle, it.Key(), it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(it.Key()))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}

// GetRangeEndKey gets the actual end key for the range of [startKey, endKey).
func GetRangeEndKey(ctx *ReorgContext, store kv.Storage, priority int, keyPrefix kv.Key, startKey, endKey kv.Key) (kv.Key, error) {
	snap := store.GetSnapshot(kv.MaxVersion)
	snap.SetOption(kv.Priority, priority)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.ResourceGroupName, ctx.resourceGroupName)
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	it, err := snap.IterReverse(endKey, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() || !it.Key().HasPrefix(keyPrefix) {
		return startKey.Next(), nil
	}
	if it.Key().Cmp(startKey) < 0 {
		return startKey.Next(), nil
	}

	return it.Key().Next(), nil
}

func mergeWarningsAndWarningsCount(partWarnings, totalWarnings map[errors.ErrorID]*terror.Error, partWarningsCount, totalWarningsCount map[errors.ErrorID]int64) (map[errors.ErrorID]*terror.Error, map[errors.ErrorID]int64) {
	for _, warn := range partWarnings {
		if _, ok := totalWarningsCount[warn.ID()]; ok {
			totalWarningsCount[warn.ID()] += partWarningsCount[warn.ID()]
		} else {
			totalWarningsCount[warn.ID()] = partWarningsCount[warn.ID()]
			totalWarnings[warn.ID()] = warn
		}
	}
	return totalWarnings, totalWarningsCount
}

func logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&vardef.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.DDLLogger().Info("slow operations",
			zap.Duration("takeTimes", elapsed),
			zap.String("msg", slowMsg))
	}
}

// doneTaskKeeper keeps the done tasks and update the latest next key.
type doneTaskKeeper struct {
	doneTaskNextKey map[int]kv.Key
	current         int
	nextKey         kv.Key
}

func newDoneTaskKeeper(start kv.Key) *doneTaskKeeper {
	return &doneTaskKeeper{
		doneTaskNextKey: make(map[int]kv.Key),
		current:         0,
		nextKey:         start,
	}
}

func (n *doneTaskKeeper) updateNextKey(doneTaskID int, next kv.Key) {
	if doneTaskID == n.current {
		n.current++
		n.nextKey = next
		for {
			nKey, ok := n.doneTaskNextKey[n.current]
			if !ok {
				break
			}
			delete(n.doneTaskNextKey, n.current)
			n.current++
			n.nextKey = nKey
		}
		return
	}
	n.doneTaskNextKey[doneTaskID] = next
}
