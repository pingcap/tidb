// Copyright 2022 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// loadTableRanges load table key ranges from PD between given start key and end key.
// It returns up to `limit` ranges.
func loadTableRanges(
	ctx context.Context,
	pid int64,
	store kv.Storage,
	startKey, endKey kv.Key,
	splitKeys []kv.Key,
	limit int,
) ([]kv.KeyRange, error) {
	if len(startKey) == 0 && len(endKey) == 0 {
		logutil.DDLLogger().Info("load empty range",
			zap.Int64("physicalTableID", pid))
		return []kv.KeyRange{}, nil
	}

	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		logutil.DDLLogger().Info("load table ranges failed, unsupported storage",
			zap.String("storage", fmt.Sprintf("%T", store)),
			zap.Int64("physicalTableID", pid))
		return []kv.KeyRange{{StartKey: startKey, EndKey: endKey}}, nil
	}
	failpoint.Inject("setLimitForLoadTableRanges", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			limit = v
		}
	})

	rc := s.GetRegionCache()
	maxSleep := 10000 // ms
	bo := tikv.NewBackofferWithVars(ctx, maxSleep, nil)
	var ranges []kv.KeyRange
	maxRetryTimes := util.DefaultMaxRetries
	failpoint.Inject("loadTableRangesNoRetry", func() {
		maxRetryTimes = 1
	})
	err := util.RunWithRetry(maxRetryTimes, util.RetryInterval, func() (bool, error) {
		logutil.DDLLogger().Info("load table ranges from PD",
			zap.Int64("physicalTableID", pid),
			zap.String("start key", hex.EncodeToString(startKey)),
			zap.String("end key", hex.EncodeToString(endKey)))
		rs, err := rc.BatchLoadRegionsWithKeyRange(bo, startKey, endKey, limit)
		if err != nil {
			return false, errors.Trace(err)
		}
		var mockErr bool
		failpoint.InjectCall("beforeLoadRangeFromPD", &mockErr)
		if mockErr {
			return false, kv.ErrTxnRetryable
		}

		ranges = make([]kv.KeyRange, 0, len(rs))
		for _, r := range rs {
			ranges = append(ranges, kv.KeyRange{StartKey: r.StartKey(), EndKey: r.EndKey()})
		}
		err = validateAndFillRanges(ranges, startKey, endKey)
		if err != nil {
			return true, errors.Trace(err)
		}
		return false, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	ranges = splitRangesByKeys(ranges, splitKeys)
	logutil.DDLLogger().Info("load table ranges from PD done",
		zap.Int64("physicalTableID", pid),
		zap.String("range start", hex.EncodeToString(ranges[0].StartKey)),
		zap.String("range end", hex.EncodeToString(ranges[len(ranges)-1].EndKey)),
		zap.Int("range count", len(ranges)))
	failpoint.InjectCall("afterLoadTableRanges", len(ranges))
	return ranges, nil
}

// splitRangesByKeys splits the ranges into more ranges by given split keys.
// The split keys should be ordered.
func splitRangesByKeys(ranges []kv.KeyRange, splitKeys []kv.Key) []kv.KeyRange {
	if len(splitKeys) == 0 {
		return ranges
	}
	ret := make([]kv.KeyRange, 0, len(ranges)+len(splitKeys))
	for _, r := range ranges {
		start := r.StartKey
		finishOneRange := false
		for !finishOneRange {
			if len(splitKeys) == 0 {
				break
			}
			split := splitKeys[0]
			switch {
			case split.Cmp(start) <= 0:
				splitKeys = splitKeys[1:]
			case split.Cmp(r.EndKey) < 0:
				splitKeys = splitKeys[1:]
				ret = append(ret, kv.KeyRange{StartKey: start, EndKey: split})
				start = split
			default:
				finishOneRange = true
			}
		}
		ret = append(ret, kv.KeyRange{StartKey: start, EndKey: r.EndKey})
	}
	return ret
}

func validateAndFillRanges(ranges []kv.KeyRange, startKey, endKey []byte) error {
	failpoint.Inject("validateAndFillRangesErr", func() {
		failpoint.Return(dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs("mock"))
	})
	if len(ranges) == 0 {
		errMsg := fmt.Sprintf("cannot find region in range [%s, %s]",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		return dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	for i, r := range ranges {
		if i == 0 {
			s := r.StartKey
			if len(s) == 0 || bytes.Compare(s, startKey) < 0 {
				ranges[i].StartKey = startKey
			} else if bytes.Compare(s, startKey) > 0 {
				errMsg := fmt.Sprintf("get empty range at the beginning of ranges, expected %s, but got %s",
					hex.EncodeToString(startKey), hex.EncodeToString(s))
				return dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
			}
		}
		if i == len(ranges)-1 {
			e := r.EndKey
			if len(e) == 0 || bytes.Compare(e, endKey) > 0 {
				ranges[i].EndKey = endKey
			}
			// We don't need to check the end key because a limit may set before scanning regions.
		}
		if len(ranges[i].StartKey) == 0 || len(ranges[i].EndKey) == 0 {
			return dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs("get empty start/end key in the middle of ranges")
		}
		if i > 0 && !bytes.Equal(ranges[i-1].EndKey, ranges[i].StartKey) {
			errMsg := fmt.Sprintf("ranges are not continuous, last end key %s, next start key %s",
				hex.EncodeToString(ranges[i-1].EndKey), hex.EncodeToString(ranges[i].StartKey))
			return dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
		}
	}
	return nil
}

func getBatchTasks(
	t table.Table,
	reorgInfo *reorgInfo,
	kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator,
	bfWorkerTp backfillerType,
) []*reorgBackfillTask {
	batchTasks := make([]*reorgBackfillTask, 0, len(kvRanges))
	//nolint:forcetypeassert
	phyTbl := t.(table.PhysicalTable)
	for _, r := range kvRanges {
		taskID := taskIDAlloc.alloc()
		startKey := r.StartKey
		endKey := r.EndKey
		endKey = getActualEndKey(t, reorgInfo, bfWorkerTp, startKey, endKey, taskID)
		task := &reorgBackfillTask{
			id:            taskID,
			jobID:         reorgInfo.Job.ID,
			physicalTable: phyTbl,
			priority:      reorgInfo.Priority,
			startKey:      startKey,
			endKey:        endKey,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

func getActualEndKey(
	t table.Table,
	reorgInfo *reorgInfo,
	bfTp backfillerType,
	rangeStart, rangeEnd kv.Key,
	taskID int,
) kv.Key {
	job := reorgInfo.Job
	//nolint:forcetypeassert
	phyTbl := t.(table.PhysicalTable)

	if bfTp == typeAddIndexMergeTmpWorker {
		// Temp Index data does not grow infinitely, we can return the whole range
		// and IndexMergeTmpWorker should still be finished in a bounded time.
		return rangeEnd
	}
	if bfTp == typeAddIndexWorker && job.ReorgMeta.ReorgTp == model.ReorgTypeIngest {
		// Ingest worker uses coprocessor to read table data. It is fast enough,
		// we don't need to get the actual end key of this range.
		return rangeEnd
	}

	// Otherwise to avoid the future data written to key range of [backfillChunkEndKey, rangeEnd) and
	// backfill worker can't catch up, we shrink the end key to the actual written key for now.
	jobCtx := reorgInfo.NewJobContext()

	actualEndKey, err := GetRangeEndKey(jobCtx, reorgInfo.jobCtx.store, job.Priority, t.RecordPrefix(), rangeStart, rangeEnd)
	if err != nil {
		logutil.DDLLogger().Info("get backfill range task, get reverse key failed", zap.Error(err))
		return rangeEnd
	}
	logutil.DDLLogger().Info("get backfill range task, change end key",
		zap.Int("id", taskID),
		zap.Int64("pTbl", phyTbl.GetPhysicalID()),
		zap.String("end key", hex.EncodeToString(rangeEnd)),
		zap.String("current end key", hex.EncodeToString(actualEndKey)))
	return actualEndKey
}

// sendTasks sends tasks to workers, and returns remaining kvRanges that is not handled.
func sendTasks(
	exec backfillExecutor,
	t table.PhysicalTable,
	kvRanges []kv.KeyRange,
	reorgInfo *reorgInfo,
	taskIDAlloc *taskIDAllocator,
	bfWorkerTp backfillerType,
) error {
	batchTasks := getBatchTasks(t, reorgInfo, kvRanges, taskIDAlloc, bfWorkerTp)
	for _, task := range batchTasks {
		if err := exec.sendTask(task); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
