// Copyright 2024 PingCAP, Inc.
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

package infosync

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

// ForceMergeKeyRange describes one PD key range that should be force merged.
type ForceMergeKeyRange struct {
	StartKey []byte
	EndKey   []byte
}

type addForceMergeRangesRequest struct {
	StartKeysHex []string `json:"start_keys"`
	EndKeysHex   []string `json:"end_keys"`
}

const (
	forceMergeMaxBatchSize   = 2048
	forceMergeBatchSleepTime = 5 * time.Second
)

var forceMergePDRequestTimeout = 20 * time.Second

var getForceMergePDAddrs = func() ([]string, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if is.etcdCli == nil {
		return nil, errors.New("pd unavailable")
	}

	addrs := is.etcdCli.Endpoints()
	if len(addrs) == 0 {
		return nil, errors.New("pd unavailable")
	}
	return addrs, nil
}

var forceMergeBatchSleep = sleepForceMergeBatch

// AddForceMergeRanges sends `/regions/force-merge` request to PD.
func AddForceMergeRanges(ctx context.Context, ranges []ForceMergeKeyRange) error {
	if len(ranges) == 0 {
		return nil
	}

	addrs, err := getForceMergePDAddrs()
	if err != nil {
		logutil.BgLogger().Error("get PD addresses for force merge failed", zap.Error(err))
		return errors.Trace(err)
	}

	route := path.Join(pdapi.Regions, "force-merge")
	totalRangeCount := len(ranges)
	totalBatches := (totalRangeCount + forceMergeMaxBatchSize - 1) / forceMergeMaxBatchSize
	for batchIdx, start := 0, 0; start < len(ranges); batchIdx, start = batchIdx+1, start+forceMergeMaxBatchSize {
		end := start + forceMergeMaxBatchSize
		if end > len(ranges) {
			end = len(ranges)
		}
		batchNumber := batchIdx + 1
		batchRangeCount := end - start

		body, err := marshalAddForceMergeRangesRequest(ranges[start:end])
		if err != nil {
			logutil.BgLogger().Error("marshal force merge ranges batch failed", forceMergeBatchLogFields(
				batchNumber, totalBatches, batchRangeCount, totalRangeCount, zap.Error(err),
			)...)
			return errors.Trace(err)
		}

		respBody, err := doRequestWithBodyBytesTimeoutPerAddress(
			ctx,
			forceMergePDRequestTimeout,
			"AddForceMergeRanges",
			addrs,
			route,
			"POST",
			body,
		)
		if err != nil {
			logutil.BgLogger().Error("send force merge ranges batch to PD failed", forceMergeBatchLogFields(
				batchNumber, totalBatches, batchRangeCount, totalRangeCount, zap.Error(err),
			)...)
			return errors.Trace(err)
		}
		if respBody == nil {
			err = fmt.Errorf("PD %s endpoint is unsupported or rejected by precondition", route)
			logutil.BgLogger().Error("send force merge ranges batch to PD failed", forceMergeBatchLogFields(
				batchNumber, totalBatches, batchRangeCount, totalRangeCount, zap.Error(err),
			)...)
			return err
		}

		logutil.BgLogger().Info("sent force merge ranges batch to PD", forceMergeBatchLogFields(
			batchNumber, totalBatches, batchRangeCount, totalRangeCount, zap.Int("sentRangeCount", end),
		)...)

		if end < len(ranges) {
			if err := forceMergeBatchSleep(ctx, forceMergeBatchSleepTime); err != nil {
				logutil.BgLogger().Error("sleep between force merge range batches failed", forceMergeBatchLogFields(
					batchNumber, totalBatches, batchRangeCount, totalRangeCount,
					zap.Int("sentRangeCount", end),
					zap.Error(err),
				)...)
				return errors.Trace(err)
			}
		}
	}

	logutil.BgLogger().Info("sent all force merge ranges to PD",
		zap.Int("batchCount", totalBatches),
		zap.Int("totalRangeCount", totalRangeCount))
	return nil
}

func forceMergeBatchLogFields(
	batchNumber int,
	totalBatches int,
	batchRangeCount int,
	totalRangeCount int,
	extra ...zap.Field,
) []zap.Field {
	fields := []zap.Field{
		zap.Int("batchIndex", batchNumber),
		zap.Int("batchCount", totalBatches),
		zap.Int("rangeCount", batchRangeCount),
		zap.Int("totalRangeCount", totalRangeCount),
	}
	return append(fields, extra...)
}

func marshalAddForceMergeRangesRequest(ranges []ForceMergeKeyRange) ([]byte, error) {
	reqBody := addForceMergeRangesRequest{
		StartKeysHex: make([]string, 0, len(ranges)),
		EndKeysHex:   make([]string, 0, len(ranges)),
	}
	for _, keyRange := range ranges {
		reqBody.StartKeysHex = append(reqBody.StartKeysHex, hex.EncodeToString(keyRange.StartKey))
		reqBody.EndKeysHex = append(reqBody.EndKeysHex, hex.EncodeToString(keyRange.EndKey))
	}
	return json.Marshal(reqBody)
}

func sleepForceMergeBatch(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
