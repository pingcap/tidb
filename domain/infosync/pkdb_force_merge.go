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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/pdapi"
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
		return errors.Trace(err)
	}

	for start := 0; start < len(ranges); start += forceMergeMaxBatchSize {
		end := start + forceMergeMaxBatchSize
		if end > len(ranges) {
			end = len(ranges)
		}

		body, err := marshalAddForceMergeRangesRequest(ranges[start:end])
		if err != nil {
			return errors.Trace(err)
		}

		res, err := doRequest(
			ctx,
			"AddForceMergeRanges",
			addrs,
			path.Join(pdapi.Regions, "force-merge"),
			"POST",
			bytes.NewBuffer(body),
		)
		if err != nil {
			return errors.Trace(err)
		}
		if res == nil {
			return fmt.Errorf("InfoSyncer returns error in AddForceMergeRanges")
		}

		if end < len(ranges) {
			if err := forceMergeBatchSleep(ctx, forceMergeBatchSleepTime); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
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
