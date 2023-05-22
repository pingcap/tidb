// Copyright 2023 PingCAP, Inc.
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

package ingest

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const (
	ingestIndexRetryTimes  = 5
	ingestIndexConcurrency = 8
)

func validateChecksum(ctx context.Context, physicalID, indexID int64,
	store kv.Storage, local *verification.KVChecksum) error {
	beginTime := time.Now()
	client := store.GetClient()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	currentRetryTimes := 0
	for currentRetryTimes = 0; currentRetryTimes < ingestIndexRetryTimes; currentRetryTimes++ {
		req, err := buildIndexChecksumRequest(physicalID, indexID, ver.Ver, ingestIndexConcurrency)
		if err != nil {
			logutil.BgLogger().Warn("[ddl-ingest] build index checksum request failed",
				zap.Error(err), zap.Int64("physicalID", physicalID), zap.Int64("indexID", indexID))
			return nil
		}
		resp, err := sendIndexChecksumRequest(ctx, client, req)
		if err != nil {
			logutil.BgLogger().Warn("[ddl-ingest] send index checksum request failed",
				zap.Error(err), zap.Int64("physicalID", physicalID), zap.Int64("indexID", indexID))
			continue
		}
		localSum := local.Sum()
		localKVs := local.SumKVS()
		localKVBytes := local.SumSize()
		if localSum != resp.Checksum || localKVs != resp.TotalKvs || localKVBytes != resp.TotalBytes {
			logutil.BgLogger().Error("[ddl-ingest] inconsistent checksum detected",
				zap.Uint64("localSum", localSum), zap.Uint64("remoteSum", resp.Checksum),
				zap.Uint64("localKVs", localKVs), zap.Uint64("remoteKVs", resp.TotalKvs),
				zap.Uint64("localKVBytes", localKVBytes), zap.Uint64("remoteKVBytes", resp.TotalBytes),
				zap.Int64("physicalID", physicalID), zap.Int64("indexID", indexID))
			return dbterror.ErrIngestFailed.GenWithStackByArgs("inconsistent checksum")
		}
		logutil.BgLogger().Info("[ddl-ingest] finish checksum",
			zap.Uint64("checksum", localSum), zap.Uint64("totalKVs", localKVs),
			zap.Int64("physicalID", physicalID), zap.Int64("indexID", indexID),
			zap.Duration("takeTime", time.Since(beginTime)), zap.Int("retryTimes", currentRetryTimes))
		return nil
	}
	logutil.BgLogger().Warn("[ddl-ingest] send index checksum request failed",
		zap.Error(err), zap.Int64("physicalID", physicalID), zap.Int64("indexID", indexID))
	return nil
}

func buildIndexChecksumRequest(physicalID, indexID int64,
	startTS uint64, concurrency int) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}
	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(nil, physicalID, indexID, ranger.FullRange()).
		SetStartTS(startTS).
		SetChecksumRequest(checksum).
		SetConcurrency(concurrency).Build()
}

func sendIndexChecksumRequest(ctx context.Context, client kv.Client, req *kv.Request) (*tipb.ChecksumResponse, error) {
	killed := uint32(0)
	result, err := distsql.Checksum(ctx, client, req, kv.NewVariables(&killed))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer terror.Call(result.Close)
	resp := &tipb.ChecksumResponse{}
	for {
		data, err := result.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, errors.Trace(err)
		}
		resp.Checksum ^= checksum.Checksum
		resp.TotalKvs += checksum.TotalKvs
		resp.TotalBytes += checksum.TotalBytes
	}
	return resp, nil
}
