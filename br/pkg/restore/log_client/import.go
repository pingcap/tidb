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

package logclient

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type LogFileImporter struct {
	metaClient   split.SplitClient
	importClient importclient.ImporterClient
	backend      *backuppb.StorageBackend

	cacheKey string
}

// NewFileImporter returns a new file importClient.
func NewLogFileImporter(
	metaClient split.SplitClient,
	importClient importclient.ImporterClient,
	backend *backuppb.StorageBackend,
) *LogFileImporter {
	return &LogFileImporter{
		metaClient:   metaClient,
		backend:      backend,
		importClient: importClient,
		cacheKey:     fmt.Sprintf("BR-%s-%d", time.Now().Format("20060102150405"), rand.Int63()),
	}
}

func (importer *LogFileImporter) Close() error {
	if importer != nil && importer.importClient != nil {
		return importer.importClient.CloseGrpcClient()
	}
	return nil
}

func (importer *LogFileImporter) ClearFiles(ctx context.Context, pdClient pd.Client, prefix string) error {
	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range allStores {
		if s.State != metapb.StoreState_Up {
			continue
		}
		req := &import_sstpb.ClearRequest{
			Prefix: prefix,
		}
		_, err = importer.importClient.ClearFiles(ctx, s.GetId(), req)
		if err != nil {
			log.Warn("cleanup kv files failed", zap.Uint64("store", s.GetId()), zap.Error(err))
		}
	}
	return nil
}

// ImportKVFiles restores the kv events.
func (importer *LogFileImporter) ImportKVFiles(
	ctx context.Context,
	files []*LogDataFileInfo,
	rule *restoreutils.RewriteRules,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	supportBatch bool,
) error {
	var (
		startKey []byte
		endKey   []byte
		ranges   = make([]kv.KeyRange, len(files))
		err      error
	)

	if !supportBatch && len(files) > 1 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"do not support batch apply but files count:%v > 1", len(files))
	}
	log.Debug("import kv files", zap.Int("batch file count", len(files)))

	for i, f := range files {
		ranges[i].StartKey, ranges[i].EndKey, err = restoreutils.GetRewriteEncodedKeys(f, rule)
		if err != nil {
			return errors.Trace(err)
		}

		if len(startKey) == 0 || bytes.Compare(ranges[i].StartKey, startKey) < 0 {
			startKey = ranges[i].StartKey
		}
		if len(endKey) == 0 || bytes.Compare(ranges[i].EndKey, endKey) > 0 {
			endKey = ranges[i].EndKey
		}
	}

	log.Debug("rewrite file keys",
		logutil.Key("startKey", startKey), logutil.Key("endKey", endKey))

	// This RetryState will retry 45 time, about 10 min.
	rs := utils.InitialRetryState(45, 100*time.Millisecond, 15*time.Second)
	ctl := OverRegionsInRange(startKey, endKey, importer.metaClient, &rs)
	err = ctl.Run(ctx, func(ctx context.Context, r *split.RegionInfo) RPCResult {
		subfiles, errFilter := filterFilesByRegion(files, ranges, r)
		if errFilter != nil {
			return RPCResultFromError(errFilter)
		}
		if len(subfiles) == 0 {
			return RPCResultOK()
		}
		return importer.importKVFileForRegion(ctx, subfiles, rule, shiftStartTS, startTS, restoreTS, r, supportBatch)
	})
	return errors.Trace(err)
}

func filterFilesByRegion(
	files []*LogDataFileInfo,
	ranges []kv.KeyRange,
	r *split.RegionInfo,
) ([]*LogDataFileInfo, error) {
	if len(files) != len(ranges) {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument,
			"count of files no equals count of ranges, file-count:%v, ranges-count:%v",
			len(files), len(ranges))
	}

	output := make([]*LogDataFileInfo, 0, len(files))
	if r != nil && r.Region != nil {
		for i, f := range files {
			if bytes.Compare(r.Region.StartKey, ranges[i].EndKey) <= 0 &&
				(len(r.Region.EndKey) == 0 || bytes.Compare(r.Region.EndKey, ranges[i].StartKey) >= 0) {
				output = append(output, f)
			}
		}
	} else {
		output = files
	}

	return output, nil
}

// Import tries to import a file.
func (importer *LogFileImporter) importKVFileForRegion(
	ctx context.Context,
	files []*LogDataFileInfo,
	rule *restoreutils.RewriteRules,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	info *split.RegionInfo,
	supportBatch bool,
) RPCResult {
	// Try to download file.
	result := importer.downloadAndApplyKVFile(ctx, files, rule, info, shiftStartTS, startTS, restoreTS, supportBatch)
	if !result.OK() {
		errDownload := result.Err
		for _, e := range multierr.Errors(errDownload) {
			switch errors.Cause(e) { // nolint:errorlint
			case berrors.ErrKVRewriteRuleNotFound, berrors.ErrKVRangeIsEmpty:
				// Skip this region
				logutil.CL(ctx).Warn("download file skipped",
					logutil.Region(info.Region),
					logutil.ShortError(e))
				return RPCResultOK()
			}
		}
		logutil.CL(ctx).Warn("download and apply file failed",
			logutil.ShortError(&result))
		return result
	}
	summary.CollectInt("RegionInvolved", 1)
	return RPCResultOK()
}

func (importer *LogFileImporter) downloadAndApplyKVFile(
	ctx context.Context,
	files []*LogDataFileInfo,
	rules *restoreutils.RewriteRules,
	regionInfo *split.RegionInfo,
	shiftStartTS uint64,
	startTS uint64,
	restoreTS uint64,
	supportBatch bool,
) RPCResult {
	leader := regionInfo.Leader
	if leader == nil {
		return RPCResultFromError(errors.Annotatef(berrors.ErrPDLeaderNotFound,
			"region id %d has no leader", regionInfo.Region.Id))
	}

	metas := make([]*import_sstpb.KVMeta, 0, len(files))
	rewriteRules := make([]*import_sstpb.RewriteRule, 0, len(files))

	for _, file := range files {
		// Get the rewrite rule for the file.
		fileRule := restoreutils.FindMatchedRewriteRule(file, rules)
		if fileRule == nil {
			return RPCResultFromError(errors.Annotatef(berrors.ErrKVRewriteRuleNotFound,
				"rewrite rule for file %+v not find (in %+v)", file, rules))
		}
		rule := import_sstpb.RewriteRule{
			OldKeyPrefix: restoreutils.EncodeKeyPrefix(fileRule.GetOldKeyPrefix()),
			NewKeyPrefix: restoreutils.EncodeKeyPrefix(fileRule.GetNewKeyPrefix()),
		}

		meta := &import_sstpb.KVMeta{
			Name:        file.Path,
			Cf:          file.Cf,
			RangeOffset: file.RangeOffset,
			Length:      file.Length,
			RangeLength: file.RangeLength,
			IsDelete:    file.Type == backuppb.FileType_Delete,
			StartTs: func() uint64 {
				if file.Cf == stream.DefaultCF {
					return shiftStartTS
				}
				return startTS
			}(),
			RestoreTs:       restoreTS,
			StartKey:        regionInfo.Region.GetStartKey(),
			EndKey:          regionInfo.Region.GetEndKey(),
			Sha256:          file.GetSha256(),
			CompressionType: file.CompressionType,
		}

		metas = append(metas, meta)
		rewriteRules = append(rewriteRules, &rule)
	}

	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.GetId(),
		RegionEpoch: regionInfo.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	var req *import_sstpb.ApplyRequest
	if supportBatch {
		req = &import_sstpb.ApplyRequest{
			Metas:          metas,
			StorageBackend: importer.backend,
			RewriteRules:   rewriteRules,
			Context:        reqCtx,
			StorageCacheId: importer.cacheKey,
		}
	} else {
		req = &import_sstpb.ApplyRequest{
			Meta:           metas[0],
			StorageBackend: importer.backend,
			RewriteRule:    *rewriteRules[0],
			Context:        reqCtx,
			StorageCacheId: importer.cacheKey,
		}
	}

	log.Debug("apply kv file", logutil.Leader(leader))
	resp, err := importer.importClient.ApplyKVFile(ctx, leader.GetStoreId(), req)
	if err != nil {
		return RPCResultFromError(errors.Trace(err))
	}
	if resp.GetError() != nil {
		logutil.CL(ctx).Warn("import meet error", zap.Stringer("error", resp.GetError()))
		return RPCResultFromPBError(resp.GetError())
	}
	return RPCResultOK()
}
