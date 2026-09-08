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

package snapclient

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/kv"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// restoreRegions deliberately has no restore retry: a failed RPC can already
// have applied. In particular, do not replay successful Regions in a file group.
func (importer *SnapFileImporter) restoreRegions(
	ctx context.Context, startKey, endKey []byte, files []restore.BackupFileSet,
) error {
	regions, err := importer.paginateScanRegion(ctx, startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}
	for _, region := range regions {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := importer.buildRestoreRegionRequest(region, files)
		if err != nil {
			return err
		}
		if len(req.Sources) == 0 {
			continue
		}
		if err := importer.restoreOneRegion(ctx, req); err != nil {
			return errors.Annotatef(err, "RestoreRegion %d failed; automatic retry is disabled because the restore may have applied", req.Context.RegionId)
		}
	}
	return nil
}

func (importer *SnapFileImporter) buildRestoreRegionRequest(
	info *split.RegionInfo, files []restore.BackupFileSet,
) (*import_sstpb.RestoreRegionRequest, error) {
	if info.Leader == nil {
		return nil, errors.Annotatef(berrors.ErrPDLeaderNotFound, "region id %d has no leader", info.Region.Id)
	}
	req := &import_sstpb.RestoreRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId: info.Region.GetId(), RegionEpoch: info.Region.GetRegionEpoch(),
			Peer: info.Leader, ApiVersion: importer.apiVersion,
			RequestSource: kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR),
		},
		StorageBackend: importer.backend,
	}
	// The Store distinguishes an absent cipher from an unsupported cipher type.
	if importer.cipher != nil && importer.cipher.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT {
		req.CipherInfo = importer.cipher
	}
	for _, set := range files {
		for _, file := range set.SSTFiles {
			rule := restoreutils.FindMatchedRewriteRule(file, set.RewriteRules)
			if rule == nil {
				return nil, errors.Trace(berrors.ErrKVRewriteRuleNotFound)
			}
			start := restoreutils.RewriteAndEncodeRawKey(file.StartKey, rule)
			end := restoreutils.RewriteAndEncodeRawKey(file.EndKey, rule)
			if (len(info.Region.EndKey) > 0 && bytes.Compare(start, info.Region.EndKey) >= 0) ||
				bytes.Compare(end, info.Region.StartKey) <= 0 {
				continue
			}
			if file.Cf != restoreutils.WriteCFName && file.Cf != restoreutils.DefaultCFName {
				return nil, errors.New("RestoreRegion requires write/default source files")
			}
			ruleCopy := *rule
			if err := restoreutils.SetTimeRangeFilter(set.RewriteRules, &ruleCopy, file.Cf); err != nil {
				return nil, err
			}
			if ruleCopy.IgnoreBeforeTimestamp != 0 || ruleCopy.IgnoreAfterTimestamp != 0 {
				return nil, errors.New("RestoreRegion does not support timestamp filtering")
			}
			// Prefixes stay logical, including keyspace bytes. The Store supplies
			// the Region crop range; no Download UUID or temporary SSTMeta is used.
			req.Sources = append(req.Sources, &import_sstpb.RestoreRegionSource{
				Name: file.Name, Length: file.Size_, Cf: file.Cf,
				RewriteRule: &ruleCopy, CipherIv: file.CipherIv,
			})
		}
	}
	// Match the existing Store/Worker protocol limits before any restore work.
	if len(req.Sources) > 16384 || req.Size() > 16<<20 {
		return nil, errors.New("RestoreRegion request exceeds the Store/Worker limit")
	}
	return req, nil
}

func (importer *SnapFileImporter) restoreOneRegion(ctx context.Context, req *import_sstpb.RestoreRegionRequest) error {
	storeID := req.Context.Peer.GetStoreId()
	tokens := importer.ingestTokensMap.acquireTokenCh(storeID, importer.concurrencyPerStore)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tokens:
	}
	defer importer.releaseToken(tokens)
	if err := ctx.Err(); err != nil {
		return err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, gRPCTimeOut)
	defer cancel()
	logger := logutil.CL(ctx).With(zap.Uint64("region-id", req.Context.RegionId),
		zap.Uint64("store-id", storeID), zap.Int("source-count", len(req.Sources)))
	logger.Info("RestoreRegion started")
	for _, source := range req.Sources {
		logger.Debug("RestoreRegion source", zap.String("name", source.Name), zap.String("cf", source.Cf),
			zap.Binary("old-prefix", source.RewriteRule.OldKeyPrefix), zap.Binary("new-prefix", source.RewriteRule.NewKeyPrefix))
	}
	resp, err := importer.importClient.RestoreRegion(rpcCtx, storeID, req)
	if err != nil {
		return errors.Trace(err)
	}
	if resp == nil {
		return errors.New("RestoreRegion returned no response")
	}
	if resp.GetError() != nil {
		return errors.Annotatef(berrors.ErrKVIngestFailed, "RestoreRegion error: %s", resp.GetError())
	}
	logger.Info("RestoreRegion completed")
	return nil
}
