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
	stderrors "errors"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoreRegions retries a logical file batch with the same planned task ID.
// Every attempt refreshes Region routing; committed tasks are deduplicated by TiKV.
func (importer *SnapFileImporter) restoreRegions(
	ctx context.Context, startKey, endKey []byte, files []restore.BackupFileSet,
) error {
	backoff := utils.InitialRetryState(15, 40*time.Millisecond, 10*time.Second)
	outcomeUnknown := false
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return restoreBatchError(err, outcomeUnknown)
		}
		err := importer.restoreRegionBatch(ctx, startKey, endKey, files, &outcomeUnknown)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return restoreBatchError(ctx.Err(), outcomeUnknown)
		}
		if !retryRestoreRegion(err) || !backoff.ShouldRetry() {
			return restoreBatchError(err, outcomeUnknown)
		}
		delay := backoff.ExponentialBackoff()
		failpoint.Inject("restoreRegionRetryDelay", func(_ failpoint.Value) { delay = 0 })
		logutil.CL(ctx).Warn("retry RestoreRegion batch", zap.Int("attempt", attempt), zap.Duration("backoff", delay), zap.Error(err))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return restoreBatchError(ctx.Err(), outcomeUnknown)
		case <-timer.C:
		}
	}
}

func restoreBatchError(err error, outcomeUnknown bool) error {
	if outcomeUnknown {
		return errors.Annotate(err, "RestoreRegion batch failed; a previous RPC may have applied")
	}
	return errors.Trace(err)
}

// Keep the Region error structured: parsing its diagnostic would lose the
// distinction between routing/admission failures and permanent ingest errors.
type restoreRegionError struct{ region *errorpb.Error }

func (e *restoreRegionError) Error() string { return "RestoreRegion error: " + e.region.String() }

type restoreRegionRPCError struct{ error }

func (e *restoreRegionRPCError) Unwrap() error { return e.error }

func retryRestoreRegion(err error) bool {
	var region *restoreRegionError
	if stderrors.As(err, &region) {
		e := region.region
		return e.NotLeader != nil || e.EpochNotMatch != nil || e.RegionNotFound != nil ||
			e.ServerIsBusy != nil || e.IsWitness != nil || e.RegionNotInitialized != nil
	}
	if errors.ErrorEqual(err, berrors.ErrPDLeaderNotFound) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func (importer *SnapFileImporter) checkRestoreRegionCapability(ctx context.Context, storeID uint64) error {
	rpcCtx, cancel := context.WithTimeout(ctx, gRPCTimeOut)
	defer cancel()
	client, err := importer.importClient.GetImportClient(rpcCtx, storeID)
	if err != nil {
		return errors.Trace(err)
	}
	response, err := client.GetMode(rpcCtx, &import_sstpb.GetModeRequest{})
	if err != nil {
		return errors.Annotatef(err, "check RestoreRegion retry capability on store %d", storeID)
	}
	if response == nil || !response.SupportsRestoreRegionRetry {
		return errors.Errorf("store %d does not support safe RestoreRegion retries; upgrade every replica before restoring", storeID)
	}
	return nil
}

func (importer *SnapFileImporter) restoreRegionBatch(
	ctx context.Context, startKey, endKey []byte, files []restore.BackupFileSet, outcomeUnknown *bool,
) error {
	regions, err := importer.paginateScanRegion(ctx, startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}
	checked := make(map[uint64]struct{})
	// Include learners: any replica can apply the command and persist its identity.
	// Recheck on every round so membership changes do not reuse stale capability data.
	for _, region := range regions {
		for _, peer := range region.Region.Peers {
			if _, ok := checked[peer.StoreId]; ok {
				continue
			}
			if err := importer.checkRestoreRegionCapability(ctx, peer.StoreId); err != nil {
				return err
			}
			checked[peer.StoreId] = struct{}{}
		}
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
		if _, ok := checked[req.Context.Peer.StoreId]; !ok {
			if err := importer.checkRestoreRegionCapability(ctx, req.Context.Peer.StoreId); err != nil {
				return err
			}
			checked[req.Context.Peer.StoreId] = struct{}{}
		}
		if err := importer.restoreOneRegion(ctx, req); err != nil {
			var rpcError *restoreRegionRPCError
			if stderrors.As(err, &rpcError) {
				*outcomeUnknown = true
			}
			return err
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
	if importer.apiVersion != kvrpcpb.APIVersion_V2 {
		return nil, errors.New("RestoreRegion batch task identity requires API V2")
	}
	if len(files) == 0 || files[0].RestoreTaskID == [32]byte{} {
		return nil, errors.New("RestoreRegion requires a planned batch task ID")
	}
	for _, set := range files[1:] {
		if set.RestoreTaskID != files[0].RestoreTaskID {
			return nil, errors.New("RestoreRegion file sets have inconsistent batch task IDs")
		}
	}
	req := &import_sstpb.RestoreRegionRequest{
		RestoreTaskId: bytes.Clone(files[0].RestoreTaskID[:]),
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
			source := &import_sstpb.RestoreRegionSource{
				Name: file.Name, Length: file.Size_, Cf: file.Cf,
				RewriteRule: &ruleCopy,
			}
			// Backup metadata can contain an IV even for plaintext SSTs. The
			// Worker requires an IV exactly when a source cipher is present.
			if req.CipherInfo != nil {
				source.CipherIv = file.CipherIv
			}
			req.Sources = append(req.Sources, source)
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
		return &restoreRegionRPCError{errors.Trace(err)}
	}
	if resp == nil {
		return &restoreRegionRPCError{errors.New("RestoreRegion returned no response")}
	}
	if resp.GetError() != nil {
		return &restoreRegionError{region: resp.GetError()}
	}
	logger.Info("RestoreRegion completed")
	return nil
}
