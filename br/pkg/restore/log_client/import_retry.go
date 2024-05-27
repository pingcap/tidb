// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/client-go/v2/kv"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegionFunc func(ctx context.Context, r *split.RegionInfo) RPCResult

type OverRegionsInRangeController struct {
	start      []byte
	end        []byte
	metaClient split.SplitClient

	errors error
	rs     *utils.RetryState
}

// OverRegionsInRange creates a controller that cloud be used to scan regions in a range and
// apply a function over these regions.
// You can then call the `Run` method for applying some functions.
func OverRegionsInRange(start, end []byte, metaClient split.SplitClient, retryStatus *utils.RetryState) OverRegionsInRangeController {
	// IMPORTANT: we record the start/end key with TimeStamp.
	// but scanRegion will drop the TimeStamp and the end key is exclusive.
	// if we do not use PrefixNextKey. we might scan fewer regions than we expected.
	// and finally cause the data lost.
	end = restoreutils.TruncateTS(end)
	end = kv.PrefixNextKey(end)

	return OverRegionsInRangeController{
		start:      start,
		end:        end,
		metaClient: metaClient,
		rs:         retryStatus,
	}
}

func (o *OverRegionsInRangeController) onError(_ context.Context, result RPCResult, region *split.RegionInfo) {
	o.errors = multierr.Append(o.errors, errors.Annotatef(&result, "execute over region %v failed", region.Region))
	// TODO: Maybe handle some of region errors like `epoch not match`?
}

func (o *OverRegionsInRangeController) tryFindLeader(ctx context.Context, region *split.RegionInfo) (*metapb.Peer, error) {
	var leader *metapb.Peer
	failed := false
	leaderRs := utils.InitialRetryState(4, 5*time.Second, 10*time.Second)
	err := utils.WithRetry(ctx, func() error {
		r, err := o.metaClient.GetRegionByID(ctx, region.Region.Id)
		if err != nil {
			return err
		}
		if !split.CheckRegionEpoch(r, region) {
			failed = true
			return nil
		}
		if r.Leader != nil {
			leader = r.Leader
			return nil
		}
		return errors.Annotatef(berrors.ErrPDLeaderNotFound, "there is no leader for region %d", region.Region.Id)
	}, &leaderRs)
	if failed {
		return nil, errors.Annotatef(berrors.ErrKVEpochNotMatch, "the current epoch of %s is changed", region)
	}
	if err != nil {
		return nil, err
	}
	return leader, nil
}

// handleInRegionError handles the error happens internal in the region. Update the region info, and perform a suitable backoff.
func (o *OverRegionsInRangeController) handleInRegionError(ctx context.Context, result RPCResult, region *split.RegionInfo) (cont bool) {
	if result.StoreError.GetServerIsBusy() != nil {
		if strings.Contains(result.StoreError.GetMessage(), "memory is limited") {
			sleepDuration := 15 * time.Second

			failpoint.Inject("hint-memory-is-limited", func(val failpoint.Value) {
				if val.(bool) {
					logutil.CL(ctx).Debug("failpoint hint-memory-is-limited injected.")
					sleepDuration = 100 * time.Microsecond
				}
			})
			time.Sleep(sleepDuration)
			return true
		}
	}

	if nl := result.StoreError.GetNotLeader(); nl != nil {
		if nl.Leader != nil {
			region.Leader = nl.Leader
			// try the new leader immediately.
			return true
		}
		// we retry manually, simply record the retry event.
		time.Sleep(o.rs.ExponentialBackoff())
		// There may not be leader, waiting...
		leader, err := o.tryFindLeader(ctx, region)
		if err != nil {
			// Leave the region info unchanged, let it retry then.
			logutil.CL(ctx).Warn("failed to find leader", logutil.Region(region.Region), logutil.ShortError(err))
			return false
		}
		region.Leader = leader
		return true
	}
	// For other errors, like `ServerIsBusy`, `RegionIsNotInitialized`, just trivially backoff.
	time.Sleep(o.rs.ExponentialBackoff())
	return true
}

func (o *OverRegionsInRangeController) prepareLogCtx(ctx context.Context) context.Context {
	lctx := logutil.ContextWithField(
		ctx,
		logutil.Key("startKey", o.start),
		logutil.Key("endKey", o.end),
	)
	return lctx
}

// Run executes the `regionFunc` over the regions in `o.start` and `o.end`.
// It would retry the errors according to the `rpcResponse`.
func (o *OverRegionsInRangeController) Run(ctx context.Context, f RegionFunc) error {
	return o.runOverRegions(o.prepareLogCtx(ctx), f)
}

func (o *OverRegionsInRangeController) runOverRegions(ctx context.Context, f RegionFunc) error {
	if !o.rs.ShouldRetry() {
		return o.errors
	}

	// Scan regions covered by the file range
	regionInfos, errScanRegion := split.PaginateScanRegion(
		ctx, o.metaClient, o.start, o.end, split.ScanRegionPaginationLimit)
	if errScanRegion != nil {
		return errors.Trace(errScanRegion)
	}

	for _, region := range regionInfos {
		cont, err := o.runInRegion(ctx, f, region)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// runInRegion executes the function in the region, and returns `cont = false` if no need for trying for next region.
func (o *OverRegionsInRangeController) runInRegion(ctx context.Context, f RegionFunc, region *split.RegionInfo) (cont bool, err error) {
	if !o.rs.ShouldRetry() {
		return false, o.errors
	}
	result := f(ctx, region)

	if !result.OK() {
		o.onError(ctx, result, region)
		switch result.StrategyForRetry() {
		case StrategyGiveUp:
			logutil.CL(ctx).Warn("unexpected error, should stop to retry", logutil.ShortError(&result), logutil.Region(region.Region))
			return false, o.errors
		case StrategyFromThisRegion:
			logutil.CL(ctx).Warn("retry for region", logutil.Region(region.Region), logutil.ShortError(&result))
			if !o.handleInRegionError(ctx, result, region) {
				return false, o.runOverRegions(ctx, f)
			}
			return o.runInRegion(ctx, f, region)
		case StrategyFromStart:
			logutil.CL(ctx).Warn("retry for execution over regions", logutil.ShortError(&result))
			// TODO: make a backoffer considering more about the error info,
			//       instead of ingore the result and retry.
			time.Sleep(o.rs.ExponentialBackoff())
			return false, o.runOverRegions(ctx, f)
		}
	}
	return true, nil
}

// RPCResult is the result after executing some RPCs to TiKV.
type RPCResult struct {
	Err error

	ImportError string
	StoreError  *errorpb.Error
}

func RPCResultFromPBError(err *import_sstpb.Error) RPCResult {
	return RPCResult{
		ImportError: err.GetMessage(),
		StoreError:  err.GetStoreError(),
	}
}

func RPCResultFromError(err error) RPCResult {
	return RPCResult{
		Err: err,
	}
}

func RPCResultOK() RPCResult {
	return RPCResult{}
}

type RetryStrategy int

const (
	StrategyGiveUp RetryStrategy = iota
	StrategyFromThisRegion
	StrategyFromStart
)

func (r *RPCResult) StrategyForRetry() RetryStrategy {
	if r.Err != nil {
		return r.StrategyForRetryGoError()
	}
	return r.StrategyForRetryStoreError()
}

func (r *RPCResult) StrategyForRetryStoreError() RetryStrategy {
	if r.StoreError == nil && r.ImportError == "" {
		return StrategyGiveUp
	}

	if r.StoreError.GetServerIsBusy() != nil ||
		r.StoreError.GetRegionNotInitialized() != nil ||
		r.StoreError.GetNotLeader() != nil ||
		r.StoreError.GetServerIsBusy() != nil {
		return StrategyFromThisRegion
	}

	return StrategyFromStart
}

func (r *RPCResult) StrategyForRetryGoError() RetryStrategy {
	if r.Err == nil {
		return StrategyGiveUp
	}

	// we should unwrap the error or we cannot get the write gRPC status.
	if gRPCErr, ok := status.FromError(errors.Cause(r.Err)); ok {
		switch gRPCErr.Code() {
		case codes.Unavailable, codes.Aborted, codes.ResourceExhausted, codes.DeadlineExceeded:
			return StrategyFromThisRegion
		}
	}

	return StrategyGiveUp
}

func (r *RPCResult) Error() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	if r.StoreError != nil {
		return r.StoreError.GetMessage()
	}
	if r.ImportError != "" {
		return r.ImportError
	}
	return "BUG(There is no error but reported as error)"
}

func (r *RPCResult) OK() bool {
	return r.Err == nil && r.ImportError == "" && r.StoreError == nil
}
