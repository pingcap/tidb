package tikv

import (
	"github.com/pingcap/tidb/ddl/util"
	"context"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"bytes"
	"github.com/juju/errors"
)

type DeleteRangeResult struct {
	Regions int
	Canceled bool
}

func DeleteRange(ctx context.Context, bo *Backoffer, store Storage, r util.DelRangeTask) (DeleteRangeResult, error) {
	result := DeleteRangeResult{
		Regions: 0,
		Canceled: false,
	}

	startKey, rangeEndKey := r.Range()
	for {
		select {
		case <-ctx.Done():
			result.Canceled = true
			return result, nil
		default:
		}

		loc, err := store.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return result, errors.Trace(err)
		}

		endKey := loc.EndKey
		if loc.Contains(rangeEndKey) {
			endKey = rangeEndKey
		}

		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDeleteRange,
			DeleteRange: &kvrpcpb.DeleteRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			},
		}

		resp, err := store.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return result, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return result, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return result, errors.Trace(err)
			}
			continue
		}
		deleteRangeResp := resp.DeleteRange
		if deleteRangeResp == nil {
			return result, errors.Trace(ErrBodyMissing)
		}
		if err := deleteRangeResp.GetError(); err != "" {
			return result, errors.Errorf("unexpected delete range err: %v", err)
		}
		result.Regions++
		if bytes.Equal(endKey, rangeEndKey) {
			break
		}
		startKey = endKey
	}

	return result, nil
}
