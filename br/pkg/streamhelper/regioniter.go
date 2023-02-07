// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
)

const (
	defaultPageSize = 2048
)

type RegionWithLeader struct {
	Region *metapb.Region
	Leader *metapb.Peer
}

type TiKVClusterMeta interface {
	// RegionScan gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	RegionScan(ctx context.Context, key, endKey []byte, limit int) ([]RegionWithLeader, error)

	// Stores returns the store metadata from the cluster.
	Stores(ctx context.Context) ([]Store, error)
}

type Store struct {
	ID     uint64
	BootAt uint64
}

type RegionIter struct {
	cli              TiKVClusterMeta
	startKey, endKey []byte
	currentStartKey  []byte
	// When the endKey become "", we cannot check whether the scan is done by
	// comparing currentStartKey and endKey (because "" has different meaning in start key and end key).
	// So set this to `true` when endKey == "" and the scan is done.
	infScanFinished bool

	// The max slice size returned by `Next`.
	// This can be changed before calling `Next` each time,
	// however no thread safety provided.
	PageSize int
}

func (r *RegionIter) String() string {
	return fmt.Sprintf("RegionIter:%s;%v;from=%s",
		logutil.StringifyKeys([]kv.KeyRange{{StartKey: r.currentStartKey, EndKey: r.endKey}}),
		r.infScanFinished,
		redact.Key(r.startKey))
}

// IterateRegion creates an iterater over the region range.
func IterateRegion(cli TiKVClusterMeta, startKey, endKey []byte) *RegionIter {
	return &RegionIter{
		cli:             cli,
		startKey:        startKey,
		endKey:          endKey,
		currentStartKey: startKey,
		PageSize:        defaultPageSize,
	}
}

func CheckRegionConsistency(startKey, endKey []byte, regions []RegionWithLeader) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "scan region return empty result, startKey: %s, endKey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "first region's startKey > startKey, startKey: %s, regionStartKey: %s",
			redact.Key(startKey), redact.Key(regions[0].Region.StartKey))
	} else if len(regions[len(regions)-1].Region.EndKey) != 0 && bytes.Compare(regions[len(regions)-1].Region.EndKey, endKey) < 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "last region's endKey < endKey, endKey: %s, regionEndKey: %s",
			redact.Key(endKey), redact.Key(regions[len(regions)-1].Region.EndKey))
	}

	cur := regions[0]
	for _, r := range regions[1:] {
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion, "region endKey not equal to next region startKey, endKey: %s, startKey: %s",
				redact.Key(cur.Region.EndKey), redact.Key(r.Region.StartKey))
		}
		cur = r
	}

	return nil
}

// Next get the next page of regions.
func (r *RegionIter) Next(ctx context.Context) ([]RegionWithLeader, error) {
	var rs []RegionWithLeader
	state := utils.InitialRetryState(8, 500*time.Millisecond, 500*time.Millisecond)
	err := utils.WithRetry(ctx, func() (retErr error) {
		defer func() {
			if retErr != nil {
				log.Warn("failed with trying to scan regions", logutil.ShortError(retErr),
					logutil.Key("start", r.currentStartKey),
					logutil.Key("end", r.endKey),
				)
			}
			metrics.RegionCheckpointFailure.WithLabelValues("retryable-scan-region").Inc()
		}()
		regions, err := r.cli.RegionScan(ctx, r.currentStartKey, r.endKey, r.PageSize)
		if err != nil {
			return err
		}
		if len(regions) > 0 {
			endKey := regions[len(regions)-1].Region.GetEndKey()
			if err := CheckRegionConsistency(r.currentStartKey, endKey, regions); err != nil {
				return err
			}
			rs = regions
			return nil
		}
		return CheckRegionConsistency(r.currentStartKey, r.endKey, regions)
	}, &state)
	if err != nil {
		return nil, err
	}
	endKey := rs[len(rs)-1].Region.EndKey
	// We have meet the last region.
	if len(endKey) == 0 {
		r.infScanFinished = true
	}
	r.currentStartKey = endKey
	return rs, nil
}

// Done checks whether the iteration is done.
func (r *RegionIter) Done() bool {
	// special case: we want to scan to the end of key space.
	// at this time, comparing currentStartKey and endKey may be misleading when
	// they are both "".
	if len(r.endKey) == 0 {
		return r.infScanFinished
	}
	return r.infScanFinished || bytes.Compare(r.currentStartKey, r.endKey) >= 0
}
