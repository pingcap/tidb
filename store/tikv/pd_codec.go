// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/util/codec"
)

type codecPDClient struct {
	pd.Client
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, peer, err := c.Client.GetRegion(ctx, encodedKey)
	return processRegionResult(region, peer, err)
}

func (c *codecPDClient) GetPrevRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, peer, err := c.Client.GetPrevRegion(ctx, encodedKey)
	return processRegionResult(region, peer, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	region, peer, err := c.Client.GetRegionByID(ctx, regionID)
	return processRegionResult(region, peer, err)
}

func (c *codecPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*metapb.Region, []*metapb.Peer, error) {
	startKey = codec.EncodeBytes([]byte(nil), startKey)
	if len(endKey) > 0 {
		endKey = codec.EncodeBytes([]byte(nil), endKey)
	}

	regions, peers, err := c.Client.ScanRegions(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	for _, region := range regions {
		if region != nil {
			err = decodeRegionMetaKey(region)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
	return regions, peers, nil
}

func processRegionResult(region *metapb.Region, peer *metapb.Peer, err error) (*metapb.Region, *metapb.Peer, error) {
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if region == nil {
		return nil, nil, nil
	}
	err = decodeRegionMetaKey(region)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return region, peer, nil
}

func decodeRegionMetaKey(r *metapb.Region) error {
	if len(r.StartKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.StartKey, nil)
		if err != nil {
			return errors.Trace(err)
		}
		r.StartKey = decoded
	}
	if len(r.EndKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.EndKey, nil)
		if err != nil {
			return errors.Trace(err)
		}
		r.EndKey = decoded
	}
	return nil
}
