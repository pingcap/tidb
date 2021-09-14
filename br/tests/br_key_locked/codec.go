// Copyright 2019 PingCAP, Inc.
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

// This file is copied from pingcap/tidb/store/tikv/pd_codec.go https://git.io/Je1Ww

package main

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"
)

type codecPDClient struct {
	pd.Client
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes(nil, key)
	region, err := c.Client.GetRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

func (c *codecPDClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes(nil, key)
	region, err := c.Client.GetPrevRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	region, err := c.Client.GetRegionByID(ctx, regionID)
	return processRegionResult(region, err)
}

func (c *codecPDClient) ScanRegions(
	ctx context.Context,
	startKey []byte,
	endKey []byte,
	limit int,
) ([]*pd.Region, error) {
	startKey = codec.EncodeBytes(nil, startKey)
	if len(endKey) > 0 {
		endKey = codec.EncodeBytes(nil, endKey)
	}

	regions, err := c.Client.ScanRegions(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, region := range regions {
		if region != nil {
			err = decodeRegionMetaKey(region.Meta)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return regions, nil
}

func processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, errors.Trace(err)
	}
	if region == nil {
		return nil, nil
	}
	err = decodeRegionMetaKey(region.Meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return region, nil
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
