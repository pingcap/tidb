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
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/util/codec"
)

type codecPDClient struct {
	pd.Client
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegion(key []byte) (*metapb.Region, error) {
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, err := c.Client.GetRegion(encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(region.StartKey) != 0 {
		_, decoded, err := codec.DecodeBytes(region.StartKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		region.StartKey = decoded
	}
	if len(region.EndKey) != 0 {
		_, decoded, err := codec.DecodeBytes(region.EndKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		region.EndKey = decoded
	}
	return region, nil
}
