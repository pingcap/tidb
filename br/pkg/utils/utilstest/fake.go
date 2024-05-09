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

package utilstest

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

var DefaultTestKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

type FakePDClient struct {
	pd.Client
	stores []*metapb.Store

	notLeader  bool
	retryTimes *int
}

func NewFakePDClient(stores []*metapb.Store, notLeader bool, retryTime *int) FakePDClient {
	var retryTimeInternal int
	if retryTime == nil {
		retryTime = &retryTimeInternal
	}
	return FakePDClient{
		stores: stores,

		notLeader:  notLeader,
		retryTimes: retryTime,
	}
}

func (fpdc FakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (fpdc FakePDClient) GetTS(ctx context.Context) (int64, int64, error) {
	(*fpdc.retryTimes)++
	if *fpdc.retryTimes >= 3 { // the mock PD leader switched successfully
		fpdc.notLeader = false
	}

	if fpdc.notLeader {
		return 0, 0, errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
	}
	return 1, 1, nil
}
