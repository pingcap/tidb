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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
)

func buildSPFreshBuildRoutePlan(
	store kv.Storage,
	job *model.Job,
	tblInfo *model.TableInfo,
	indexInfos []*model.IndexInfo,
) (*backend.SPFreshBuildRoutePlan, error) {
	if !hasSPFreshIndex(indexInfos) {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("SPFresh build route plan requires KV storage")
	}
	if job == nil {
		return nil, errors.New("SPFresh build route plan requires job metadata")
	}
	if tblInfo == nil {
		return nil, errors.New("SPFresh build route plan requires table metadata")
	}
	index, err := singleSPFreshReadIndex(indexInfos)
	if err != nil {
		return nil, err
	}
	if job.RealStartTS == 0 {
		return nil, errors.New("SPFresh build route plan base snapshot ts is not set")
	}
	endpoints, err := tikvapi.NormalizeEndpoints(config.GetGlobalConfig().TiKVAPIServiceAddrs, true)
	if err != nil {
		return nil, err
	}
	keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      keyspaceID,
		JobID:           job.ID,
		PhysicalTableID: tblInfo.ID,
		IndexID:         index.ID,
		BaseSnapshotTS:  job.RealStartTS,
		OwnerEndpoints:  endpoints,
	}
	if err := routePlan.Validate(); err != nil {
		return nil, err
	}
	return routePlan, nil
}
