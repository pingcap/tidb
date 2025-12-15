// Copyright 2025 PingCAP, Inc.
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

package infosync

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// DeleteAffinityGroups removes affinity groups in PD with force=true.
func DeleteAffinityGroups(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	return is.affinityManager.DeleteAffinityGroups(ctx, ids)
}

// DeleteAffinityGroupsWithDefaultRetry will retry for default times.
func DeleteAffinityGroupsWithDefaultRetry(ctx context.Context, ids []string) (err error) {
	for i := 0; i <= RequestPDMaxRetry; i++ {
		if err = DeleteAffinityGroups(ctx, ids); err == nil || ErrHTTPServiceError.Equal(err) {
			return err
		}
		if i != RequestPDMaxRetry {
			logutil.BgLogger().Warn("Error occurs when DeleteAffinityGroups, retry", zap.Error(err), zap.Strings("groupIDs", ids))
			time.Sleep(RequestRetryInterval)
		}
	}
	return
}

// GetAffinityGroups gets affinity groups by their IDs.
func GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.affinityManager.GetAffinityGroups(ctx, ids)
}

// CreateAffinityGroupsIfNotExists creates affinity groups in PD (idempotent).
// It checks which groups already exist and only creates the ones that don't exist.
// This makes the operation safe for DDL job retries (e.g., after Owner switch or network failures).
func CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	return is.affinityManager.CreateAffinityGroupsIfNotExists(ctx, groups)
}
