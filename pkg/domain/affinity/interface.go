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

package affinity

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	// maxRetryTimes is the max retry times for affinity group operations
	maxRetryTimes = 3
	// retryInterval is the sleep time before next retry
	retryInterval = 200 * time.Millisecond
)

var (
	manager  Manager
	pdClient pdhttp.Client
)

// InitManager initializes the package-level affinity manager and PD client.
// This is called by infosync during initialization.
func InitManager(pdCli pdhttp.Client) {
	if pdCli == nil {
		manager = NewMockManager()
	} else {
		manager = NewPDManager(pdCli)
	}
	pdClient = pdCli
}

// CreateGroupsIfNotExists creates affinity groups in PD (idempotent).
// It checks which groups already exist and only creates the ones that don't exist.
// This makes the operation safe for DDL job retries (e.g., after Owner switch or network failures).
func CreateGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}
	return manager.CreateAffinityGroupsIfNotExists(ctx, groups)
}

// DeleteGroups removes affinity groups in PD with force=true.
func DeleteGroups(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return manager.DeleteAffinityGroups(ctx, ids)
}

// DeleteGroupsWithRetry deletes groups with retry logic.
// This is a best-effort cleanup operation. Errors are logged on final failure.
func DeleteGroupsWithRetry(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	var err error
	for i := 0; i <= maxRetryTimes; i++ {
		err = DeleteGroups(ctx, ids)
		if err == nil {
			return nil
		}
		if i != maxRetryTimes {
			time.Sleep(retryInterval)
		} else {
			// Only log error on final retry failure
			logutil.BgLogger().Error("Failed to delete affinity groups after retries", zap.Error(err), zap.Strings("groupIDs", ids))
		}
	}
	return err
}

// GetGroups gets affinity groups by their IDs.
func GetGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}
	return manager.GetAffinityGroups(ctx, ids)
}

// GetAllGroupStates gets all affinity group states from PD.
// This is used by SHOW AFFINITY to directly query PD without going through the manager.
func GetAllGroupStates(ctx context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	if pdClient == nil {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}
	return pdClient.GetAllAffinityGroups(ctx)
}
