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
	"net/http"
	"time"

	"github.com/pingcap/errors"
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
	return retryTransientError(ctx, func(reqCtx context.Context) error {
		return manager.CreateAffinityGroupsIfNotExists(reqCtx, groups)
	}, isRetryableError)
}

// retryTransientError retries idempotent PD requests without holding up a
// cancelled job step. maxRetryTimes counts retries after the first request.
func retryTransientError(ctx context.Context, operation func(context.Context) error, isRetryable func(error) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return errors.Trace(err)
		}
		err := operation(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		if !isRetryable(err) || i == maxRetryTimes {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	code, ok := extractPDHTTPStatusCode(err)
	return !ok || code >= http.StatusInternalServerError
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
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	for i := 0; i <= maxRetryTimes; i++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		err = DeleteGroups(ctx, ids)
		if err == nil {
			return nil
		}
		if i != maxRetryTimes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
			}
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

// GetGroupStates gets the requested affinity group states from PD for SHOW AFFINITY.
// It uses the same compatibility fallbacks as DDL group lookups.
func GetGroupStates(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if pdClient == nil || len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}
	return NewPDManager(pdClient).GetAffinityGroups(ctx, ids)
}

// SetPDClientForTest sets the PD client for testing.
// Please do not use it in the production environment.
func SetPDClientForTest(cli pdhttp.Client) func() {
	originalCli := pdClient
	pdClient = cli
	return func() {
		pdClient = originalCli
	}
}
