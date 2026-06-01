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

package vardef

import (
	"context"
	"sync/atomic"
)

// ClusterReadOnlyChecker returns whether all live TiDB instances are effectively read-only.
type ClusterReadOnlyChecker func(context.Context) (bool, error)

// ReadOnlyStatusReporter publishes the local TiDB read-only status.
type ReadOnlyStatusReporter func(context.Context) error

var (
	clusterReadOnlyChecker atomic.Value
	readOnlyStatusReporter atomic.Value
)

// LocalTiDBReadOnlyStatus returns the local TiDB effective read-only status.
func LocalTiDBReadOnlyStatus() bool {
	return RestrictedReadOnly.Load() || VarTiDBSuperReadOnly.Load()
}

// SetClusterReadOnlyChecker registers the checker used by the derived tidb_is_read_only variable.
func SetClusterReadOnlyChecker(checker ClusterReadOnlyChecker) {
	clusterReadOnlyChecker.Store(checker)
}

// GetClusterReadOnlyStatus returns whether the whole TiDB cluster is effectively read-only.
func GetClusterReadOnlyStatus(ctx context.Context) (bool, error) {
	checker, ok := clusterReadOnlyChecker.Load().(ClusterReadOnlyChecker)
	if !ok || checker == nil {
		return LocalTiDBReadOnlyStatus(), nil
	}
	return checker(ctx)
}

// SetReadOnlyStatusReporter registers a reporter for the local TiDB read-only status.
func SetReadOnlyStatusReporter(reporter ReadOnlyStatusReporter) {
	readOnlyStatusReporter.Store(reporter)
}

// ReportReadOnlyStatus publishes the local TiDB read-only status when a reporter is available.
func ReportReadOnlyStatus(ctx context.Context) error {
	reporter, ok := readOnlyStatusReporter.Load().(ReadOnlyStatusReporter)
	if !ok || reporter == nil {
		return nil
	}
	return reporter(ctx)
}
