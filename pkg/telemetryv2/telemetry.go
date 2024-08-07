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

// Package telemetryv2 is unlike telemetry, it only collects usage data and update
// to local metrics. It does not report data to any remote endpoint.
package telemetryv2

import (
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
)

const (
	// UpdateInterval is the interval to update the telemetry status to local metrics.
	UpdateInterval = 1 * time.Minute
)

// ApplyUpdates collects data and records them to metrics.
// Some data collection is expensive so we don't implement it as a custom Prometheus collector.
// Instead, we collect them in a fixed interval and update the metrics.
func ApplyUpdates(ctx sessionctx.Context) {
	applyUpdatesVectorSearch(ctx)
}
