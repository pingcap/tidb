// Copyright 2020 PingCAP, Inc.
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

package telemetry

import (
	"time"

	"github.com/pingcap/tidb/sessionctx"
)

type telemetryData struct {
	Hardware           []*clusterHardwareItem  `json:"hardware"`
	Instances          []*clusterInfoItem      `json:"instances"`
	TelemetryHostExtra *telemetryHostExtraInfo `json:"hostExtra"`
	ReportTimestamp    int64                   `json:"reportTimestamp"`
	TrackingID         string                  `json:"trackingId"`
}

func generateTelemetryData(ctx sessionctx.Context, trackingID string) telemetryData {
	r := telemetryData{
		ReportTimestamp: time.Now().Unix(),
		TrackingID:      trackingID,
	}
	if h, err := getClusterHardware(ctx); err == nil {
		r.Hardware = h
	}
	if i, err := getClusterInfo(ctx); err == nil {
		r.Instances = i
	}
	r.TelemetryHostExtra = getTelemetryHostExtraInfo()
	return r
}
