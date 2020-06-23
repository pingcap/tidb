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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.etcd.io/etcd/clientv3"
)

const (
	// OwnerKey is the telemetry owner path that is saved to etcd.
	OwnerKey = "/tidb/telemetry/owner"
	// Prompt is the prompt for telemetry owner manager.
	Prompt = "telemetry"
	// ReportInterval is the interval of the report.
	ReportInterval = 24 * time.Hour
)

const (
	etcdOpTimeout = 3 * time.Second
	uploadTimeout = 60 * time.Second
	apiEndpoint   = "https://telemetry.pingcap.com/api/v1/tidb/report"
)

func getTelemetryGlobalVariable(ctx sessionctx.Context) (bool, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(`SELECT @@global.tidb_enable_telemetry`)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) != 1 || rows[0].Len() == 0 {
		return false, fmt.Errorf("unexpected telemetry global variable")
	}
	return rows[0].GetString(0) == "1", nil
}

func isTelemetryEnabled(ctx sessionctx.Context) (bool, error) {
	if !config.GetGlobalConfig().EnableTelemetry {
		return false, nil
	}
	enabled, err := getTelemetryGlobalVariable(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	return enabled, nil
}

// PreviewUsageData returns a preview of the usage data that is going to be reported.
func PreviewUsageData(ctx sessionctx.Context, etcdClient *clientv3.Client) (string, error) {
	if etcdClient == nil {
		return "", nil
	}
	if enabled, err := isTelemetryEnabled(ctx); err != nil || !enabled {
		return "", err
	}

	trackingID, err := GetTrackingID(etcdClient)
	if err != nil {
		return "", errors.Trace(err)
	}

	// NOTE: trackingID may be empty. However, as a preview data, it is fine.
	data := generateTelemetryData(ctx, trackingID)

	prettyJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", errors.Trace(err)
	}

	return string(prettyJSON), nil
}

func reportUsageData(ctx sessionctx.Context, etcdClient *clientv3.Client) (bool, error) {
	if etcdClient == nil {
		// silently ignore
		return false, nil
	}
	enabled, err := isTelemetryEnabled(ctx)
	if err != nil {
		return false, err
	}
	if !enabled {
		return false, errors.Errorf("telemetry is disabled")
	}

	trackingID, err := GetTrackingID(etcdClient)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(trackingID) == 0 {
		trackingID, err = ResetTrackingID(etcdClient)
		if err != nil {
			return false, errors.Trace(err)
		}
	}

	data := generateTelemetryData(ctx, trackingID)

	rawJSON, err := json.Marshal(data)
	if err != nil {
		return false, errors.Trace(err)
	}

	// TODO: We should use the context from domain, so that when request is blocked for a long time it will not
	// affect TiDB shutdown.
	reqCtx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "POST", apiEndpoint, bytes.NewReader(rawJSON))
	if err != nil {
		return false, errors.Trace(err)
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, errors.Trace(err)
	}
	err = resp.Body.Close() // We don't even want to know any response body. Just close it.
	_ = err
	if resp.StatusCode != http.StatusOK {
		return false, errors.Errorf("Received non-Ok response when reporting usage data, http code: %d", resp.StatusCode)
	}

	return true, nil
}

// ReportUsageData generates the latest usage data and sends it to PingCAP. Status will be saved to etcd. Status update failures will be returned.
func ReportUsageData(ctx sessionctx.Context, etcdClient *clientv3.Client) error {
	if etcdClient == nil {
		// silently ignore
		return nil
	}
	s := status{
		CheckAt: time.Now().Format(time.RFC3339),
	}
	reported, err := reportUsageData(ctx, etcdClient)
	if err != nil {
		s.IsError = true
		s.ErrorMessage = err.Error()
	} else {
		s.IsRequestSent = reported
	}

	return updateTelemetryStatus(s, etcdClient)
}
