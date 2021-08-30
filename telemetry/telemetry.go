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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// OwnerKey is the telemetry owner path that is saved to etcd.
	OwnerKey = "/tidb/telemetry/owner"
	// Prompt is the prompt for telemetry owner manager.
	Prompt = "telemetry"
	// ReportInterval is the interval of the report.
	ReportInterval = 6 * time.Hour
)

const (
	etcdOpTimeout = 3 * time.Second
	uploadTimeout = 60 * time.Second
	apiEndpoint   = "https://telemetry.pingcap.com/api/v1/tidb/report"
)

func getTelemetryGlobalVariable(ctx sessionctx.Context) (bool, error) {
	val, err := ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableTelemetry)
	return variable.TiDBOptOn(val), err
}

// IsTelemetryEnabled check whether telemetry enabled.
func IsTelemetryEnabled(ctx sessionctx.Context) (bool, error) {
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
	if enabled, err := IsTelemetryEnabled(ctx); err != nil || !enabled {
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
	enabled, err := IsTelemetryEnabled(ctx)
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
	postReportTelemetryData()

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
	logutil.BgLogger().Info(fmt.Sprintf("Uploading telemetry data to %s", apiEndpoint))
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

// InitialRun reports the Telmetry configuration and trigger an initial run
func InitialRun(ctx sessionctx.Context, etcdClient *clientv3.Client) error {
	enabled, err := IsTelemetryEnabled(ctx)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("Telemetry configuration", zap.String("endpoint", apiEndpoint), zap.Duration("report_interval", ReportInterval), zap.Bool("enabled", enabled))
	return ReportUsageData(ctx, etcdClient)
}
