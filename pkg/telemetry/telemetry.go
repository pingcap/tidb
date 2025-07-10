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
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// ReportInterval is the interval of the report.
	ReportInterval = 6 * time.Hour
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

// ReportUsageData generates the latest usage data and print it to log.
func ReportUsageData(ctx sessionctx.Context) error {
	enabled, err := IsTelemetryEnabled(ctx)
	if err != nil || !enabled {
		return err
	}

	data := generateTelemetryData(ctx)
	postReportTelemetryData()

	rawJSON, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	Logger().Info("", zap.ByteString("telemetry data", rawJSON))

	return nil
}

// InitialRun reports the Telmetry configuration and trigger an initial run
func InitialRun(ctx sessionctx.Context) error {
	enabled, err := IsTelemetryEnabled(ctx)
	if err != nil {
		return err
	}
	Logger().Info("Telemetry configuration", zap.Duration("report_interval", ReportInterval), zap.Bool("enabled", enabled))
	return ReportUsageData(ctx)
}

// Logger with category "telemetry" is used to log telemetry related messages.
func Logger() *zap.Logger {
	return logutil.BgLogger().With(zap.String(logutil.LogFieldCategory, "telemetry"))
}
