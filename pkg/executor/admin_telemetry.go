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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// AdminShowTelemetryExec is an executor for ADMIN SHOW TELEMETRY.
type AdminShowTelemetryExec struct {
	exec.BaseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminShowTelemetryExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	dom := domain.GetDomain(e.Ctx())
	id, err := telemetry.GetTrackingID(dom.GetEtcdClient())
	if err != nil {
		return err
	}
	status, err := telemetry.GetTelemetryStatus(dom.GetEtcdClient())
	if err != nil {
		return err
	}
	previewData, err := telemetry.PreviewUsageData(e.Ctx(), dom.GetEtcdClient())
	if err != nil {
		return err
	}
	req.AppendString(0, id)
	req.AppendString(1, status)
	req.AppendString(2, previewData)
	return nil
}

// AdminResetTelemetryIDExec is an executor for ADMIN RESET TELEMETRY_ID.
type AdminResetTelemetryIDExec struct {
	exec.BaseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminResetTelemetryIDExec) Next(context.Context, *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	dom := domain.GetDomain(e.Ctx())
	_, err := telemetry.ResetTrackingID(dom.GetEtcdClient())
	return err
}
