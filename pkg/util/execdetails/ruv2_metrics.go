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

package execdetails

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type ruv2MetricsKeyType struct{}

// RUV2MetricsCtxKey is used to carry statement-level RUv2 metrics in context.Context.
var RUV2MetricsCtxKey = ruv2MetricsKeyType{}

// RUV2MetricsFromContext returns the RUv2 metrics stored in ctx.
func RUV2MetricsFromContext(ctx context.Context) *RUV2Metrics {
	if ctx == nil {
		return nil
	}
	if stmtDetails, _ := ctx.Value(StmtExecDetailKey).(*StmtExecDetails); stmtDetails != nil {
		if metrics := stmtDetails.getRUV2Metrics(); metrics != nil {
			return metrics
		}
	}
	// Keep the standalone context key as the fallback path for callers that
	// intentionally inherit RUv2 metrics into a context without StmtExecDetails.
	if metrics, _ := ctx.Value(RUV2MetricsCtxKey).(*RUV2Metrics); metrics != nil {
		return metrics
	}
	return nil
}

// UpdateRUV2MetricsFromRUV2 adds raw RUv2 counters into the statement-level metrics snapshot.
func UpdateRUV2MetricsFromRUV2(m *RUV2Metrics, ru *kvrpcpb.RUV2) {
	if m == nil || ru == nil || m.Bypass() {
		return
	}
	m.applyRawCounters(ru)
}

// applyRawCounters writes ru into m. Caller must check Bypass.
func (m *RUV2Metrics) applyRawCounters(ru *kvrpcpb.RUV2) {
	if v := ru.CoprocessorResponseBytes; v != 0 {
		atomic.AddInt64(&m.ensureExtra().tikvCoprocessorResponseBytes, int64(v))
	}
}

// SyncRUV2MetricsFromRUDetails drains the raw RUv2 counters accumulated in
// RUDetails since the last drain and adds them into the statement-level metrics.
// It is safe to call multiple times; each call transfers only the delta.
func SyncRUV2MetricsFromRUDetails(metrics *RUV2Metrics, ruDetails *tikvutil.RUDetails) {
	if metrics == nil || ruDetails == nil || metrics.Bypass() {
		return
	}
	UpdateRUV2MetricsFromRUV2(metrics, ruDetails.DrainRUV2())
}

// RUV2Metrics stores statement-level RUv2 metrics.
type RUV2Metrics struct {
	bypass atomic.Bool

	extra atomic.Pointer[ruv2MetricsExtra]
}

type ruv2MetricsExtra struct {
	tikvCoprocessorResponseBytes int64
}

func (m *RUV2Metrics) loadExtra() *ruv2MetricsExtra {
	if m == nil {
		return nil
	}
	return m.extra.Load()
}

func (m *RUV2Metrics) ensureExtra() *ruv2MetricsExtra {
	if m == nil {
		return nil
	}
	if extra := m.extra.Load(); extra != nil {
		return extra
	}
	extra := &ruv2MetricsExtra{}
	if m.extra.CompareAndSwap(nil, extra) {
		return extra
	}
	return m.extra.Load()
}

// NewRUV2Metrics creates a new RUv2 metrics container.
func NewRUV2Metrics() *RUV2Metrics {
	return &RUV2Metrics{}
}

// SetBypass marks whether statement-level RU accounting should be skipped.
func (m *RUV2Metrics) SetBypass(enabled bool) {
	m.bypass.Store(enabled)
}

// Bypass returns whether statement-level RU accounting should be skipped.
func (m *RUV2Metrics) Bypass() bool {
	return m.bypass.Load()
}

// AddTiKVCoprocessorResponseBytes records TiKV coprocessor response bytes.
func (m *RUV2Metrics) AddTiKVCoprocessorResponseBytes(delta int64) {
	if m.Bypass() {
		return
	}
	atomic.AddInt64(&m.ensureExtra().tikvCoprocessorResponseBytes, delta)
}

// TiKVCoprocessorResponseBytes returns TiKV coprocessor response bytes.
func (m *RUV2Metrics) TiKVCoprocessorResponseBytes() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.tikvCoprocessorResponseBytes)
}
