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

package startertest

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const (
	txnFileMetricName     = "tikv_client_go_txn_file_requests"
	tidbTxnFileMetricName = "tidb_tikvclient_txn_file_requests"
)

type txnFileCounters struct {
	ok  float64
	err float64
}

func readTxnFileCounters(ctx context.Context, statusURL string) (txnFileCounters, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, statusURL+"/metrics", nil)
	if err != nil {
		return txnFileCounters{}, fmt.Errorf("create metrics request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return txnFileCounters{}, fmt.Errorf("read metrics: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return txnFileCounters{}, fmt.Errorf("read metrics: status %s", resp.Status)
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return txnFileCounters{}, fmt.Errorf("parse metrics: %w", err)
	}
	family := families[txnFileMetricName]
	if family == nil {
		family = families[tidbTxnFileMetricName]
	}
	if family == nil || family.GetType() != dto.MetricType_COUNTER {
		return txnFileCounters{}, fmt.Errorf("parse metrics: txn-file counter family missing or invalid")
	}
	var counters txnFileCounters
	found := map[string]bool{"ok": false, "err": false}
	for _, metric := range family.GetMetric() {
		if metric.Counter == nil || math.IsNaN(metric.Counter.GetValue()) || math.IsInf(metric.Counter.GetValue(), 0) || metric.Counter.GetValue() < 0 {
			return txnFileCounters{}, fmt.Errorf("parse metrics: malformed txn-file counter sample")
		}
		labels := make(map[string]string, len(metric.GetLabel()))
		for _, label := range metric.GetLabel() {
			if _, exists := labels[label.GetName()]; label.GetName() == "" || exists {
				return txnFileCounters{}, fmt.Errorf("parse metrics: malformed txn-file labels")
			}
			labels[label.GetName()] = label.GetValue()
		}
		switch labels["type"] {
		case "ok":
			counters.ok += metric.Counter.GetValue()
			found["ok"] = true
		case "err":
			counters.err += metric.Counter.GetValue()
			found["err"] = true
		}
	}
	if !found["ok"] || !found["err"] {
		return txnFileCounters{}, fmt.Errorf("parse metrics: txn-file ok/err samples missing")
	}
	return counters, nil
}
