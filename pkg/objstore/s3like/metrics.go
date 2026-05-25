// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3like

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// BackendS3 is the metric label value for AWS S3 and compatible S3 stores.
	BackendS3 = "s3"
	// BackendOSS is the metric label value for Alibaba Cloud OSS.
	BackendOSS = "oss"
	// BackendKS3 is the metric label value for Kingsoft Cloud KS3.
	BackendKS3 = "ks3"

	// APICallListObjects is the metric label value for ListObjects API calls.
	APICallListObjects = "ListObjects"
	// APICallHeadObjects is the metric label value for HeadObject API calls.
	APICallHeadObjects = "HeadObjects"
)

var (
	// S3APICallCounter counts S3-compatible API calls made by external storage.
	S3APICallCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "br_s3",
			Name:      "api_call_total",
			Help:      "The total number of S3-compatible API calls made by BR external storage.",
		},
		[]string{"backend", "api"},
	)
)

func init() {
	prometheus.MustRegister(S3APICallCounter)
}

// RecordAPICall records one S3-compatible API call.
func RecordAPICall(backend, api string) {
	S3APICallCounter.WithLabelValues(backend, api).Inc()
}
