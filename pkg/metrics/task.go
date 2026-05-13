// Copyright 2025 PingCAP, Inc.
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

package metrics

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/pingcap/errors"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryableErrorCount counts retryable errors that occur during long-running
// background tasks (DDL jobs and DXF tasks such as IMPORT INTO, plus their
// shared dependencies in lightning / objstore / etcd helpers).
var RetryableErrorCount *prometheus.CounterVec

var (
	errMapMutex sync.Mutex
	errMapLimit = 1024
	errMap      = make(map[string]struct{}, errMapLimit)
)

// digitsRE collapses consecutive digit runs in fallback error strings to a single '?',
// so messages differing only in IDs / ports / counters share the same metric label.
var digitsRE = regexp.MustCompile(`[0-9]+`)

// AddRetryableError increments the retryable error counter with the given error.
// To bound Prometheus label cardinality, when the number of distinct labels seen
// since the last reset exceeds errMapLimit, the entire counter vector is Reset()
// and the label set is cleared — accumulated history for this metric is lost in
// exchange for a hard cap on series count.
func AddRetryableError(err error) {
	var s string
	err = errors.Cause(err)
	if e, ok := err.(*errors.Error); ok {
		s = string(e.RFCCode())
	} else if rpcStatus, ok := status.FromError(err); ok && rpcStatus.Code() != codes.Unknown {
		s = fmt.Sprintf("rpc error: %s", rpcStatus.Code())
	} else {
		s = digitsRE.ReplaceAllString(err.Error(), "?")
	}

	errMapMutex.Lock()
	defer errMapMutex.Unlock()

	errMap[s] = struct{}{}
	if len(errMap) > errMapLimit {
		errMap = make(map[string]struct{}, errMapLimit)
		RetryableErrorCount.Reset()
	}

	RetryableErrorCount.WithLabelValues(s).Inc()
}

// InitTaskMetrics initializes metrics shared across DDL jobs and DXF tasks.
func InitTaskMetrics() {
	RetryableErrorCount = metricscommon.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "task",
		Name:      "retryable_error_total",
		Help:      "Retryable error count during DDL jobs and DXF tasks (e.g., IMPORT INTO).",
	}, []string{LblType})
}
