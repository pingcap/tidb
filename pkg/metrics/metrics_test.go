// Copyright 2018 PingCAP, Inc.
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

package metrics_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	_ "github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/stretchr/testify/require"
)

func TestMetrics(_ *testing.T) {
	// Make sure it doesn't panic.
	metrics.PanicCounter.WithLabelValues(metrics.LabelDomain).Inc()
}

func TestRegisterMetrics(_ *testing.T) {
	// Make sure it doesn't panic.
	metrics.RegisterMetrics()
}

func TestExecuteErrorToLabel(t *testing.T) {
	require.Equal(t, `unknown`, metrics.ExecuteErrorToLabel(errors.New("test")))
	require.Equal(t, `global:2`, metrics.ExecuteErrorToLabel(terror.ErrResultUndetermined))
}
