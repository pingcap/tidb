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

package metrics

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {
	// Make sure it doesn't panic.
	PanicCounter.WithLabelValues(LabelDomain).Inc()
}

func TestRegisterMetrics(t *testing.T) {
	// Make sure it doesn't panic.
	RegisterMetrics()
}

func TestRetLabel(t *testing.T) {
	require.Equal(t, opSucc, RetLabel(nil))
	require.Equal(t, opFailed, RetLabel(errors.New("test error")))
}

func TestExecuteErrorToLabel(t *testing.T) {
	require.Equal(t, `unknown`, ExecuteErrorToLabel(errors.New("test")))
	require.Equal(t, `global:2`, ExecuteErrorToLabel(terror.ErrResultUndetermined))
}

func BenchmarkPromHistogram(b *testing.B) {
	r := PacketIOHistogram.WithLabelValues("read")
	for i := 0; i < b.N; i++ {
		r.Observe(10)
	}
}

func BenchmarkPromCounter(b *testing.B) {
	r := QueryTotalCounter.WithLabelValues("read", "write")
	for i := 0; i < b.N; i++ {
		r.Add(10)
	}
}
