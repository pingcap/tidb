// Copyright 2022 PingCAP, Inc.
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

package promutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNoopRegistry(t *testing.T) {
	reg := NewNoopRegistry()

	// Registering a metric should not fail.
	err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NoError(t, err)
	// Registering a metric twice should not fail because this registry does nothing.
	err = reg.Register(prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NoError(t, err)

	// Unregistering a metric should always succeed.
	unregistered := reg.Unregister(prometheus.NewCounter(prometheus.CounterOpts{}))
	require.True(t, unregistered)
	unregistered = reg.Unregister(prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{}))
	require.True(t, unregistered)
}
