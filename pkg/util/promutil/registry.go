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

import "github.com/prometheus/client_golang/prometheus"

// Registry is the interface to register or unregister metrics.
type Registry = prometheus.Registerer

var _ Registry = noopRegistry{}

type noopRegistry struct{}

func (noopRegistry) Register(_ prometheus.Collector) error {
	return nil
}

func (noopRegistry) MustRegister(_ ...prometheus.Collector) {}

func (noopRegistry) Unregister(_ prometheus.Collector) bool {
	return true
}

// NewNoopRegistry returns a Registry that does nothing.
// It is used for the case where metrics have been registered
// in factory automatically.
func NewNoopRegistry() Registry {
	return noopRegistry{}
}

// NewDefaultRegistry returns a default implementation of Registry.
func NewDefaultRegistry() Registry {
	return prometheus.NewRegistry()
}
