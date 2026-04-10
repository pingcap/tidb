// Copyright 2023 PingCAP, Inc.
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

package cpuprofile

import (
	"net/http"
)

// ExportedDefProfileDuration is an exported alias for DefProfileDuration variable to be used in tests.
var ExportedDefProfileDuration = &DefProfileDuration

// ExportedStartCPUProfiler is an exported wrapper for StartCPUProfiler function to be used in tests.
func ExportedStartCPUProfiler() error {
	return StartCPUProfiler()
}

// ExportedStopCPUProfiler is an exported wrapper for StopCPUProfiler function to be used in tests.
func ExportedStopCPUProfiler() {
	StopCPUProfiler()
}

// ExportedErrProfilerAlreadyStarted is an exported alias for errProfilerAlreadyStarted variable to be used in tests.
var ExportedErrProfilerAlreadyStarted = &errProfilerAlreadyStarted

// ExportedGlobalCPUProfiler is an exported alias for globalCPUProfiler variable to be used in tests.
var ExportedGlobalCPUProfiler = &globalCPUProfiler

// ExportedNewParallelCPUProfiler is an exported wrapper for newParallelCPUProfiler function to be used in tests.
func ExportedNewParallelCPUProfiler() *parallelCPUProfiler {
	return newParallelCPUProfiler()
}

// ExportedParallelCPUProfiler is an exported alias for parallelCPUProfiler type to be used in tests.
type ExportedParallelCPUProfiler = parallelCPUProfiler

// ExportedConsumersCount calls the consumersCount method on parallelCPUProfiler for tests.
func ExportedConsumersCount(p *parallelCPUProfiler) int {
	return p.consumersCount()
}

// ExportedProfileConsumer is an exported alias for ProfileConsumer type to be used in tests.
type ExportedProfileConsumer = ProfileConsumer

// ExportedRegister is an exported wrapper for Register function to be used in tests.
func ExportedRegister(ch ProfileConsumer) {
	Register(ch)
}

// ExportedUnregister is an exported wrapper for Unregister function to be used in tests.
func ExportedUnregister(ch ProfileConsumer) {
	Unregister(ch)
}

// ExportedNewCollector is an exported wrapper for NewCollector function to be used in tests.
func ExportedNewCollector() *Collector {
	return NewCollector()
}

// ExportedProfileHTTPHandler is an exported wrapper for ProfileHTTPHandler function to be used in tests.
func ExportedProfileHTTPHandler(w http.ResponseWriter, r *http.Request) {
	ProfileHTTPHandler(w, r)
}
