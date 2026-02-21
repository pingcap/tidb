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

package modelruntime

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/yalue/onnxruntime_go"
)

func swapEnv(values map[string]string) func() {
	old := getenvFn
	getenvFn = func(key string) string {
		if v, ok := values[key]; ok {
			return v
		}
		return ""
	}
	return func() { getenvFn = old }
}

func swapStat(fn func(string) (os.FileInfo, error)) func() {
	old := statFn
	statFn = fn
	return func() { statFn = old }
}

func swapExecutable(fn func() (string, error)) func() {
	old := execPathFn
	execPathFn = fn
	return func() { execPathFn = old }
}

func writeFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(""), 0o600)
}

func resetInitForTest() {
	initOnce = sync.Once{}
	initErr = nil
	initInfo = RuntimeInfo{}
	resolveLibraryPathFn = ResolveLibraryPath
	setSharedLibraryPathFn = onnxruntime_go.SetSharedLibraryPath
	initializeEnvironmentFn = onnxruntime_go.InitializeEnvironment
	getVersionFn = onnxruntime_go.GetVersion
	disableTelemetryFn = onnxruntime_go.DisableTelemetry
}
