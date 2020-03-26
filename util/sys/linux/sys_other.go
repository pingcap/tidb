// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
// +build !linux

package linux

import (
	"fmt"
	"runtime"
)

// OSVersion returns version info of operation system.
// for non-linux system will only return os and arch info.
func OSVersion() (osVersion string, err error) {
	osVersion = runtime.GOOS + "." + runtime.GOARCH
	return
}

// SetAffinity sets cpu affinity.
func SetAffinity(cpus []int) error {
	return nil
}

func GetTargetDirectoryCapacity(path string) (uint64, error) {
	return 0, fmt.Errorf("Get directory capacity not supported in non-linux system yet")
}
