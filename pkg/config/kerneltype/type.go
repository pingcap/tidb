// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kerneltype

const (
	classicKernelName = "Classic"
	nextgenKernelName = "Next Generation"
)

// IsMatch checks if the given PD kernel type matches current instance.
// PD defines the kernel type name in the same wey as TiDB does, might we can unify
// them in the future. see:
// https://github.com/tikv/pd/blob/29ead019cd0982a3120bc79d4a4d19199dab2279/pkg/versioninfo/kerneltype/nextgen.go#L25
// https://github.com/tikv/pd/blob/29ead019cd0982a3120bc79d4a4d19199dab2279/pkg/versioninfo/kerneltype/classic.go#L25
func IsMatch(pdKernelType string) bool {
	if pdKernelType == "" {
		// old PD versions do not have kernel type defined, we will take it as classic.
		return classicKernelName == Name()
	}
	return pdKernelType == Name()
}

// Name returns the name of the current kernel type.
func Name() string {
	if IsNextGen() {
		return nextgenKernelName
	}
	return classicKernelName
}
