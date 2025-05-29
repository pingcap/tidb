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

//go:build !nextgen

package kerneltype

// IsNextGen returns true if the current kernel type is NextGen.
// see doc.go for more info.
func IsNextGen() bool {
	return false
}

// IsClassic returns true if the current kernel type is Classic.
func IsClassic() bool {
	return !IsNextGen()
}
