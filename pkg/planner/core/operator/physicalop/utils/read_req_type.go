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

package utils

// ReadReqType is the read request type of the operator. Currently, only PhysicalTableReader uses this.
type ReadReqType uint8

const (
	// Cop means read from storage by cop request.
	Cop ReadReqType = iota
	// BatchCop means read from storage by BatchCop request, only used for TiFlash
	BatchCop
	// MPP means read from storage by MPP request, only used for TiFlash
	MPP
)

// Name returns the name of read request type.
func (r ReadReqType) Name() string {
	switch r {
	case BatchCop:
		return "batchCop"
	case MPP:
		return "mpp"
	default:
		// return cop by default
		return "cop"
	}
}
