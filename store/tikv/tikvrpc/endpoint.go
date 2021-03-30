// Copyright 2021 PingCAP, Inc.
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

package tikvrpc

// EndpointType represents the type of a remote endpoint..
type EndpointType uint8

// EndpointType type enums.
const (
	TiKV EndpointType = iota
	TiFlash
	TiDB
)

// Name returns the name of endpoint type.
func (t EndpointType) Name() string {
	switch t {
	case TiKV:
		return "tikv"
	case TiFlash:
		return "tiflash"
	case TiDB:
		return "tidb"
	}
	return "unspecified"
}
