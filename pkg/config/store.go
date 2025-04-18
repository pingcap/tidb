// Copyright 2024 PingCAP, Inc.
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

package config

// StoreType is the type of storage.
// TODO maybe put it inside pkg/store, but it introduces a cycle import.
type StoreType string

const (
	// StoreTypeTiKV is TiKV type. the underlying storage engines might be one or
	// multiple of TiKV/TiFlash/TiDB, see kv.StoreType for more details.
	StoreTypeTiKV StoreType = "tikv"
	// StoreTypeUniStore is UniStore type which we implemented using badger, for test only.
	StoreTypeUniStore StoreType = "unistore"
	// StoreTypeMockTiKV is MockTiKV type which we implemented using goleveldb, for test only.
	StoreTypeMockTiKV StoreType = "mocktikv"
)

// String implements fmt.Stringer interface.
func (t StoreType) String() string {
	return string(t)
}

// Valid returns true if the storage type is valid.
func (t StoreType) Valid() bool {
	switch t {
	case StoreTypeTiKV, StoreTypeUniStore, StoreTypeMockTiKV:
		return true
	}
	return false
}

// StoreTypeList returns all valid storage types.
func StoreTypeList() []StoreType {
	return []StoreType{StoreTypeTiKV, StoreTypeUniStore, StoreTypeMockTiKV}
}
