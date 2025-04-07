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

const (
	// in this generation, TiKV store all data in its own disk, and each KV will
	// have 3 replica normally.
	storeEngineGenerationOP = "op"
	// in this generation, TiKV store all data in cloud storage, such as S3, and
	// use cloud infrastructure to make sure high availability. TiKV will load
	// data from cloud storage when needed.
	storeEngineGenerationCloud = "cloud"
)

// this var will be set at compile time.
var storeEngineGeneration = storeEngineGenerationOP

// IsCloudStore returns true if the store engine is based on cloud storage.
// currently, we use same TiDB code base for both OP and cloud store, we will use
// this method to distinguish them and run different code path.
func IsCloudStore() bool {
	return storeEngineGeneration == storeEngineGenerationCloud
}
