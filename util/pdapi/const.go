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

package pdapi

// HotRead / HotWrite is the pd apis to get the corresponding hot region information.
const (
	HotRead  = "/pd/api/v1/hotspot/regions/read"
	HotWrite = "/pd/api/v1/hotspot/regions/read"
	Stores   = "/pd/api/v1/stores"
)
