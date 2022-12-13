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

// The following constants are the APIs of PD server.
const (
	HotRead        = "/pd/api/v1/hotspot/regions/read"
	HotWrite       = "/pd/api/v1/hotspot/regions/write"
	Regions        = "/pd/api/v1/regions"
	RegionByID     = "/pd/api/v1/region/id/"
	Stores         = "/pd/api/v1/stores"
	ClusterVersion = "/pd/api/v1/config/cluster-version"
	Status         = "/pd/api/v1/status"
	Config         = "/pd/api/v1/config"
)
