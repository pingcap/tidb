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

// Package geosrs describes the spatial reference systems (SRSs) TiDB supports: SRID 0,
// the unitless Cartesian plane, and SRID 4326, WGS 84 geographic.
//
// It is a stub for now that knows only the two SRIDs. It will hold the SRS catalog, such
// as the rows information_schema.st_spatial_reference_systems shows.
package geosrs

const (
	// SRID0 is the abstract Cartesian plane, with unitless X and Y and no bounds.
	SRID0 uint32 = 0
	// SRID4326 is WGS 84, the geographic SRS GPS coordinates are expressed in.
	SRID4326 uint32 = 4326
)

// Supported reports whether srid is a spatial reference system TiDB supports.
func Supported(srid uint32) bool {
	switch srid {
	case SRID0, SRID4326:
		return true
	}
	return false
}
