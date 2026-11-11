// Copyright 2015 PingCAP, Inc.
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

package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLError(t *testing.T) {
	e := NewErrf(ErrNoDB, "no db error", nil)
	require.Greater(t, len(e.Error()), 0)

	e = NewErrf(0, "customized error", nil)
	require.Greater(t, len(e.Error()), 0)

	e = NewErr(ErrNoDB)
	require.Greater(t, len(e.Error()), 0)

	e = NewErr(0, "customized error", nil)
	require.Greater(t, len(e.Error()), 0)
}

func TestGISErrors(t *testing.T) {
	// Each GIS error has the SQLSTATE MySQL 9.7.2 reports for it
	// (performance_schema.events_errors_summary_global_by_error) and MySQL 9.7.2's
	// message for the same arguments, as MySQL raised it, except 3055, 3672 and 3673,
	// rendered from MySQL's formats. MySQL cuts a user string at the precision of its
	// format, 192 characters for an option key or value.
	long := strings.Repeat("abcdefghij", 30)
	cut := long[:192]
	for code, want := range map[uint16]struct {
		state string
		args  []any
		msg   string
	}{
		ErrGISDifferentSRIDs: {"HY000", []any{"st_distance", 0, 4326},
			"Binary geometry function st_distance given two geometries of different srids: 0 and 4326, which should have been identical."},
		ErrGISInvalidData: {"22023", []any{"st_astext"},
			"Invalid GIS data provided to function st_astext."},
		ErrStdOverflow: {"HY000", []any{"INFINITY", "st_distance_sphere"},
			"Overflow error: INFINITY in function st_distance_sphere."},
		ErrGISDataWrongEndianess: {"HY000", nil,
			"Geometry byte string must be little endian."},
		ErrInvalidGeoJSONMissingMember: {"HY000", []any{"st_geomfromgeojson", "coordinates"},
			"Invalid GeoJSON data provided to function st_geomfromgeojson: Missing required member 'coordinates'"},
		ErrInvalidGeoJSONWrongType: {"HY000", []any{"st_geomfromgeojson", "type", "string"},
			"Invalid GeoJSON data provided to function st_geomfromgeojson: Member 'type' must be of type 'string'"},
		ErrInvalidGeoJSONUnspecified: {"HY000", []any{"st_geomfromgeojson"},
			"Invalid GeoJSON data provided to function st_geomfromgeojson"},
		ErrDimensionUnsupported: {"HY000", []any{"st_geomfromgeojson", 3, 2},
			"Unsupported number of coordinate dimensions in function st_geomfromgeojson: Found 3, expected 2"},
		ErrUnexpectedGeometryType: {"22S01", []any{"POINT", "LINESTRING", "st_x"},
			"POINT value is a geometry of unexpected type LINESTRING in st_x."},
		ErrInvalidOptionKey: {"22023", []any{long, "st_geomfromtext"},
			"Invalid option key '" + cut + "' in function st_geomfromtext."},
		ErrInvalidOptionValue: {"22023", []any{long, "axis-order", "st_geomfromtext"},
			"Invalid value '" + cut + "' for option 'axis-order' in function 'st_geomfromtext'."},
		ErrInvalidOptionKeyValuePair: {"22023", []any{long, "=", "st_geomfromtext"},
			"The string '" + cut + "' is not a valid key = value pair in function st_geomfromtext."},
		ErrInvalidOptionStartCharacter: {"22023", []any{"st_geomfromtext", ","},
			"The options argument in function st_geomfromtext starts with the invalid character ','."},
		ErrInvalidOptionEndCharacter: {"22023", []any{"st_geomfromtext", ","},
			"The options argument in function st_geomfromtext ends with the invalid character ','."},
		ErrInvalidOptionCharacters: {"22023", []any{"st_geomfromtext", "=="},
			"The options argument in function st_geomfromtext contains the invalid character sequence '=='."},
		// MySQL 9.7.2 misspells "function" in this message.
		ErrDuplicateOptionKey: {"22023", []any{"axis-order", "st_geomfromtext"},
			"Duplicate option key 'axis-order' in function 'st_geomfromtext'."},
		ErrLongitudeOutOfRange: {"22S02", []any{200.0, "st_geomfromtext", -180.0, 180.0},
			"Longitude 200.000000 is out of range in function st_geomfromtext. It must be within (-180.000000, 180.000000]."},
		ErrLatitudeOutOfRange: {"22S03", []any{100.0, "st_geomfromtext", -90.0, 90.0},
			"Latitude 100.000000 is out of range in function st_geomfromtext. It must be within [-90.000000, 90.000000]."},
		ErrNotImplementedForGeographicSRS: {"22S00", []any{"st_centroid", "POLYGON"},
			"st_centroid(POLYGON) has not been implemented for geographic spatial reference systems."},
		ErrWrongSRIDForColumn: {"HY000", []any{"g", 0, 4326},
			"The SRID of the geometry does not match the SRID of the column 'g'. The SRID of the geometry is 0, but the SRID of the column is 4326. Consider changing the SRID of the geometry or the SRID property of the column."},
		ErrInvalidGeoJSONCRSNotTopLevel: {"HY000", []any{"st_geomfromgeojson"},
			"Invalid GeoJSON data provided to function st_geomfromgeojson: Member 'crs' must be specified in the top level object."},
		ErrBadNullNotIgnored: {"23000", []any{"g"},
			"Column 'g' cannot be null"},
		ErrNotImplementedForCartesianSRS: {"22S00", []any{"st_distance_sphere", "LINESTRING, POINT"},
			"st_distance_sphere(LINESTRING, POINT) has not been implemented for Cartesian spatial reference systems."},
		ErrNonPositiveRadius: {"22003", []any{"st_distance_sphere"},
			"Invalid radius provided to function st_distance_sphere: Radius must be greater than zero."},
		ErrSRSNotGeographic: {"22S00", []any{"st_latitude", 0},
			"Function st_latitude is only defined for geographic spatial reference systems, but one of its arguments is in SRID 0, which is not geographic."},
		ErrGeometryParamLongitudeOutOfRange: {"22S02", []any{"st_distance", 200.0, -180.0, 180.0},
			"A parameter of function st_distance contains a geometry with longitude 200.000000, which is out of range. It must be within (-180.000000, 180.000000]."},
		ErrGeometryParamLatitudeOutOfRange: {"22S03", []any{"st_distance", 200.0, -90.0, 90.0},
			"A parameter of function st_distance contains a geometry with latitude 200.000000, which is out of range. It must be within [-90.000000, 90.000000]."},
	} {
		e := NewErr(code, want.args...)
		require.Equal(t, want.state, e.State, "error %d", code)
		require.Equal(t, want.msg, e.Message, "error %d", code)
	}
}
