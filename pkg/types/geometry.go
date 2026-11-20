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

package types

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ast "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// GeometryEnabled gates geometry columns; true only in test builds, a variable so tests
// can turn it off.
var GeometryEnabled = intest.InTest

// TypeToStrWithSubtype is TypeToStr, except that it names a geometry type by its subtype
// geo, such as point, as MySQL does in information_schema.COLUMNS.DATA_TYPE.
func TypeToStrWithSubtype(tp byte, cs string, geo ast.GeometryType) string {
	if tp == mysql.TypeGeometry {
		return geo.String()
	}
	return TypeToStr(tp, cs)
}
