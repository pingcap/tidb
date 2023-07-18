// Copyright 2022 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnParseType(t *testing.T) {
	col := Column{}

	col.ParseType(`varchar`)
	assert.Equal(t, col.Type, "varchar", "parse data type")
	assert.Equal(t, col.Length, 0, "parse data length")

	col.ParseType(`varchar(111)`)
	assert.Equal(t, col.Type, "varchar", "parse data type")
	assert.Equal(t, col.Length, 111, "parse data length")

	col.ParseType(`int(16)`)
	assert.Equal(t, col.Type, "int", "parse data type")
	assert.Equal(t, col.Length, 16, "parse data length")

	col.ParseType(`tinyint(2)`)
	assert.Equal(t, col.Type, "tinyint", "parse data type")
	assert.Equal(t, col.Length, 2, "parse data length")
}
