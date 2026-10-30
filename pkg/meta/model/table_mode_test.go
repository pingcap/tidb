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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableModeCanTransitionTo(t *testing.T) {
	tests := []struct {
		name   string
		from   TableMode
		to     TableMode
		expect bool
	}{
		{name: "normal to normal", from: TableModeNormal, to: TableModeNormal, expect: true},
		{name: "normal to import", from: TableModeNormal, to: TableModeImport, expect: true},
		{name: "normal to restore", from: TableModeNormal, to: TableModeRestore, expect: true},
		{name: "import to normal", from: TableModeImport, to: TableModeNormal, expect: true},
		{name: "import to import", from: TableModeImport, to: TableModeImport, expect: true},
		{name: "import to restore", from: TableModeImport, to: TableModeRestore, expect: false},
		{name: "restore to normal", from: TableModeRestore, to: TableModeNormal, expect: true},
		{name: "restore to import", from: TableModeRestore, to: TableModeImport, expect: false},
		{name: "restore to restore", from: TableModeRestore, to: TableModeRestore, expect: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, tt.from.CanTransitionTo(tt.to))
		})
	}
}
