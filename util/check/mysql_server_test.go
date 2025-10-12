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

package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMysqlVersion(t *testing.T) {
	var versionChecker = &MySQLVersionChecker{}

	cases := []struct {
		rawVersion string
		pass       bool
	}{
		{"5.5.0-log", false},
		{"5.6.0-log", true},
		{"5.7.0-log", true},
		{"5.8.0-log", true}, // although it does not exist
		{"8.0.1-log", true},
		{"8.0.20", true},
		{"5.5.50-MariaDB-1~wheezy", false},
		{"10.1.1-MariaDB-1~wheezy", false},
		{"10.1.2-MariaDB-1~wheezy", true},
		{"10.13.1-MariaDB-1~wheezy", true},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		versionChecker.checkVersion(cs.rawVersion, result)
		require.Equal(t, cs.pass, result.State == StateSuccess)
	}
}
