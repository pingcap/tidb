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

func TestVersionComparsion(t *testing.T) {
	// test normal cases
	cases := []struct {
		rawVersion     string
		ge, gt, lt, le bool
	}{
		{"0.0.0", false, false, true, true},
		{"5.5.0", false, false, true, true},
		{"5.6.0", true, false, true, true},
		{"5.7.0", true, true, true, true},
		{"5.8.0", true, true, true, true}, // although it does not exist
		{"8.0.1", true, true, true, true},
		{"255.255.255", true, true, false, true}, // max version
	}

	var (
		version MySQLVersion
		err     error
	)
	for _, cs := range cases {
		version, err = toMySQLVersion(cs.rawVersion)
		require.NoError(t, err)

		require.Equal(t, cs.ge, version.Ge(SupportedVersion["mysql"].Min))
		require.Equal(t, cs.gt, version.Gt(SupportedVersion["mysql"].Min))
		require.Equal(t, cs.lt, version.Lt(SupportedVersion["mysql"].Max))
		require.Equal(t, cs.le, version.Le(SupportedVersion["mysql"].Max))
	}

	require.Equal(t, false, version.Lt(SupportedVersion["mariadb"].Max))
	require.Equal(t, true, version.Le(SupportedVersion["mariadb"].Max))
}

func TestToVersion(t *testing.T) {
	// test normal cases
	cases := []struct {
		rawVersion      string
		expectedVersion MySQLVersion
		hasError        bool
	}{
		{"", MinVersion, true},
		{"1.2.3.4", MinVersion, true},
		{"1.x.3", MySQLVersion{1, 0, 0}, true},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.5.50-MariaDB-1~wheezy", MySQLVersion{5, 5, 50}, false},
		{"5.7.19-17-log", MySQLVersion{5, 7, 19}, false},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.7.16-log", MySQLVersion{5, 7, 16}, false},
	}

	for _, cs := range cases {
		version, err := toMySQLVersion(cs.rawVersion)
		require.Equal(t, cs.expectedVersion, version)
		require.Equal(t, cs.hasError, err != nil)
	}
}

func TestIsMariaDB(t *testing.T) {
	require.True(t, IsMariaDB("5.5.50-MariaDB-1~wheezy"))
	require.False(t, IsMariaDB("5.7.19-17-log"))
}
