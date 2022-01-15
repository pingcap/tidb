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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2018 PingCAP, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package timeutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTZNameFromFileName(t *testing.T) {
	tz, err := inferTZNameFromFileName("/usr/share/zoneinfo/Asia/Shanghai")

	require.NoError(t, err)
	require.Equal(t, "Asia/Shanghai", tz)

	tz, err = inferTZNameFromFileName("/usr/share/zoneinfo.default/Asia/Shanghai")

	require.NoError(t, err)
	require.Equal(t, "Asia/Shanghai", tz)
}

func TestLocal(t *testing.T) {
	os.Setenv("TZ", "Asia/Shanghai")
	systemTZ.Store(InferSystemTZ())
	loc := SystemLocation()
	require.Equal(t, "Asia/Shanghai", systemTZ.Load())
	require.Equal(t, "Asia/Shanghai", loc.String())

	os.Setenv("TZ", "UTC")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	require.Equal(t, "UTC", loc.String())

	os.Setenv("TZ", "")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	require.Equal(t, "UTC", loc.String())
	os.Unsetenv("TZ")
}

func TestInferOneStepLinkForPath(t *testing.T) {
	os.Remove(filepath.Join(os.TempDir(), "testlink1"))
	os.Remove(filepath.Join(os.TempDir(), "testlink2"))
	os.Remove(filepath.Join(os.TempDir(), "testlink3"))
	var link2, link3 string
	var err error
	var link1 *os.File
	link1, err = os.Create(filepath.Join(os.TempDir(), "testlink1"))
	require.NoError(t, err)
	err = os.Symlink(link1.Name(), filepath.Join(os.TempDir(), "testlink2"))
	require.NoError(t, err)
	err = os.Symlink(filepath.Join(os.TempDir(), "testlink2"), filepath.Join(os.TempDir(), "testlink3"))
	require.NoError(t, err)
	link2, err = inferOneStepLinkForPath(filepath.Join(os.TempDir(), "testlink3"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(os.TempDir(), "testlink2"), link2)
	link3, err = filepath.EvalSymlinks(filepath.Join(os.TempDir(), "testlink3"))
	require.NoError(t, err)
	require.NotEqual(t, -1, strings.Index(link3, link1.Name()))
}
