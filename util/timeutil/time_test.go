// Copyright 2018 PingCAP, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package timeutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTZNameFromFileName(t *testing.T) {
	t.Parallel()

	tz, err := inferTZNameFromFileName("/usr/share/zoneinfo/Asia/Shanghai")

	assert.Nil(t, err)
	assert.Equal(t, "Asia/Shanghai", tz)

	tz, err = inferTZNameFromFileName("/usr/share/zoneinfo.default/Asia/Shanghai")

	assert.Nil(t, err)
	assert.Equal(t, "Asia/Shanghai", tz)
}

func TestLocal(t *testing.T) {
	t.Parallel()

	os.Setenv("TZ", "Asia/Shanghai")
	systemTZ.Store(InferSystemTZ())
	loc := SystemLocation()
	assert.Equal(t, "Asia/Shanghai", systemTZ.Load())
	assert.Equal(t, "Asia/Shanghai", loc.String())

	os.Setenv("TZ", "UTC")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	assert.Equal(t, "UTC", loc.String())

	os.Setenv("TZ", "")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	assert.Equal(t, "UTC", loc.String())
	os.Unsetenv("TZ")
}

func TestInferOneStepLinkForPath(t *testing.T) {
	t.Parallel()

	os.Remove(filepath.Join(os.TempDir(), "testlink1"))
	os.Remove(filepath.Join(os.TempDir(), "testlink2"))
	os.Remove(filepath.Join(os.TempDir(), "testlink3"))
	var link2, link3 string
	var err error
	var link1 *os.File
	link1, err = os.Create(filepath.Join(os.TempDir(), "testlink1"))
	assert.Nil(t, err)
	err = os.Symlink(link1.Name(), filepath.Join(os.TempDir(), "testlink2"))
	assert.Nil(t, err)
	err = os.Symlink(filepath.Join(os.TempDir(), "testlink2"), filepath.Join(os.TempDir(), "testlink3"))
	assert.Nil(t, err)
	link2, err = inferOneStepLinkForPath(filepath.Join(os.TempDir(), "testlink3"))
	assert.Nil(t, err)
	assert.Equal(t, filepath.Join(os.TempDir(), "testlink2"), link2)
	link3, err = filepath.EvalSymlinks(filepath.Join(os.TempDir(), "testlink3"))
	assert.Nil(t, err)
	assert.NotEqual(t, -1, strings.Index(link3, link1.Name()))
}
