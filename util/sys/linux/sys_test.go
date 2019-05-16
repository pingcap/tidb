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
package linux_test

import (
	"testing"

	"github.com/pingcap/tidb/util/sys/linux"
)

func TestGetOSVersion(t *testing.T) {
	osRelease, err := linux.OSVersion()
	if err != nil {
		t.Fatal(t)
	}
	if len(osRelease) == 0 {
		t.Fatalf("counld not get os version")
	}
}
