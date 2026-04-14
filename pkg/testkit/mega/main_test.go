// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS, ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mega

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Ensure flags are parsed so we can check -ut before m.Run().
	// m.Run() calls flag.Parse() internally, but flag.Parse() is idempotent
	// after the first call, so calling it here is safe.
	flag.Parse()

	// When -ut is set, run as orchestrator: list tests and spawn subprocesses.
	// This happens before m.Run() so no test functions execute in this process.
	if *flagUT {
		RunUT()
		// RunUT calls os.Exit, never reaches here
	}
	os.Exit(m.Run())
}

func TestMega(t *testing.T) {
	RunMega(t)
}
