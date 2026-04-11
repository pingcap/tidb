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

package mega

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/mega/register"
)

var (
	testRunPattern = flag.String("mega.run", "", "Pattern for mega tests to run (e.g., 'ddl/*', '*/GetTimeZone')")
	testList       = flag.Bool("mega.list", false, "List all registered tests and exit")
)

// RunMega runs all registered tests from various packages.
// This test serves as the entry point for monolithic testing,
// where tests from multiple packages are compiled together
// and run in a single binary to reduce link time.
func RunMega(t *testing.T) {
	// Handle -mega.list flag
	if *testList {
		listTests(t)
		return
	}

	store := testkit.CreateMockStore(t)
	defer store.Close()

	t.Log("Mega test framework initialized")

	if *testRunPattern != "" {
		t.Log("Running tests matching pattern:", *testRunPattern)
		register.GlobalRegistry().RunByPattern(t, *testRunPattern)
	} else {
		t.Log("Running all registered tests")
		register.GlobalRegistry().RunAll(t)
	}
}

// listTests prints all registered tests in a formatted way.
func listTests(t *testing.T) {
	fmt.Println("=== Mega Test Registry ===")
	allTests := register.GlobalRegistry().ListAll()
	fmt.Printf("Total tests: %d\n", len(allTests))
	fmt.Println()

	testsByPkg := make(map[string][]string)
	for _, testFullName := range allTests {
		// The format from registry is "pkg/name" where pkg can contain slashes
		// We need to find the last slash to separate pkg from test name
		lastSlashIndex := strings.LastIndex(testFullName, "/")
		if lastSlashIndex > 0 {
			pkg := testFullName[:lastSlashIndex]
			name := testFullName[lastSlashIndex+1:]
			testsByPkg[pkg] = append(testsByPkg[pkg], name)
		}
	}

	// Sort packages alphabetically
	packages := make([]string, 0, len(testsByPkg))
	for pkg := range testsByPkg {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	for _, pkg := range packages {
		tests := testsByPkg[pkg]
		sort.Strings(tests)
		fmt.Printf("Package %s (%d tests):\n", pkg, len(tests))
		for _, test := range tests {
			fmt.Printf("  - %s/%s\n", pkg, test)
		}
		fmt.Println()
	}
	os.Exit(0)
}
