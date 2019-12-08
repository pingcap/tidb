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

package cli

import (
	"fmt"
)

var (
	// ReleaseVersion is the current program version.
	ReleaseVersion = "Unknown"
	// BuildTimestamp is the UTC date time when the program is compiled.
	BuildTimestamp = "Unknown"
	// GitHash is the git commit hash when the program is compiled.
	GitHash = "Unknown"
	// GitBranch is the active git branch when the program is compiled.
	GitBranch = "Unknown"
	// GoVersion is the Go compiler version used to compile this program.
	GoVersion = "Unknown"
)

// LongVersion returns the version information of this program as a string.
func LongVersion() string {
	return fmt.Sprintf(
		"Release version: %s\n"+
			"Git commit hash: %s\n"+
			"Git branch:      %s\n"+
			"Build timestamp: %sZ\n"+
			"Go version:      %s\n",
		ReleaseVersion,
		GitHash,
		GitBranch,
		BuildTimestamp,
		GoVersion,
	)
}
