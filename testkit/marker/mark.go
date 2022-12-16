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

package marker

import (
	"testing"
)

type markType int

const (
	// Feature marks a feature
	Feature markType = iota
	// Issue marks a fix for an issue
	Issue

	// NoID is used to mark a feature or issue without ID
	NoID = "noid"
)

// As marks feature or issue information, the args must be basic a string literal: https://go.dev/ref/spec#String_literals
//
// For example, used to mark a feature:
//
//	marker.As(t, marker.Feature, "FD-231", "GBK Support")
//
// If the feature has no a Jira FD ID, `maker.NoID` can be used:
//
//	marker.As(t, marker.Feature, marker.NoID)
//
// If the testcase is for covering a bug issue, please use `marker.Issue`:
//
//	marker.As(t, marker.Issue, 39688)
func As(t *testing.T, marktype markType, args ...any) {}
