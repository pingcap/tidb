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

package context

// Exported types for testing

// ExportedSQLWarn exports SQLWarn for testing
type ExportedSQLWarn = SQLWarn

// ExportedWarnLevelError gets the WarnLevelError for testing
func ExportedWarnLevelError() string {
	return WarnLevelError
}

// ExportedWarnLevelWarning gets the WarnLevelWarning for testing
func ExportedWarnLevelWarning() string {
	return WarnLevelWarning
}

// ExportedWarnLevelNote gets the WarnLevelNote for testing
func ExportedWarnLevelNote() string {
	return WarnLevelNote
}

// ExportedIgnoreWarn gets the IgnoreWarn for testing
func ExportedIgnoreWarn() WarnHandler {
	return IgnoreWarn
}

// ExportedNewStaticWarnHandler creates a new StaticWarnHandler for testing
func ExportedNewStaticWarnHandler(capacity int) *StaticWarnHandler {
	return NewStaticWarnHandler(capacity)
}

// ExportedNewStaticWarnHandlerWithHandler creates a new StaticWarnHandlerWithHandler for testing
func ExportedNewStaticWarnHandlerWithHandler(handler WarnHandler) *StaticWarnHandler {
	return NewStaticWarnHandlerWithHandler(handler)
}

// ExportedGetStaticWarnHandlerWarnings gets the warnings field from a StaticWarnHandler for testing
func ExportedGetStaticWarnHandlerWarnings(h *StaticWarnHandler) []SQLWarn {
	return h.warnings
}
