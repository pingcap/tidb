// Copyright 2024 PingCAP, Inc.
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

package format

import "io"

// ExportFormatter exports Formatter type for testing
type ExportFormatter = Formatter

// ExportOutputFormat exports OutputFormat function for testing
func ExportOutputFormat(s string) string {
	return OutputFormat(s)
}

// ExportIndentFormatter exports IndentFormatter function for testing
func ExportIndentFormatter(w io.Writer, indent string) Formatter {
	return IndentFormatter(w, indent)
}

// ExportFlatFormatter exports FlatFormatter function for testing
func ExportFlatFormatter(w io.Writer) Formatter {
	return FlatFormatter(w)
}
