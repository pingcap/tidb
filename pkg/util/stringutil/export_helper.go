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

package stringutil

// ExportedUnquote exports Unquote for testing
func ExportedUnquote(s string) (t string, err error) {
	return Unquote(s)
}

// ExportedCompilePattern exports CompilePattern for testing
func ExportedCompilePattern(pattern string, escape byte) (patWeights []rune, patTypes []byte) {
	return CompilePattern(pattern, escape)
}

// ExportedDoMatch exports DoMatch for testing
func ExportedDoMatch(str string, patChars []rune, patTypes []byte) bool {
	return DoMatch(str, patChars, patTypes)
}

// ExportedCompileLike2Regexp exports CompileLike2Regexp for testing
func ExportedCompileLike2Regexp(str string) string {
	return CompileLike2Regexp(str)
}

// ExportedIsExactMatch exports IsExactMatch for testing
func ExportedIsExactMatch(patTypes []byte) bool {
	return IsExactMatch(patTypes)
}

// ExportedBuildStringFromLabels exports BuildStringFromLabels for testing
func ExportedBuildStringFromLabels(labels map[string]string) string {
	return BuildStringFromLabels(labels)
}

// ExportedEscapeGlobQuestionMark exports EscapeGlobQuestionMark for testing
func ExportedEscapeGlobQuestionMark(s string) string {
	return EscapeGlobQuestionMark(s)
}

// ExportedMemoizeStr exports MemoizeStr for testing
func ExportedMemoizeStr(l func() string) interface {
	String() string
} {
	return MemoizeStr(l)
}
