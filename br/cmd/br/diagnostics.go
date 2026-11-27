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

package main

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/objstore"
)

var diagnosticURLStart = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://`)

// redactDiagnosticErrorText preserves context before the first URL and suppresses
// the remainder when its boundaries cannot be distinguished from credentials.
func redactDiagnosticErrorText(text string) string {
	start := diagnosticURLStart.FindStringIndex(text)
	if start == nil {
		return text
	}
	rawURL := text[start[0]:]
	// Error messages do not define URL boundaries. Quotes, spaces, or nested
	// URLs can be part of credentials. A truncated sample can also make partial
	// userinfo look like a host. Suppress these uncertain remainders rather than
	// exposing credentials after a guessed boundary.
	if strings.HasSuffix(rawURL, "...") || strings.ContainsAny(rawURL, " \t\r\n\"'`") ||
		diagnosticURLStart.MatchString(rawURL[start[1]-start[0]:]) {
		return text[:start[0]] + "(storage URL and following text redacted)"
	}
	// An authority delimiter before the last '@' can be an unescaped part of
	// a password. Parsing would treat its prefix as a host and retain it.
	if at := strings.LastIndex(rawURL, "@"); at >= 0 &&
		strings.ContainsAny(rawURL[start[1]-start[0]:at], "/?#") {
		return text[:start[0]] + "(storage URL and following text redacted)"
	}
	redactedURL := objstore.RedactURL(rawURL)
	// Query values can contain percent-encoded endpoint URLs with their own
	// credentials. Drop queries and fragments entirely instead of guessing how
	// many times nested values must be decoded before they are safe to report.
	if suffix := strings.IndexAny(redactedURL, "?#"); suffix >= 0 {
		redactedURL = redactedURL[:suffix]
	}
	return text[:start[0]] + redactedURL
}
