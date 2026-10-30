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

package task

import (
	"encoding/json"
	"net/url"
	"slices"
	"strings"
)

// storageURLForRestoreHash freezes the legacy ast.RedactURL representation used
// by persisted restore checkpoints. Do not replace it with a logging redactor:
// stricter log policies must not change checkpoint identity. Recognized credential
// values are masked, but their query keys remain part of the identity.
// The result can still contain secrets in other query parameters or fragments
// and must never be logged. The literals and URL serialization are also part of
// the checkpoint format, including historical handling of malformed URLs.
func storageURLForRestoreHash(rawURL string) string {
	if !strings.Contains(rawURL, "://") && !strings.Contains(rawURL, "?") {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "(invalid URL)"
	}
	u.User = nil
	var credentialKeys map[string]struct{}
	switch strings.ToLower(u.Scheme) {
	case "s3", "ks3", "oss":
		credentialKeys = map[string]struct{}{
			"access-key":        {},
			"secret-access-key": {},
			"session-token":     {},
			"sse-kms-context":   {},
		}
	case "azure", "azblob":
		credentialKeys = map[string]struct{}{
			"account-key":    {},
			"encryption-key": {},
			"sas-token":      {},
			"endpoint":       {},
		}
	}
	if len(credentialKeys) > 0 {
		u.Fragment = ""
		u.RawFragment = ""
		values, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			u.RawQuery = ""
			return u.String()
		}
		for key, vs := range values {
			normalizedKey := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
			if _, ok := credentialKeys[normalizedKey]; ok {
				// Preserve the legacy treatment of split encryption contexts.
				if normalizedKey == "sse-kms-context" && slices.ContainsFunc(vs, func(v string) bool {
					return !json.Valid([]byte(v))
				}) {
					u.RawQuery = ""
					return u.String()
				}
				values[key] = []string{"xxxxxx"}
			}
		}
		u.RawQuery = values.Encode()
	}
	return u.String()
}
