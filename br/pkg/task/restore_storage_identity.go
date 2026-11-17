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
	"net/url"
	"strings"
)

// storageURLForRestoreHash freezes release-8.5's legacy ast.RedactURL
// representation used by persisted restore checkpoints. Do not replace it with
// a logging redactor: stricter log policies must not change checkpoint identity.
// This value can contain secrets and must never be logged.
func storageURLForRestoreHash(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	var credentialKeys map[string]struct{}
	switch strings.ToLower(u.Scheme) {
	case "s3", "ks3":
		credentialKeys = map[string]struct{}{
			"access-key":        {},
			"secret-access-key": {},
			"session-token":     {},
		}
	case "azure", "azblob":
		credentialKeys = map[string]struct{}{"sas-token": {}}
	}
	if len(credentialKeys) > 0 {
		values := u.Query()
		for key := range values {
			normalizedKey := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
			if _, ok := credentialKeys[normalizedKey]; ok {
				values[key] = []string{"xxxxxx"}
			}
		}
		u.RawQuery = values.Encode()
	}
	return u.String()
}
