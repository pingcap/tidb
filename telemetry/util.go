// Copyright 2020 PingCAP, Inc.
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

package telemetry

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// hashString returns the SHA1 checksum in hex of the string.
func hashString(text string) string {
	hash := sha1.New()
	hash.Write([]byte(text))
	hashed := hash.Sum(nil)
	return fmt.Sprintf("%x", hashed)
}

// parseAddressAndHash parses an address in HOST:PORT format, returns the hashed host and the port.
func parseAddressAndHash(address string) (string, string) {
	var host, port string
	if !strings.Contains(address, ":") {
		host = address
		port = ""
	} else {
		parts := strings.Split(address, ":")
		lastPart := parts[len(parts)-1]
		if _, err := strconv.Atoi(lastPart); err != nil {
			// Ensure that all plaintext part (i.e. port) will never contain sensitive data.
			// The port part is not int, recognize all as host.
			host = address
			port = ""
		} else {
			host = strings.Join(parts[:len(parts)-1], ":")
			port = lastPart
		}
	}

	return hashString(host), port
}

// See https://stackoverflow.com/a/58026884
func sortedStringContains(s []string, searchTerm string) bool {
	i := sort.SearchStrings(s, searchTerm)
	return i < len(s) && s[i] == searchTerm
}
