// Copyright 2023 PingCAP, Inc.
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

package errno

import (
	_ "embed"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:embed errcode.go
var errCodeSrc string

func TestAllErrCodeHasMsg(t *testing.T) {
	lines := strings.Split(errCodeSrc, "\n")
	errCodes := make([]uint16, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "Err") {
			continue
		}
		codeStr := strings.TrimSpace(strings.Split(l, "=")[1])
		code, err := strconv.Atoi(codeStr)
		assert.NoErrorf(t, err, "parse code definition: %s", codeStr)
		errCodes = append(errCodes, uint16(code))
	}

	for _, code := range errCodes {
		_, ok := MySQLErrName[code]
		assert.Truef(t, ok, "ErrCode: %d is unknown", code)
	}
}

func TestReservedErrCodeRange(t *testing.T) {
	const reservedStart = 8800
	const reservedEnd = 8900

	for _, l := range strings.Split(errCodeSrc, "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "Err") {
			continue
		}

		parts := strings.Split(l, "=")
		assert.Len(t, parts, 2, "parse code definition: %s", l)

		errName := strings.TrimSpace(parts[0])
		codeStr := strings.TrimSpace(parts[1])
		code, err := strconv.Atoi(codeStr)
		assert.NoErrorf(t, err, "parse code definition: %s", codeStr)

		assert.Falsef(t, code >= reservedStart && code < reservedEnd,
			"%s must not be in reserved range [%d, %d), but got %d",
			errName, reservedStart, reservedEnd, code)
	}
}

func TestRedactArgsUseStringVerbs(t *testing.T) {
	// Redacted arguments are replaced with a string placeholder, so they must
	// be formatted with a verb that can print a string.
	verbRe := regexp.MustCompile(`%[-+# 0]*(?:[0-9]+|\*)?(?:\.(?:[0-9]+|\*))?([a-zA-Z%])`)
	for code, msg := range MySQLErrName {
		if len(msg.RedactArgPos) == 0 {
			continue
		}
		verbs := make([]string, 0, 4)
		for _, m := range verbRe.FindAllStringSubmatch(msg.Raw, -1) {
			if m[1] == "%" {
				continue
			}
			verbs = append(verbs, m[1])
		}
		for _, pos := range msg.RedactArgPos {
			if !assert.Lessf(t, pos, len(verbs), "ErrCode %d: redacted argument %d out of range in %q", code, pos, msg.Raw) {
				continue
			}
			assert.Containsf(t, []string{"s", "v", "q"}, verbs[pos],
				"ErrCode %d: redacted argument %d uses %%%s in %q", code, pos, verbs[pos], msg.Raw)
		}
	}
}
