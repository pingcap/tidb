// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"encoding/hex"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func setFromString(value string) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert,Update", "Update") returns "Select,Insert,Update".
func addToSet(set []string, value string) []string {
	for _, v := range set {
		if v == value {
			return set
		}
	}
	return append(set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set []string, value string) []string {
	for i, v := range set {
		if v == value {
			copy(set[i:], set[i+1:])
			return set[:len(set)-1]
		}
	}
	return set
}

const (
	resourceGroupTagPrefixSQLDigest = byte(1)
)

func EncodeResourceGroupTag(sqlDigest string) []byte {
	if len(sqlDigest) == 0 {
		return nil
	}
	if len(sqlDigest) >= 512 {
		logutil.BgLogger().Warn("failed to encode sql digest to resource group tag: length too long", zap.String("sqlDigest", sqlDigest))
		return nil
	}

	res := make([]byte, 3+len(sqlDigest)/2)
	const encodingVersion = 1
	res[0] = encodingVersion
	res[1] = resourceGroupTagPrefixSQLDigest
	res[2] = byte(len(sqlDigest) / 2)
	_, err := hex.Decode(res[3:], []byte(sqlDigest))
	if err != nil {
		logutil.BgLogger().Warn("failed to encode sql digest to resource group tag: invalid hex string", zap.String("sqlDigest", sqlDigest))
		return nil
	}

	return res
}

func DecodeResourceGroupTag(data []byte) (sqlDigest string, err error) {
	if len(data) == 0 {
		return "", nil
	}

	encodingVersion := data[0]
	if encodingVersion != 1 {
		return "", errors.Errorf("unsupported resource group tag version %v", data[0])
	}
	rem := data[1:]

	for len(rem) > 0 {
		switch rem[0] {
		case resourceGroupTagPrefixSQLDigest:
			// There must be one more byte at rem[1] to represent the content's length, and the remaining bytes should
			// not be shorter than the length specified by rem[1].
			if len(rem) < 2 || len(rem)-2 < int(rem[1]) {
				return "", errors.Errorf("cannot parse resource group tag: field length mismatch, tag: %v", hex.EncodeToString(data))
			}
			fieldLen := int(rem[1])
			sqlDigest = hex.EncodeToString(rem[2 : 2+fieldLen])
			rem = rem[2+fieldLen:]
		default:
			return "", errors.Errorf("resource group tag field not recognized, prefix: %v, tag: %v", rem[0], hex.EncodeToString(data))
		}
	}

	return
}
