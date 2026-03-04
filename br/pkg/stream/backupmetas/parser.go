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

package backupmetas

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

const (
	legacyBackupMetaPartCount = 4
	taggedMetaTagValueLen     = 17

	NameMinBeginTSTag byte = 'd'
	NameMinTSTag      byte = 'l'
	NameMaxTSTag      byte = 'u'
)

var (
	legacyBackupMetaPattern = regexp.MustCompile(
		`^([0-9a-fA-F]{16})-([0-9a-fA-F]{16})-([0-9a-fA-F]{16})-([0-9a-fA-F]{16})$`)
	taggedBackupMetaPattern = regexp.MustCompile(`^[0-9a-fA-F]{32}-(?:[0-9A-Za-z][0-9a-fA-F]{16})+$`)
)

type ParsedName struct {
	FlushTS      uint64
	StoreID      uint64
	MinDefaultTS uint64
	MinTS        uint64
	MaxTS        uint64
}

func ParseName(fileName string) (ParsedName, error) {
	switch {
	case taggedBackupMetaPattern.MatchString(fileName):
		return parseTaggedBackupMetaFileName(fileName)
	case legacyBackupMetaPattern.MatchString(fileName):
		return parseLegacyBackupMetaFileName(fileName)
	default:
		return ParsedName{}, errors.Errorf("invalid backupmeta file name format: %s", fileName)
	}
}

func parseLegacyBackupMetaFileName(fileName string) (ParsedName, error) {
	parts := strings.Split(fileName, "-")
	if len(parts) != legacyBackupMetaPartCount {
		return ParsedName{}, errors.Errorf("invalid backupmeta legacy file name format: %s", fileName)
	}

	flushTs, err := parseBackupMetaHexU64(fileName, "flushTs", parts[0])
	if err != nil {
		return ParsedName{}, err
	}
	minDefaultTs, err := parseBackupMetaHexU64(fileName, "minDefaultTs", parts[1])
	if err != nil {
		return ParsedName{}, err
	}
	minTs, err := parseBackupMetaHexU64(fileName, "minTs", parts[2])
	if err != nil {
		return ParsedName{}, err
	}
	maxTs, err := parseBackupMetaHexU64(fileName, "maxTs", parts[3])
	if err != nil {
		return ParsedName{}, err
	}

	return ParsedName{
		FlushTS:      flushTs,
		MinDefaultTS: minDefaultTs,
		MinTS:        minTs,
		MaxTS:        maxTs,
	}, nil
}

func parseTaggedBackupMetaFileName(fileName string) (ParsedName, error) {
	prefix, suffix, ok := strings.Cut(fileName, "-")
	if !ok {
		return ParsedName{}, errors.Errorf("invalid backupmeta tagged file name format: %s", fileName)
	}
	if len(prefix) != 32 {
		return ParsedName{}, errors.Errorf("invalid backupmeta tagged prefix length in %s", fileName)
	}

	flushTs, err := parseBackupMetaHexU64(fileName, "flushTs", prefix[:16])
	if err != nil {
		return ParsedName{}, err
	}
	storeID, err := parseBackupMetaHexU64(fileName, "storeID", prefix[16:])
	if err != nil {
		return ParsedName{}, err
	}

	var (
		minDefaultTs uint64
		minTs        uint64
		maxTs        uint64
		seenTags     [256]bool
	)
	for pos := 0; pos < len(suffix); {
		remain := len(suffix) - pos
		if remain < taggedMetaTagValueLen {
			return ParsedName{}, errors.Errorf("incomplete tag segment in %s", fileName)
		}
		tag := suffix[pos]
		if !isASCIIAlphanumeric(tag) {
			return ParsedName{}, errors.Errorf("invalid suffix tag %q in %s", tag, fileName)
		}
		hexValue := suffix[pos+1 : pos+taggedMetaTagValueLen]
		value, err := parseBackupMetaHexU64(fileName, "tag value", hexValue)
		if err != nil {
			return ParsedName{}, err
		}
		if seenTags[tag] {
			return ParsedName{}, errors.Errorf("duplicate suffix tag %q in %s", tag, fileName)
		}
		seenTags[tag] = true
		switch tag {
		case NameMinBeginTSTag:
			minDefaultTs = value
		case NameMinTSTag:
			minTs = value
		case NameMaxTSTag:
			maxTs = value
		}
		pos += taggedMetaTagValueLen
	}

	required := []byte{NameMinBeginTSTag, NameMinTSTag, NameMaxTSTag}
	for _, tag := range required {
		if !seenTags[tag] {
			return ParsedName{}, errors.Errorf("missing %q tag in %s", tag, fileName)
		}
	}

	return ParsedName{
		FlushTS:      flushTs,
		StoreID:      storeID,
		MinDefaultTS: minDefaultTs,
		MinTS:        minTs,
		MaxTS:        maxTs,
	}, nil
}

func parseBackupMetaHexU64(fileName, partName, hexPart string) (uint64, error) {
	if len(hexPart) != 16 {
		return 0, errors.Errorf("%s must be 16 hex digits in %s", partName, fileName)
	}
	value, err := strconv.ParseUint(hexPart, 16, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "failed to parse %s in %s", partName, fileName)
	}
	return value, nil
}

func isASCIIAlphanumeric(ch byte) bool {
	return '0' <= ch && ch <= '9' || 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}
