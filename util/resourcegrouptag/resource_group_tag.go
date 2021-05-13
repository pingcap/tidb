package resourcegrouptag

import (
	"encoding/hex"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	resourceGroupTagPrefixSQLDigest = byte(1)
)

// EncodeResourceGroupTag encodes sqlDigest into resource group tag.
// A resource group tag can be carried in the Context field of TiKV requests, which is a byte array, and sent to TiKV as
// diagnostic information. Currently it contains only the SQL Digest, and the codec method is naive but extendable.
// This function doesn't return error. When there's some error, which can only be caused by unexpected format of the
// arguments, it simply returns an empty result.
// The format:
// +-----------+-----------------------+----------------------------+---------------+----------------+----
// | version=1 | field1 prefix (1byte) | field1 content (var bytes) | field2 prefix | field2 content | ...
// +-----------+-----------------------+----------------------------+---------------+----------------+----
// The `version` section marks the codec version, which makes it easier for changing the format in the future.
// Each field starts with a byte to mark what field it is, and the length of the content depends on the field's
// definition.
// Currently there's only one field (SQL Digest), and its content starts with a byte `B` describing it's length, and
// then follows by exactly `B` bytes.
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
	// The SQL Digest is expected to be a hex string. Convert it back to bytes to save half of the memory.
	res[2] = byte(len(sqlDigest) / 2)
	_, err := hex.Decode(res[3:], []byte(sqlDigest))
	if err != nil {
		logutil.BgLogger().Warn("failed to encode sql digest to resource group tag: invalid hex string", zap.String("sqlDigest", sqlDigest))
		return nil
	}

	return res
}

// DecodeResourceGroupTag decodes a resource group tag into various information contained in it. Currently it contains
// only the SQL Digest.
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
