package localstore

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

var tombstone = []byte{'\xde', '\xad'}

func isTombstone(v []byte) bool {
	return bytes.Compare(v, tombstone) == 0
}

// MvccEncodeVersionKey returns the encoded key
func MvccEncodeVersionKey(key kv.Key, ver kv.Version) kv.EncodedKey {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver.Ver)
	return ret
}

// MvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key
func MvccDecode(encodedKey kv.EncodedKey) (kv.Key, kv.Version, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes([]byte(encodedKey))
	if err != nil {
		// should never happen
		return nil, kv.Version{}, errors.Trace(err)
	}
	// if it's meta key
	if len(remainBytes) == 0 {
		return key, kv.Version{}, nil
	}
	var ver uint64
	remainBytes, ver, err = codec.DecodeUintDesc(remainBytes)
	if err != nil {
		// should never happen
		return nil, kv.Version{}, errors.Trace(err)
	}
	if len(remainBytes) != 0 {
		return nil, kv.Version{}, errors.New("invalid encoded key")
	}
	return key, kv.Version{ver}, nil
}
