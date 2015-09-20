package localstore

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

var Tombstone = []byte{'\xde', '\xad'}

func IsTombstone(v []byte) bool {
	return bytes.Compare(v, Tombstone) == 0
}

func MvccEncodeVersionKey(key kv.Key, ver kv.Version) kv.EncodedKey {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver.Ver)
	return ret
}

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
