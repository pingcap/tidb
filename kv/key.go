package kv

import "bytes"

// Key represents high-level Key type.
type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	// add 0x0 to the end of key
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	return bytes.Compare(k, another)
}

// EncodedKey represents encoded key in low-level storage engine.
type EncodedKey []byte

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k EncodedKey) Cmp(another EncodedKey) int {
	return bytes.Compare(k, another)
}

// Next returns the next key in byte-order.
func (k EncodedKey) Next() EncodedKey {
	return EncodedKey(bytes.Join([][]byte{k, Key{0}}, nil))
}
