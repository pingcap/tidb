package uuid

/****************
 * Date: 15/02/14
 * Time: 12:26 PM
 ***************/

import (
	"testing"
)

var struct_bytes = []byte{
	0xAA, 0xCF, 0xEE, 0x12,
	0xD4, 0x00,
	0x27, 0x23,
	0x00,
	0xD3,
	0x23, 0x12, 0x4A, 0x11, 0x89, 0xFF,
}

func TestUUID_Struct_UnmarshalBinary(t *testing.T) {
	u := new(Struct)
	u.size = length
	err := u.UnmarshalBinary([]byte{1, 2, 3, 4, 5})
	if err == nil {
		t.Errorf("Expected error due to invalid byte length")
	}
	err = u.UnmarshalBinary(struct_bytes)
	if err != nil {
		t.Errorf("Expected bytes")
	}
}
