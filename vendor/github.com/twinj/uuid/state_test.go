package uuid

/****************
 * Date: 14/02/14
 * Time: 9:08 PM
 ***************/

import (
	"bytes"
	"fmt"
	"net"
	"testing"
)

var state_bytes = []byte{
	0xAA, 0xCF, 0xEE, 0x12,
	0xD4, 0x00,
	0x27, 0x23,
	0x00,
	0xD3,
	0x23, 0x12, 0x4A, 0x11, 0x89, 0xFF,
}


func TestUUID_getHardwareAddress(t *testing.T) {
	intfcs, err := net.Interfaces()
	if err != nil {
		return
	}
	addr := getHardwareAddress(intfcs)
	if addr == nil {
		return
	}
	fmt.Println(addr)
}

func TestUUID_StateSeed(t *testing.T) {
	if state.past < Timestamp((1391463463*10000000)+(100*10)+gregorianToUNIXOffset) {
		t.Errorf("Expected a value greater than 02/03/2014 @ 9:37pm in UTC but got %d", state.past)
	}
	if state.node == nil {
		t.Errorf("Expected a non nil node")
	}
	if state.sequence <= 0 {
		t.Errorf("Expected a value greater than but got %d", state.sequence)
	}
}

func TestUUID_State_read(t *testing.T) {
	s := new(State)
	s.past = Timestamp((1391463463 * 10000000) + (100 * 10) + gregorianToUNIXOffset)
	s.node = state_bytes

	now := Timestamp((1391463463 * 10000000) + (100 * 10))
	s.read(now+(100*10), net.HardwareAddr(make([]byte, length)))
	if s.sequence != 1 {
		t.Error("The sequence should increment when the time is"+
			"older than the state past time and the node"+
			"id are not the same.", s.sequence)
	}
	s.read(now, net.HardwareAddr(state_bytes))

	if s.sequence == 1 {
		t.Error("The sequence should be randomly generated when"+
			" the nodes are equal.", s.sequence)
	}

	s = new(State)
	s.past = Timestamp((1391463463 * 10000000) + (100 * 10) + gregorianToUNIXOffset)
	s.node = state_bytes
	s.randomSequence = true
	s.read(now, net.HardwareAddr(make([]byte, length)))

	if s.sequence == 0 {
		t.Error("The sequence should be randomly generated when"+
			" the randomSequence flag is set.", s.sequence)
	}

	if s.past != now {
		t.Error("The past time should equal the time passed in" +
			" the method.")
	}

	if !bytes.Equal(s.node, make([]byte, length)) {
		t.Error("The node id should equal the node passed in" +
			" the method.")
	}
}

func TestUUID_State_init(t *testing.T) {

}
