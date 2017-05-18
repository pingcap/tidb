package util

import (
	"encoding/binary"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
)

const (
	msgHeaderSize        = 16
	msgVersion    uint16 = 1
	msgMagic      uint16 = 0xdaf4
)

// The RPC format is header + protocol buffer body
// Header is 16 bytes, format:
//  | 0xdaf4(2 bytes magic value) | 0x01(version 2 bytes) | msg_len(4 bytes) | msg_id(8 bytes) |,
// all use bigendian.

// WriteMessage writes a protocol buffer message to writer.
func WriteMessage(w io.Writer, msgID uint64, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}

	var header [msgHeaderSize]byte
	binary.BigEndian.PutUint16(header[0:2], msgMagic)
	binary.BigEndian.PutUint16(header[2:4], msgVersion)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	binary.BigEndian.PutUint64(header[8:16], msgID)

	if _, err = w.Write(header[:]); err != nil {
		return errors.Trace(err)
	}

	_, err = w.Write(body)
	return errors.Trace(err)
}

// ReadMessage reads a protocol buffer message from reader.
func ReadMessage(r io.Reader, msg proto.Message) (uint64, error) {
	msgID, msgLen, err := ReadHeader(r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	body := make([]byte, msgLen)
	_, err = io.ReadFull(r, body)
	if err != nil {
		return 0, errors.Trace(err)
	}

	err = proto.Unmarshal(body, msg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return msgID, nil
}

// ReadHeader reads a protocol header from reader.
func ReadHeader(r io.Reader) (msgID uint64, msgLen uint32, err error) {
	var header [msgHeaderSize]byte
	_, err = io.ReadFull(r, header[:])
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	if magic := binary.BigEndian.Uint16(header[0:2]); magic != msgMagic {
		return 0, 0, errors.Errorf("mismatch header magic %x != %x", magic, msgMagic)
	}

	// Skip version now.
	// TODO: check version.

	msgLen = binary.BigEndian.Uint32(header[4:8])
	msgID = binary.BigEndian.Uint64(header[8:])
	return
}
