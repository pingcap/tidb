// Copyright 2016 PingCAP, Inc.
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

// Package ast is the abstract syntax tree parsed from a SQL statement by parser.
// It can be analysed and transformed by optimizer.

// Package kvrpc provides rpc for kvserver
package kvrpc

import (
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// RPCMsgHeader public header of all message
type RPCMsgHeader struct {
	MsgMagic   uint16
	MsgVersion uint16
	MsgLen     uint32
	MsgID      uint64
}

var (
	// MsgMagic a fix identifier of message
	MsgMagic uint16 = 0xdaf4
	// MsgVersionV1 message version 1
	MsgVersionV1 uint16 = 1
	_msgHeader          = RPCMsgHeader{}
)

// MsgHeaderSize sizeof(RPCMsgHeader)
// Check this value if migrate other OS, like Windows.
const MsgHeaderSize int = int(unsafe.Sizeof(_msgHeader))

// EncodeMessage Encodes message with message ID and protobuf body.
// Message contains header + payload.
// Header is 16 bytes, format:
//  | 0xdaf4 (2 bytes)| 0x01 (version 2 bytes)| msg_len (4 bytes) |
//  |                      msg_id (8 bytes)                       |
// All use bigendian.
// Payload is a protobuf message.
func EncodeMessage(w io.Writer, msgID uint64, m proto.Message) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	// Construct tikv protocol.
	var bufLen = uint32(len(buf))
	sendBuf := new(bytes.Buffer)
	binary.Write(sendBuf, binary.BigEndian, MsgMagic)
	binary.Write(sendBuf, binary.BigEndian, MsgVersionV1)
	binary.Write(sendBuf, binary.BigEndian, bufLen)
	binary.Write(sendBuf, binary.BigEndian, msgID)
	binary.Write(sendBuf, binary.BigEndian, buf)

	w.Write(sendBuf.Bytes())
	return nil
}

func decodeMsgHeader(r io.Reader) (msgID uint64, payloadLen uint32, err error) {
	buf := make([]byte, MsgHeaderSize)
	n, err := r.Read(buf)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if n < MsgHeaderSize {
		return 0, 0, errors.Errorf("expect message size at least %d instead of %d",
			MsgHeaderSize, n)
	}
	recvBuf := bytes.NewReader(buf)
	var msgHeader RPCMsgHeader
	binary.Read(recvBuf, binary.BigEndian, &msgHeader)
	if MsgMagic != msgHeader.MsgMagic {
		return 0, 0, errors.Errorf("expect message magic at least %04x instead of %04x",
			MsgMagic, msgHeader.MsgMagic)
	}
	if MsgVersionV1 != msgHeader.MsgVersion {
		return 0, 0, errors.Errorf("message version dismatch expected %d instead of %d",
			MsgVersionV1, msgHeader.MsgVersion)
	}
	return msgHeader.MsgID, msgHeader.MsgLen, nil
}

func decodeMsgBody(r io.Reader, payloadLen uint32, m proto.Message) error {
	buf := make([]byte, payloadLen)
	n, err := r.Read(buf)
	if err != nil {
		return errors.Trace(err)
	}
	if n != int(payloadLen) {
		log.Warnf("Read size [%d] mismatch expected size [%d]", n, payloadLen)
	}
	err = proto.Unmarshal(buf, m)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DecodeMessage Decodes message with above encoded format, returns message ID
// and fill proto message.
func DecodeMessage(r io.Reader, m proto.Message) (uint64, error) {
	msgID, payloadLen, err := decodeMsgHeader(r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = decodeMsgBody(r, payloadLen, m)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return msgID, nil
}
