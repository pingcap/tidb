package server

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/juju/errors"
	mysql "github.com/pingcap/tidb/mysqldef"
)

type packetIO struct {
	rb *bufio.Reader
	wb *bufio.Writer

	sequence uint8
}

func newPacketIO(conn net.Conn) *packetIO {
	p := &packetIO{
		rb: bufio.NewReaderSize(conn, 2048),
		wb: bufio.NewWriterSize(conn, 2048),
	}

	return p
}

func (p *packetIO) readPacket() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, errors.Trace(err)
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, errors.Trace(fmt.Errorf("invalid payload length %d", length))
	}

	sequence := uint8(header[3])
	if sequence != p.sequence {
		return nil, errors.Trace(fmt.Errorf("invalid sequence %d != %d", sequence, p.sequence))
	}

	p.sequence++

	data := make([]byte, length)
	if _, err := io.ReadFull(p.rb, data); err != nil {
		return nil, errors.Trace(err)
	}
	if length < mysql.MaxPayloadLen {
		return data, nil
	}

	var buf []byte
	buf, err := p.readPacket()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return append(data, buf...), nil
}

//data already have header
func (p *packetIO) writePacket(data []byte) error {
	length := len(data) - 4

	for length >= mysql.MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.sequence

		if n, err := p.wb.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			return mysql.ErrBadConn
		} else if n != (4 + mysql.MaxPayloadLen) {
			return mysql.ErrBadConn
		} else {
			p.sequence++
			length -= mysql.MaxPayloadLen
			data = data[mysql.MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.sequence

	if n, err := p.wb.Write(data); err != nil {
		return errors.Trace(mysql.ErrBadConn)
	} else if n != len(data) {
		return errors.Trace(mysql.ErrBadConn)
	} else {
		p.sequence++
		return nil
	}
}

func (p *packetIO) flush() error {
	return p.wb.Flush()
}
