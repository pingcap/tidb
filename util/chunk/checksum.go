package chunk

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

const (
	checksumBlockSize   = 1024
	checksumSize        = 4
	checksumPayloadSize = checksumBlockSize - checksumSize
)

type checksum struct {
	disk        *os.File
	buf         []byte
	payload     []byte
	payloadUsed int
	size        int64
}

func newChecksum(disk *os.File) *checksum {
	cks := &checksum{disk: disk}
	cks.buf = make([]byte, checksumBlockSize)
	cks.payload = cks.buf[checksumSize:]
	cks.payloadUsed = 0
	return cks
}

// Available returns how many bytes are unused in the buffer.
func (cks *checksum) Available() int { return len(cks.payload) - cks.payloadUsed }

func (cks *checksum) Write(p []byte) (nn int, err error) {
	for len(p) > 0 {
		n := copy(cks.payload[cks.payloadUsed:], p)
		cks.payloadUsed += n
		err = cks.Flush()
		if err != nil {
			return
		}
		nn += n
		p = p[n:]
	}
	return
}

// Flush writes any buffered data to the disk.
func (cks *checksum) Flush() error {
	if cks.payloadUsed == 0 {
		return nil
	}
	checksum := crc32.Checksum(cks.payload[:cks.payloadUsed], crc32.MakeTable(crc32.IEEE))
	binary.LittleEndian.PutUint32(cks.buf, checksum)
	if cks.size%checksumBlockSize > 0 {
		cursor := cks.size / checksumBlockSize * checksumBlockSize
		_, err := cks.disk.Seek(int64(cursor), io.SeekStart)
		if err != nil {
			return err
		}
		cks.size = cursor
	}
	n, err := cks.disk.Write(cks.buf[:cks.payloadUsed+checksumSize])
	cks.size += int64(n)
	if n < cks.payloadUsed && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < cks.payloadUsed {
			copy(cks.payload[:cks.payloadUsed-n], cks.payload[n:cks.payloadUsed])
		}
		cks.payloadUsed -= n
		return err
	}
	cks.payloadUsed %= checksumPayloadSize
	return nil
}

func (cks *checksum) ReadAt(p []byte, off int64) (nn int, err error) {
	startBlock := off / checksumPayloadSize
	offsetInPayload := off % checksumPayloadSize
	cursor := startBlock * checksumBlockSize
	var n int
	for len(p) > 0 && cursor < int64(cks.size) {
		if cursor+checksumBlockSize > int64(cks.size) {
			n, err = cks.disk.ReadAt(cks.buf[:int64(cks.size)-cursor], cursor)
		} else {
			n, err = cks.disk.ReadAt(cks.buf, cursor)
		}
		if err != nil {
			return
		}
		cursor += int64(n)
		originChecksum := binary.LittleEndian.Uint32(cks.buf)
		checksum := crc32.Checksum(cks.buf[checksumSize:n], crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return nn, errors.New("error checksum")
		}
		n1 := copy(p, cks.buf[checksumSize+offsetInPayload:n])
		nn += n1
		p = p[n1:]
		offsetInPayload = 0
	}
	return
}
