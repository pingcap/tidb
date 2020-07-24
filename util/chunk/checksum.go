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
	err         error
	buf         []byte
	payLoad     []byte
	payLoadUsed int
	size        int
}

func newChecksum(disk *os.File) *checksum {
	cks := &checksum{disk: disk}
	cks.buf = make([]byte, checksumBlockSize)
	cks.payLoad = cks.buf[checksumSize:]
	cks.payLoadUsed = 0
	return cks
}

// Available returns how many bytes are unused in the buffer.
func (cks *checksum) Available() int { return len(cks.payLoad) - cks.payLoadUsed }

func (cks *checksum) Write(p []byte) (nn int, err error) {
	for len(p) > cks.Available() && cks.err == nil {
		n := copy(cks.payLoad[cks.payLoadUsed:], p)
		cks.payLoadUsed += n
		err := cks.Flush()
		if err != nil {
			return nn, err
		}
		nn += n
		p = p[n:]
	}
	if cks.err != nil {
		return nn, cks.err
	}
	n := copy(cks.payLoad[cks.payLoadUsed:], p)
	cks.payLoadUsed += n
	nn += n
	return nn, nil
}

// Flush writes any buffered data to the disk.
func (cks *checksum) Flush() error {
	if cks.err != nil {
		return cks.err
	}
	if cks.payLoadUsed == 0 {
		return nil
	}
	checksum := crc32.Checksum(cks.payLoad[:cks.payLoadUsed], crc32.MakeTable(crc32.IEEE))
	binary.LittleEndian.PutUint32(cks.buf, checksum)
	n, err := cks.disk.Write(cks.buf[:cks.payLoadUsed+checksumSize])
	cks.size += n
	if n < cks.payLoadUsed && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < cks.payLoadUsed {
			copy(cks.payLoad[:cks.payLoadUsed-n], cks.payLoad[n:cks.payLoadUsed])
		}
		cks.payLoadUsed -= n
		cks.err = err
		return err
	}
	cks.payLoadUsed = 0
	return nil
}

func (cks *checksum) ReadAt(p []byte, off int64) (nn int, err error) {
	startBlock := off / checksumPayloadSize
	offsetInPayload := off % checksumPayloadSize
	cursor := startBlock * checksumBlockSize
	var n int
	for len(p) > 0 && cks.err == nil && cursor < int64(cks.size) {
		if cursor+checksumBlockSize > int64(cks.size) {
			n, cks.err = cks.disk.ReadAt(cks.buf[:int64(cks.size)-cursor], cursor)
		} else {
			n, cks.err = cks.disk.ReadAt(cks.buf, cursor)
		}
		if cks.err != nil {
			return nn, cks.err
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
	return nn, cks.err
}
