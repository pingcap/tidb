package chunk

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	checksumBlockSize   = 1024
	checksumSize        = 4
	checksumPayloadSize = checksumBlockSize - checksumSize
)

var checksumReaderBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, checksumBlockSize) },
}

type checksum struct {
	disk        *os.File
	buf         []byte
	payload     []byte
	payloadUsed int
	readerMu    sync.Mutex
}

func newChecksum(disk *os.File) *checksum {
	cks := &checksum{disk: disk}
	cks.buf = make([]byte, checksumBlockSize)
	cks.payload = cks.buf[checksumSize:]
	cks.payloadUsed = 0
	return cks
}

// Available returns how many bytes are unused in the buffer.
func (cks *checksum) Available() int { return checksumPayloadSize - cks.payloadUsed }

func (cks *checksum) Write(p []byte) (nn int, err error) {
	for len(p) > cks.Available() {
		n := copy(cks.payload[cks.payloadUsed:], p)
		cks.payloadUsed += n
		err = cks.Flush()
		if err != nil {
			return
		}
		nn += n
		p = p[n:]
	}
	n := copy(cks.payload[cks.payloadUsed:], p)
	cks.payloadUsed += n
	nn += n
	return
}

// Flush writes any buffered data to the disk.
func (cks *checksum) Flush() error {
	if cks.payloadUsed == 0 {
		return nil
	}
	checksum := crc32.Checksum(cks.payload[:cks.payloadUsed], crc32.MakeTable(crc32.IEEE))
	binary.LittleEndian.PutUint32(cks.buf, checksum)
	n, err := cks.disk.Write(cks.buf[:cks.payloadUsed+checksumSize])
	if n < cks.payloadUsed && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	cks.payloadUsed = 0
	return nil
}

func (cks *checksum) ReadAt(p []byte, off int64) (nn int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	offsetInPayload := off % checksumPayloadSize
	cursor := off / checksumPayloadSize * checksumBlockSize

	buf := checksumReaderBufPool.Get().([]byte)
	defer checksumReaderBufPool.Put(buf)

	var n int
	for len(p) > 0 {
		n, err = cks.disk.ReadAt(buf, cursor)
		if err != nil {
			if err != io.EOF {
				return
			}
			err = nil
			if n == 0 {
				return
			}
		}
		cursor += int64(n)
		originChecksum := binary.LittleEndian.Uint32(buf)
		checksum := crc32.Checksum(buf[checksumSize:n], crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return nn, errors.New("error checksum")
		}
		n1 := copy(p, buf[checksumSize+offsetInPayload:n])
		nn += n1
		p = p[n1:]
		offsetInPayload = 0
	}
	return
}
