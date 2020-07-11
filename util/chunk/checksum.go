package chunk

import (
	"bufio"
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

var checksumBufWriterPool = sync.Pool{
	New: func() interface{} { return bufio.NewWriterSize(nil, checksumPayloadSize) },
}

type checksum struct {
	disk      *os.File
	bufWriter *bufio.Writer
}

func NewChecksum(disk *os.File) *checksum {
	cs := &checksum{disk: disk}
	cs.bufWriter = checksumBufWriterPool.Get().(*bufio.Writer)
	cs.bufWriter.Reset(disk)
	return cs
}

func (cks *checksum) Write(p []byte) (n int, err error) {
	buf := make([]byte, checksumBlockSize)
	copy(buf[4:], p)
	checksum := crc32.Checksum(buf[4:], crc32.MakeTable(crc32.IEEE))
	buf[0] = byte(checksum & 0xff000000 >> 24)
	buf[1] = byte(checksum & 0x00ff0000 >> 16)
	buf[2] = byte(checksum & 0x0000ff00 >> 8)
	buf[3] = byte(checksum & 0x000000ff)
	n, err = cks.bufWriter.Write(buf)
	if err != nil {
		return
	}
	err = cks.bufWriter.Flush()
	return
}

func (cks *checksum) Close() error {
	if cks.bufWriter != nil {
		checksumBufWriterPool.Put(cks.bufWriter)
	}
	return nil
}

func (cks *checksum) ReadAt(p []byte, off int64) (n int, err error) {
	blockNo := off / checksumBlockSize
	r := io.NewSectionReader(cks.disk, blockNo*checksumBlockSize, checksumBlockSize)
	buffer := make([]byte, checksumBlockSize)
	n, err = r.Read(buffer)
	if err != nil && err != io.EOF {
		// In the normal case we return here.
		return n, err
	}
	if n <= checksumSize {
		return 0, nil
	}
	originChecksum := uint32(buffer[0])<<24 | uint32(buffer[1])<<16 | uint32(buffer[2])<<8 | uint32(buffer[3])
	payload := buffer[checksumSize:]
	checksum := crc32.Checksum(payload, crc32.MakeTable(crc32.IEEE))
	if originChecksum != checksum {
		return 0, errors.New("checksum error")
	}
	copy(p, payload)
	return len(payload), nil
}
