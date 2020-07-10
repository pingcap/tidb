package chunk

import (
	"bufio"
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

var checksumBufReaderPool = sync.Pool{
	New: func() interface{} { return bufio.NewReaderSize(nil, checksumPayloadSize) },
}

type checksum struct {
	disk      *os.File
	bufWriter *bufio.Writer
}

func NewChecksum(disk *os.File) *checksum {
	cs := &checksum{disk: disk}
	cs.bufWriter = bufWriterPool.Get().(*bufio.Writer)
	cs.bufWriter.Reset(disk)
	return cs
}

func (cks *checksum) Write(p []byte) (n int, err error) {
	sum := crc32.Checksum(p, crc32.MakeTable(crc32.IEEE))
	buf := make([]byte, checksumSize)
	buf[0] = byte(sum & 0xff000000 >> 24)
	buf[1] = byte(sum & 0x00ff0000 >> 16)
	buf[2] = byte(sum & 0x0000ff00 >> 8)
	buf[3] = byte(sum & 0x000000ff)
	_, err = cks.bufWriter.Write(buf)
	if err != nil {
		return 0, err
	}
	n, err = cks.bufWriter.Write(p)
	if err != nil {
		return 0, err
	}
	return checksumSize + n, nil
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
	bufReader := bufReaderPool.Get().(*bufio.Reader)
	bufReader.Reset(r)
	defer bufReaderPool.Put(bufReader)

	return s.r.ReadAt(p, off)
}
