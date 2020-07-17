package chunk

import (
	"bufio"
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

var emptyChecksumBlock = make([]byte, checksumBlockSize)

type checksum struct {
	disk      *os.File
	bufWriter *bufio.Writer
}

func NewChecksum(disk *os.File) *checksum {
	cs := &checksum{disk: disk}
	cs.bufWriter = bufio.NewWriterSize(disk, checksumPayloadSize)
	return cs
}

func (cks *checksum) Write(p []byte) (n int, err error) {
	start, end := 0, len(p)
	if end > checksumPayloadSize {
		end = checksumPayloadSize
	}
	payload := p[start:end]
	buf := make([]byte, checksumBlockSize)
	for len(payload) > 0 {
		// We need to fill in payload before calculating the checksum.
		copy(buf[4:], payload)
		checksum := crc32.Checksum(buf[4:], crc32.MakeTable(crc32.IEEE))
		buf[0] = byte(checksum & 0xff000000 >> 24)
		buf[1] = byte(checksum & 0x00ff0000 >> 16)
		buf[2] = byte(checksum & 0x0000ff00 >> 8)
		buf[3] = byte(checksum & 0x000000ff)
		n1, err := cks.bufWriter.Write(buf)
		n += n1
		if err != nil {
			return n, err
		}
		start = end
		end = len(p)
		if end-start > checksumPayloadSize {
			end = start + checksumPayloadSize
		}
		payload = p[start:end]
		copy(buf, emptyChecksumBlock)
	}
	return
}

func (cks *checksum) ReadAt(p []byte, off int64) (n int, err error) {
	startBlock := off / checksumPayloadSize
	endBlock := (off + int64(len(p))) / checksumPayloadSize
	offsetInBlockPayload := off - startBlock*checksumPayloadSize
	writeOffset := int64(0)
	needWriteSize := int64(len(p))

	r := io.NewSectionReader(cks.disk, startBlock*checksumBlockSize, (endBlock-startBlock+1)*checksumBlockSize)
	readBuffer := bufio.NewReaderSize(r, checksumBlockSize)
	buffer := make([]byte, checksumBlockSize)
	for i := int64(0); i < endBlock-startBlock+1 || needWriteSize == 0; i++ {
		n1, err := readBuffer.Read(buffer)
		if n1 != checksumBlockSize {
			return n, err
		}

		originChecksum := uint32(buffer[0])<<24 | uint32(buffer[1])<<16 | uint32(buffer[2])<<8 | uint32(buffer[3])
		payload := buffer[checksumSize:]
		checksum := crc32.Checksum(payload, crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return n, errors.New("checksum error")
		}
		n += n1

		// checksumPayloadSize-offsetInBlockPayload get the remaining payload which is in a checksum block to be copied.
		if needWriteSize >= checksumPayloadSize-offsetInBlockPayload {
			copy(p[writeOffset:], payload[offsetInBlockPayload:])
			needWriteSize -= int64(len(payload[offsetInBlockPayload:]))
			writeOffset += int64(len(payload[offsetInBlockPayload:]))
			offsetInBlockPayload = 0
			copy(buffer, emptyChecksumBlock)
			continue
		}
		copy(p[writeOffset:], payload[offsetInBlockPayload:offsetInBlockPayload+needWriteSize])
		break
	}

	return len(p), nil
}
