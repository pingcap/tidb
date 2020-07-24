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

var emptyChecksumBlock = make([]byte, checksumBlockSize)

type checksum struct {
	disk *os.File
	err  error
	buf  []byte
	n    int
}

func newChecksum(disk *os.File) *checksum {
	cks := &checksum{disk: disk}
	cks.buf = make([]byte, checksumBlockSize)
	cks.n = checksumSize
	return cks
}

// Available returns how many bytes are unused in the buffer.
func (cks *checksum) Available() int { return len(cks.buf) - cks.n }

func (cks *checksum) Write(p []byte) (nn int, err error) {
	for len(p) > cks.Available() && cks.err == nil {
		n := copy(cks.buf[cks.n:], p)
		cks.n += n
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
	n := copy(cks.buf[cks.n:], p)
	cks.n += n
	nn += n
	return nn, nil
}

// Flush writes any buffered data to the disk.
func (cks *checksum) Flush() error {
	if cks.err != nil {
		return cks.err
	}
	if cks.n == 0 {
		return nil
	}
	checksum := crc32.Checksum(cks.buf[checksumSize:cks.n], crc32.MakeTable(crc32.IEEE))
	binary.LittleEndian.PutUint32(cks.buf, checksum)
	n, err := cks.disk.Write(cks.buf[0:cks.n])
	if n < cks.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < cks.n {
			copy(cks.buf[0:cks.n-n], cks.buf[n:cks.n])
		}
		cks.n -= n
		cks.err = err
		return err
	}
	cks.n = checksumSize
	return nil
}

func (cks *checksum) ReadAt(p []byte, off int64) (nn int, err error) {
	startBlock := off / checksumPayloadSize
	//endBlock := (off + int64(len(p))) / checksumPayloadSize
	offsetInBlock := off - startBlock*checksumPayloadSize
	off = startBlock * checksumPayloadSize
	_, err = cks.disk.Seek(startBlock*checksumBlockSize, io.SeekStart)
	if err != nil {
		return 0, err
	}
	var n int
	for len(p) > 0 && cks.err == nil {
		n, cks.err = cks.disk.Read(cks.buf)
		off += int64(n)
		if cks.err != nil {
			if cks.err != io.EOF {
				return nn, cks.err
			}
			_, err = cks.disk.Seek((startBlock-1)*checksumBlockSize, io.SeekStart)
			if err != nil {
				return 0, err
			}
			off = startBlock * checksumBlockSize
			n, cks.err = cks.disk.Read(cks.buf[:cks.n])
			if cks.err != nil {
				return nn, cks.err
			}
		}
		originChecksum := binary.LittleEndian.Uint32(cks.buf)
		checksum := crc32.Checksum(cks.buf[checksumSize:n], crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return nn, errors.New("error checksum")
		}
		n1 := copy(p, cks.buf[checksumSize+offsetInBlock:n])
		p = p[n1:]
		offsetInBlock = 0
		startBlock++
	}
	return nn, cks.err
}
