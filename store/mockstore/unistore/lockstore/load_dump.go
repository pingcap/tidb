// Copyright 2019-present PingCAP, Inc.
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

package lockstore

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// LoadFromFile load a meta from a file.
func (ls *MemStore) LoadFromFile(fileName string) (meta []byte, err error) {
	f, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	defer func() {
		err = f.Close()
	}()
	reader := bufio.NewReader(f)
	meta, err = ls.readItem(reader, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cnt := 0
	var keyBuf, valBuf []byte
	for {
		keyBuf, err = ls.readItem(reader, keyBuf)
		if errors.Cause(err) == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Trace(err)
		}
		valBuf, err = ls.readItem(reader, valBuf)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cnt++
		ls.Put(keyBuf, valBuf)
	}
	log.Info("loaded lockstore", zap.Int("entries", cnt))
	return meta, nil
}

var endian = binary.LittleEndian

func (ls *MemStore) readItem(reader *bufio.Reader, buf []byte) ([]byte, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(reader, lenBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l := endian.Uint32(lenBuf)
	if cap(buf) < int(l) {
		buf = make([]byte, l)
	}
	buf = buf[:l]
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf, nil
}

func (ls *MemStore) writeItem(writer *bufio.Writer, data []byte) error {
	lenBuf := make([]byte, 4)
	endian.PutUint32(lenBuf, uint32(len(data)))
	_, err := writer.Write(lenBuf)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

// DumpToFile dumps the meta to a file
func (ls *MemStore) DumpToFile(fileName string, meta []byte) error {
	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return errors.Trace(err)
	}
	writer := bufio.NewWriter(f)
	err = ls.writeItem(writer, meta)
	if err != nil {
		return err
	}
	cnt := 0
	it := ls.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		err = ls.writeItem(writer, it.key)
		if err != nil {
			return errors.Trace(err)
		}
		err = ls.writeItem(writer, it.val)
		if err != nil {
			return errors.Trace(err)
		}
		cnt++
	}
	err = writer.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Sync()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Close()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("dumped lockstore", zap.Int("entries", cnt))
	return errors.Trace(os.Rename(tmpFileName, fileName))
}
