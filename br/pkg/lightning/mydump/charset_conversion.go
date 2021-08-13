// Copyright 2021 PingCAP, Inc.
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

package mydump

import (
	"bytes"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

type Charset int

const (
	BINARY Charset = iota
	UTF8MB4
	GB18030
	GBK
)

func (c Charset) String() string {
	switch c {
	case BINARY:
		return "binary"
	case UTF8MB4:
		return "utf8mb4"
	case GB18030:
		return "gb18030"
	case GBK:
		return "gbk"
	default:
		return "unknown_charset"
	}
}

// CharsetConvertor is used to convert a character set to utf8mb4 encoding.
// In Lightning, we mainly use it to do the GB18030/GBK -> UTF8MB4 conversion.
type CharsetConvertor struct {
	// sourceCharacterSet represents the charset that the source file uses.
	sourceCharacterSet Charset
	// invalidCharReplacement is the default replacement character for the invalid content, e.g "\ufffd".
	invalidCharReplacement      string
	invalidCharReplacementBytes []byte

	originalReader ReadSeekCloser
}

// NewCharsetConvertor creates a new CharsetConvertor.
func NewCharsetConvertor(cfg *config.MydumperRuntime, reader ReadSeekCloser) (ReadSeekCloser, error) {
	if reader == nil {
		return nil, errors.New("the reader received is nil")
	}
	sourceCharacterSet, invalidCharReplacement, err := loadCharsetFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	invalidCharReplacementBytes := []byte(string(invalidCharReplacement))
	// No need to convert the charset encoding, just return the original reader.
	if sourceCharacterSet == BINARY || sourceCharacterSet == UTF8MB4 {
		return reader, nil
	}
	log.L().Warn(
		"incompatible strings may be encountered during the transcoding process and will be replaced, please be aware of the risk of not being able to retain the original information",
		zap.String("source-character-set", sourceCharacterSet.String()),
		zap.ByteString("invalid-char-replacement", invalidCharReplacementBytes))
	return &CharsetConvertor{
		sourceCharacterSet:          sourceCharacterSet,
		invalidCharReplacement:      invalidCharReplacement,
		invalidCharReplacementBytes: invalidCharReplacementBytes,
		originalReader:              reader,
	}, nil
}

func loadCharsetFromConfig(cfg *config.MydumperRuntime) (Charset, string, error) {
	dataInvalidCharReplace := cfg.DataInvalidCharReplace
	switch cfg.DataCharacterSet {
	case "binary":
		return BINARY, dataInvalidCharReplace, nil
	case "utf8mb4":
		return UTF8MB4, dataInvalidCharReplace, nil
	case "gb18030":
		return GB18030, dataInvalidCharReplace, nil
	case "gbk":
		return GBK, dataInvalidCharReplace, nil
	default:
		return BINARY, dataInvalidCharReplace, errors.New("found unsupported data-character-set")
	}
}

var utf8RuneErrorBytes = []byte(string(utf8.RuneError))

func (cc *CharsetConvertor) Read(p []byte) (n int, err error) {
	// Extend p to make sure it must have a enough room for a UTF8 encoding at last.
	originalLength := len(p)
	extendedLength := originalLength
	if originalLength%utf8.UTFMax != 0 {
		extendedLength = utf8.UTFMax - originalLength%utf8.UTFMax + originalLength
	}
	extendedP := make([]byte, extendedLength)
	// Read from the original reader first.
	originalData := make([]byte, len(extendedP))
	n, err = cc.originalReader.Read(originalData)
	if err != nil {
		return n, err
	}
	// Create the convertor reader first.
	var transformer *transform.Reader
	transformer, err = cc.buildTransformer(originalData)
	if err != nil {
		return 0, err
	}
	// Do the conversion then.
	n, err = transformer.Read(extendedP)
	if err != nil {
		return n, err
	}
	replacedData := bytes.ReplaceAll(extendedP, utf8RuneErrorBytes, cc.invalidCharReplacementBytes)
	// Copy replacedData to original p.
	copy(p, replacedData)
	return n, nil
}

func (cc *CharsetConvertor) buildTransformer(data []byte) (*transform.Reader, error) {
	switch cc.sourceCharacterSet {
	case GB18030:
		return transform.NewReader(bytes.NewReader(data), simplifiedchinese.GB18030.NewDecoder()), nil
	case GBK:
		return transform.NewReader(bytes.NewReader(data), simplifiedchinese.GBK.NewDecoder()), nil
	default:
		return nil, errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
	}
}

func (cc *CharsetConvertor) Seek(offset int64, whence int) (int64, error) {
	return cc.originalReader.Seek(offset, whence)
}

func (cc *CharsetConvertor) Close() error { return cc.originalReader.Close() }
