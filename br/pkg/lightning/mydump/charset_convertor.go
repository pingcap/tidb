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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"bytes"
	"io"
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
	// sourceCharacterSet represents the charset that the data source uses.
	sourceCharacterSet Charset
	// invalidCharReplacement is the default replacement character for the invalid content, e.g "\ufffd".
	invalidCharReplacement      string
	invalidCharReplacementBytes []byte
}

// NewCharsetConvertor creates a new CharsetConvertor.
func NewCharsetConvertor(cfg *config.CSVConfig) (*CharsetConvertor, error) {
	sourceCharacterSet, invalidCharReplacement, err := loadCharsetFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	// No need to convert the charset encoding, just return the original reader.
	// if sourceCharacterSet == BINARY || sourceCharacterSet == UTF8MB4 {
	// 	return nil, nil
	// }
	invalidCharReplacementBytes := []byte(invalidCharReplacement)
	log.L().Warn(
		"incompatible strings may be encountered during the transcoding process and will be replaced, please be aware of the risk of not being able to retain the original information",
		zap.String("source-character-set", sourceCharacterSet.String()),
		zap.ByteString("invalid-char-replacement", invalidCharReplacementBytes))
	return &CharsetConvertor{
		sourceCharacterSet:          sourceCharacterSet,
		invalidCharReplacement:      invalidCharReplacement,
		invalidCharReplacementBytes: invalidCharReplacementBytes,
	}, nil
}

func loadCharsetFromConfig(cfg *config.CSVConfig) (Charset, string, error) {
	dataInvalidCharReplace := cfg.DataInvalidCharReplace
	switch cfg.DataCharacterSet {
	case "", "binary":
		return BINARY, dataInvalidCharReplace, nil
	case "utf8mb4":
		return UTF8MB4, dataInvalidCharReplace, nil
	case "gb18030":
		return GB18030, dataInvalidCharReplace, nil
	case "gbk":
		return GBK, dataInvalidCharReplace, nil
	default:
		return BINARY, dataInvalidCharReplace, errors.Errorf("found unsupported data-character-set: %s", cfg.DataCharacterSet)
	}
}

var utf8RuneErrorBytes = []byte(string(utf8.RuneError))

// Decode does the charset conversion work from sourceCharacterSet to utf8mb4.
// It will return a byte slice as the conversion result whose length may be less or greater
// than the original byte slice `src`.
func (cc *CharsetConvertor) Decode(src []byte) ([]byte, error) {
	// No need to convert the charset encoding, just return the original data.
	if cc.sourceCharacterSet == BINARY || cc.sourceCharacterSet == UTF8MB4 {
		return src, nil
	}
	// To make sure the res have enough room to store the transcoded bytes, we assume every byte of src
	// will take utf8.UTFMax bytes in the res.
	res := make([]byte, len(src)*utf8.UTFMax)
	// Create the decoder transformer first.
	transformer, _, err := cc.buildTransformer(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	// Do the conversion then.
	_, err = transformer.Read(res)
	if err != nil {
		return nil, err
	}
	res = bytes.ReplaceAll(res, utf8RuneErrorBytes, cc.invalidCharReplacementBytes)
	return bytes.Trim(res, "\x00"), nil
}

// GBKMax is the maximum number of bytes of a GBK encoded character.
const GBKMax = 2

// Encode will encode the data from utf8mb4 to sourceCharacterSet.
func (cc *CharsetConvertor) Encode(src []byte) ([]byte, error) {
	// No need to convert the charset encoding, just return the original data.
	if cc.sourceCharacterSet == BINARY || cc.sourceCharacterSet == UTF8MB4 {
		return src, nil
	}
	// To make sure the res have enough room to store the transcoded bytes, we assume every byte of src
	// will take GBKMax bytes in the res.
	res := make([]byte, len(src)*GBKMax)
	// Create the encoder transformer first.
	var transformer *transform.Reader
	_, transformer, err := cc.buildTransformer(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	// Do the conversion then.
	_, err = transformer.Read(res)
	if err != nil {
		return nil, err
	}
	return bytes.Trim(res, "\x00"), nil
}

func (cc *CharsetConvertor) buildTransformer(bytesReader io.Reader) (decoder, encoder *transform.Reader, err error) {
	switch cc.sourceCharacterSet {
	case GB18030:
		decoder = transform.NewReader(bytesReader, simplifiedchinese.GB18030.NewDecoder())
		encoder = transform.NewReader(bytesReader, simplifiedchinese.GB18030.NewEncoder())
	case GBK:
		decoder = transform.NewReader(bytesReader, simplifiedchinese.GBK.NewDecoder())
		encoder = transform.NewReader(bytesReader, simplifiedchinese.GBK.NewEncoder())
	default:
		return nil, nil, errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
	}
	return decoder, encoder, nil
}

// ContainsInvalidChar returns whether the given data contains any invalid char,
// such as utf8.RuneError or invalid char replacement.
func (cc *CharsetConvertor) ContainsInvalidChar(data []byte) bool {
	return bytes.Contains(data, utf8RuneErrorBytes) || bytes.Contains(data, cc.invalidCharReplacementBytes)
}
