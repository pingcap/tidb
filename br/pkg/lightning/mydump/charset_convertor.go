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
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type Charset int

const (
	Binary Charset = iota
	UTF8MB4
	GB18030
	GBK
)

func (c Charset) String() string {
	switch c {
	case Binary:
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
	// invalidCharReplacement is the default replacement character bytes for the invalid content, e.g "\ufffd".
	invalidCharReplacement string

	decoder *encoding.Decoder
	encoder *encoding.Encoder
}

// NewCharsetConvertor creates a new CharsetConvertor.
func NewCharsetConvertor(dataCharacterSet, dataInvalidCharReplace string) (*CharsetConvertor, error) {
	sourceCharacterSet, err := loadCharsetFromConfig(dataCharacterSet)
	if err != nil {
		return nil, err
	}
	log.L().Warn(
		"incompatible strings may be encountered during the transcoding process and will be replaced, please be aware of the risk of not being able to retain the original information",
		zap.String("source-character-set", sourceCharacterSet.String()),
		zap.ByteString("invalid-char-replacement", []byte(dataInvalidCharReplace)))
	cc := &CharsetConvertor{
		sourceCharacterSet,
		dataInvalidCharReplace,
		nil, nil,
	}
	err = cc.initDecoder()
	if err != nil {
		return nil, err
	}
	err = cc.initEncoder()
	if err != nil {
		return nil, err
	}
	return cc, nil
}

func loadCharsetFromConfig(dataCharacterSet string) (Charset, error) {
	switch dataCharacterSet {
	case "", "binary":
		return Binary, nil
	case "utf8mb4":
		return UTF8MB4, nil
	case "gb18030":
		return GB18030, nil
	case "gbk":
		return GBK, nil
	default:
		return Binary, errors.Errorf("found unsupported data-character-set: %s", dataCharacterSet)
	}
}

func (cc *CharsetConvertor) initDecoder() error {
	switch cc.sourceCharacterSet {
	case Binary, UTF8MB4:
		return nil
	case GB18030:
		cc.decoder = simplifiedchinese.GB18030.NewDecoder()
		return nil
	case GBK:
		cc.decoder = simplifiedchinese.GBK.NewDecoder()
		return nil
	}
	return errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
}

func (cc *CharsetConvertor) initEncoder() error {
	switch cc.sourceCharacterSet {
	case Binary, UTF8MB4:
		return nil
	case GB18030:
		cc.encoder = simplifiedchinese.GB18030.NewEncoder()
		return nil
	case GBK:
		cc.encoder = simplifiedchinese.GBK.NewEncoder()
		return nil
	}
	return errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
}

var utf8RuneErrorStr = string(utf8.RuneError)

// Decode does the charset conversion work from sourceCharacterSet to utf8mb4.
// It will return a string as the conversion result whose length may be less or greater
// than the original string `src`.
// TODO: maybe using generic type later to make Decode/Encode accept both []byte and string.
func (cc *CharsetConvertor) Decode(src string) (string, error) {
	if !cc.precheck(src) {
		return src, nil
	}
	// Do the conversion then.
	res, err := cc.decoder.String(src)
	if err != nil {
		return res, err
	}
	return strings.ReplaceAll(res, string(utf8.RuneError), cc.invalidCharReplacement), nil
}

func (cc *CharsetConvertor) precheck(src string) bool {
	// No need to convert the charset encoding, just return the original data.
	if len(src) == 0 || cc == nil ||
		cc.sourceCharacterSet == Binary || cc.sourceCharacterSet == UTF8MB4 ||
		cc.decoder == nil || cc.encoder == nil {
		return false
	}
	return true
}

// Encode will encode the data from utf8mb4 to sourceCharacterSet.
func (cc *CharsetConvertor) Encode(src string) (string, error) {
	if !cc.precheck(src) {
		return src, nil
	}
	// Do the conversion then.
	return cc.encoder.String(src)
}

// ContainsInvalidChar returns whether the given data contains any invalid char,
// such as utf8.RuneError or invalid char replacement.
func (cc *CharsetConvertor) ContainsInvalidChar(data string) bool {
	return strings.Contains(data, utf8RuneErrorStr) || strings.Contains(data, cc.invalidCharReplacement)
}
