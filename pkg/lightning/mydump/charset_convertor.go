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
	"github.com/pingcap/tidb/pkg/lightning/config"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

// CharsetConvertor is used to convert a character set to utf8mb4 encoding.
// In Lightning, we mainly use it to do the GB18030/GBK -> UTF8MB4 conversion.
type CharsetConvertor struct {
	// sourceCharacterSet represents the charset that the data source uses.
	sourceCharacterSet config.Charset
	// invalidCharReplacement is the default replacement character bytes for the invalid content, e.g "\ufffd".
	invalidCharReplacement string

	decoder *encoding.Decoder
	encoder *encoding.Encoder
}

// NewCharsetConvertor creates a new CharsetConvertor.
func NewCharsetConvertor(dataCharacterSet, dataInvalidCharReplace string) (*CharsetConvertor, error) {
	sourceCharacterSet, err := config.ParseCharset(dataCharacterSet)
	if err != nil {
		return nil, err
	}
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

func (cc *CharsetConvertor) initDecoder() error {
	switch cc.sourceCharacterSet {
	case config.Binary, config.UTF8MB4, config.ASCII:
		return nil
	case config.GB18030:
		cc.decoder = simplifiedchinese.GB18030.NewDecoder()
		return nil
	case config.GBK:
		cc.decoder = simplifiedchinese.GBK.NewDecoder()
		return nil
	case config.Latin1:
		// use Windows1252 (not ISO 8859-1) to decode Latin1
		// https://dev.mysql.com/doc/refman/8.0/en/charset-we-sets.html
		cc.decoder = charmap.Windows1252.NewDecoder()
		return nil
	}
	return errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
}

func (cc *CharsetConvertor) initEncoder() error {
	switch cc.sourceCharacterSet {
	case config.Binary, config.UTF8MB4, config.ASCII:
		return nil
	case config.GB18030:
		cc.encoder = simplifiedchinese.GB18030.NewEncoder()
		return nil
	case config.GBK:
		cc.encoder = simplifiedchinese.GBK.NewEncoder()
		return nil
	case config.Latin1:
		// use Windows1252 (not ISO 8859-1) to encode Latin1
		// https://dev.mysql.com/doc/refman/8.0/en/charset-we-sets.html
		cc.encoder = charmap.Windows1252.NewEncoder()
		return nil
	}
	return errors.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
}

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
		cc.sourceCharacterSet == config.Binary ||
		cc.sourceCharacterSet == config.UTF8MB4 ||
		cc.sourceCharacterSet == config.ASCII ||
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
