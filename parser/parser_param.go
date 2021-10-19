package parser

import (
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
)

func ResetParams(p *Parser) {
	p.charset = mysql.DefaultCharset
	p.collation = mysql.DefaultCollationName
	p.lexer.encoding = charset.Encoding{}
}

type ParseParam interface {
	ApplyOn(*Parser) error
}

type CharsetConnection string

func (c CharsetConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.charset = mysql.DefaultCharset
	} else {
		p.charset = string(c)
	}
	return nil
}

type CollationConnection string

func (c CollationConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.collation = mysql.DefaultCollationName
	} else {
		p.collation = string(c)
	}
	return nil
}

type CharsetClient string

func (c CharsetClient) ApplyOn(p *Parser) error {
	p.lexer.encoding = *charset.NewEncoding(string(c))
	return nil
}
