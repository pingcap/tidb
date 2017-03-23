package mysql

import (
	"bytes"
	"strings"

	"github.com/juju/errors"
)

func formatENUS(number string, precision int64) (string, error) {
	var buffer bytes.Buffer
	if number[:1] == "-" {
		buffer.Write([]byte{'-'})
		number = number[1:]
	}
	comma := []byte{','}
	parts := strings.Split(number, ".")
	pos := 0
	if len(parts[0])%3 != 0 {
		pos += len(parts[0]) % 3
		buffer.WriteString(parts[0][:pos])
		buffer.Write(comma)
	}
	for ; pos < len(parts[0]); pos += 3 {
		buffer.WriteString(parts[0][pos : pos+3])
		buffer.Write(comma)
	}
	buffer.Truncate(buffer.Len() - 1)
	if precision > 0 {
		buffer.Write([]byte{'.'})
		if int64(len(parts[1])) >= precision {
			buffer.WriteString(parts[1][:precision])
		} else {
			buffer.WriteString(parts[1])
			buffer.WriteString(strings.Repeat("0", int(precision)-len(parts[1])))
		}
	}
	return buffer.String(), nil
}

func formatZHCN(number string, precision int64) (string, error) {
	return "", errors.New("not implemented")
}

func formatNotSupport(number string, precision int64) (string, error) {
	return "", errors.New("not support for the specific locale")
}
