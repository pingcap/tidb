// Copyright 2017 PingCAP, Inc.
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

package dashbase

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/linkedin/goavro"
)

// See http://avro.apache.org/docs/1.8.2/spec.html#schema_fingerprints
const emptyCRC64 uint64 = 0xc15d213aa4d7a795

const avroSchema string = `{"name":"io.dashbase.avro.DashbaseEvent","type":"record","fields":[{"name":"timeInMillis","type":"long"},{"name":"metaColumns","type":{"type":"map","values":"string"}},{"name":"numberColumns","type":{"type":"map","values":"double"}},{"name":"textColumns","type":{"type":"map","values":"string"}},{"name":"idColumns","type":{"type":"map","values":"string"}},{"name":"omitPayload","type":"boolean"}]}`

var table []uint64
var dashbaseCodec *goavro.Codec
var dashbaseSchemaChecksum uint64

func makeCRC64Table() {
	table := make([]uint64, 256)
	for i := 0; i < 256; i++ {
		fp := uint64(i)
		for j := 0; j < 8; j++ {
			fp = (fp >> 1) ^ (emptyCRC64 & -(fp & 1))
		}
		table[i] = fp
	}
}

func makeAvroCodec() {
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		panic(err)
	}
	dashbaseCodec = codec
}

func avroCRC64(buf []byte) uint64 {
	fp := emptyCRC64
	for _, val := range buf {
		fp = (fp >> 8) ^ table[int(fp^uint64(val))&0xff]
	}
	return fp
}

func AvroEncode(columns []*ColumnDefinition, values []interface{}) ([]byte, error) {
	event := make(map[string]interface{})
	metaColumns := make(map[string]string)
	textColumns := make(map[string]string)
	numberColumns := make(map[string]float64)
	idColumns := make(map[string]string)

	for idx, column := range columns {
		switch column.dataType {
		case TypeTime:
			event["timeInMillis"] = values[idx].(uint64)
		case TypeMeta:
			metaColumns[column.name] = values[idx].(string)
		case TypeText:
			textColumns[column.name] = values[idx].(string)
		case TypeNumeric:
			numberColumns[column.name] = values[idx].(float64)
		}
	}

	event["metaColumns"] = metaColumns
	event["textColumns"] = textColumns
	event["numberColumns"] = numberColumns
	event["idColumns"] = idColumns
	event["omitPayload"] = false

	body, err := dashbaseCodec.BinaryFromNative(nil, event)
	if err != nil {
		return nil, errors.Trace(err)
	}

	message := new(bytes.Buffer)
	message.Write([]byte{0xC3, 0x01})
	binary.Write(message, binary.LittleEndian, dashbaseSchemaChecksum)
	message.Write(body)

	return message.Bytes(), nil
}

func init() {
	makeAvroCodec()
	makeCRC64Table()
	dashbaseSchemaChecksum = avroCRC64([]byte(avroSchema))
}
