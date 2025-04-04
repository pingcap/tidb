// Copyright 2025 PingCAP, Inc.
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

package tracing

import (
	"io"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pingcap/errors"
)

type Reader interface {
	io.Reader
	ReadString(delim byte) (string, error)
}

func Read(buf Reader) ([]Event, error) {
	var head [10]byte
	n, err := buf.Read(head[:])
	if err != nil {
		return nil, fmt.Errorf("decode header fail: %s", err)
	}
	if n != 10 || string(head[:]) != magicHead {
		return nil, fmt.Errorf("header magic number mismatch")
	}

	var ver uint32
	err = binary.Read(buf, binary.LittleEndian, &ver)
	if err != nil {
		return nil, fmt.Errorf("decode version fail: %s", err)
	}
	if ver != version {
		return nil, fmt.Errorf("version mismatch")
	}

	var ret []Event
	for {
		var tp [1]byte
		_, err := buf.Read(tp[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Trace(err)
		}

		var ev Event
		err = readEvent(buf, tp[0], &ev)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, ev)
	}
	return ret, nil
}

type fieldDecoder = func(buf Reader) error

func StringD(field string, dest *string) fieldDecoder {
	return func(buf Reader) error {
		val, err := buf.ReadString(0)
		if err != nil {
			return errors.Trace(err)
		}
		*dest = val[:len(val)-1]
		return nil
	}
}

func Float64D(field string, dest *float64) fieldDecoder {
	return func(buf Reader) error {
		var v uint64
		err := binary.Read(buf, binary.LittleEndian, &v)
		if err != nil {
			return errors.Trace(err)
		}
		*dest = math.Float64frombits(v)
		return nil
	}
}

func Uint64D(field string, dest *uint64) fieldDecoder {
	return func(buf Reader) error {
		return binary.Read(buf, binary.LittleEndian, dest)
	}
}

func ByteD(field string, dest *byte) fieldDecoder {
	return func(buf Reader) error {
		var oneByte [1]byte
		_, err := buf.Read(oneByte[:])
		*dest = oneByte[0]
		return err
	}
}

func readEvent(buf Reader, tp byte, ev *Event) error {
	var err error
	ev.Phase = string(tp)
	switch tp {
	case 'B':
		err = decodeBuf(buf, StringD("Name", &ev.Name), Float64D("Time", &ev.Time), Uint64D("TID", &ev.TID))
	case 'E':
		err = decodeBuf(buf, StringD("Name", &ev.Name), Float64D("Time", &ev.Time), Uint64D("TID", &ev.TID))
	case 's':
		err = decodeBuf(buf, StringD("Name", &ev.Name), StringD("Catagory", &ev.Category), Uint64D("ID", &ev.ID),
			Float64D("Time", &ev.Time),
			Uint64D("TID", &ev.TID))
	case 'f':
		var b byte
		err = decodeBuf(buf, StringD("Name", &ev.Name),
			StringD("Category", &ev.Category),
			Uint64D("ID", &ev.ID),
			Float64D("Time", &ev.Time),
			Uint64D("TID", &ev.TID),
			ByteD("BindPoint", &b))
		ev.BindPoint = string(b)
	default:
		return fmt.Errorf("unknown event phase %c", tp)
	}
	return err
}

func decodeBuf(buf Reader, fields ...fieldDecoder) error {
	for _, decode := range fields {
		err := decode(buf)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type Event struct {
	Name      string  `json:"name,omitempty"`
	Phase     string  `json:"ph"`
	Scope     string  `json:"s,omitempty"`
	Time      float64 `json:"ts"`
	Dur       float64 `json:"dur,omitempty"`
	PID       uint64  `json:"pid"`
	TID       uint64  `json:"tid"`
	ID        uint64  `json:"id,omitempty"`
	BindPoint string  `json:"bp,omitempty"`
	Stack     int     `json:"sf,omitempty"`
	EndStack  int     `json:"esf,omitempty"`
	Arg       any     `json:"args,omitempty"`
	Cname     string  `json:"cname,omitempty"`
	Category  string  `json:"cat,omitempty"`
}
