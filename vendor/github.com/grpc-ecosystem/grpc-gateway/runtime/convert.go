package runtime

import (
	"encoding/base64"
	"strconv"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
)

// String just returns the given string.
// It is just for compatibility to other types.
func String(val string) (string, error) {
	return val, nil
}

// Bool converts the given string representation of a boolean value into bool.
func Bool(val string) (bool, error) {
	return strconv.ParseBool(val)
}

// Float64 converts the given string representation into representation of a floating point number into float64.
func Float64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// Float32 converts the given string representation of a floating point number into float32.
func Float32(val string) (float32, error) {
	f, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

// Int64 converts the given string representation of an integer into int64.
func Int64(val string) (int64, error) {
	return strconv.ParseInt(val, 0, 64)
}

// Int32 converts the given string representation of an integer into int32.
func Int32(val string) (int32, error) {
	i, err := strconv.ParseInt(val, 0, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

// Uint64 converts the given string representation of an integer into uint64.
func Uint64(val string) (uint64, error) {
	return strconv.ParseUint(val, 0, 64)
}

// Uint32 converts the given string representation of an integer into uint32.
func Uint32(val string) (uint32, error) {
	i, err := strconv.ParseUint(val, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(i), nil
}

// Bytes converts the given string representation of a byte sequence into a slice of bytes
// A bytes sequence is encoded in URL-safe base64 without padding
func Bytes(val string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Timestamp converts the given RFC3339 formatted string into a timestamp.Timestamp.
func Timestamp(val string) (*timestamp.Timestamp, error) {
	var r *timestamp.Timestamp
	err := jsonpb.UnmarshalString(val, r)
	return r, err
}

// Duration converts the given string into a timestamp.Duration.
func Duration(val string) (*duration.Duration, error) {
	var r *duration.Duration
	err := jsonpb.UnmarshalString(val, r)
	return r, err
}
