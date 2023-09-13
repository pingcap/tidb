package zap

import (
	"fmt"
	"log/slog"
)

type Logger = slog.Logger
type Field = slog.Attr

// String returns an Attr for a string value.
var String = slog.String

// Int64 returns an Attr for an int64.
var Int64 = slog.Int64

func Int32(msg string, val int32) slog.Attr {
	return slog.Int64(msg, int64(val))
}

func Uint32(msg string, val uint32) slog.Attr {
	return slog.Uint64(msg, uint64(val))
}

func Uint16(msg string, val uint16) slog.Attr {
	return slog.Uint64(msg, uint64(val))
}

func Uint(msg string, val uint) slog.Attr {
	return slog.Uint64(msg, uint64(val))
}

func Uint8(msg string, val uint8) slog.Attr {
	return slog.Uint64(msg, uint64(val))
}

// Int converts an int to an int64 and returns
// an Attr with that value.
var Int = slog.Int

// Uint64 returns an Attr for a uint64.
var Uint64 = slog.Uint64

// Float64 returns an Attr for a floating-point number.
var Float64 = slog.Float64

// Bool returns an Attr for a bool.
var Bool = slog.Bool

// Time returns an Attr for a time.Time.
// It discards the monotonic portion.
var Time = slog.Time

// Duration returns an Attr for a time.Duration.
var Duration = slog.Duration

var Any = slog.Any

func NamedError(key string, err error) slog.Attr {
	return slog.Any(key, err)
}

func Error(err error) slog.Attr {
	return slog.Any("error", err)
}

func Strings(key string, val []string) slog.Attr {
	return slog.Any(key, val)
}

func Stringer(key string, val fmt.Stringer) slog.Attr {
	return slog.String(key, val.String())
}

func Uint64s(key string, val []uint64) slog.Attr {
	return slog.Any(key, val)
}

func Ints(key string, val []int) slog.Attr {
	return slog.Any(key, val)
}

func Int64s(key string, val []int64) slog.Attr {
	return slog.Any(key, val)
}

func Errors(key string, val []error) slog.Attr {
	return slog.Any(key, val)
}

func Stack(key string) slog.Attr {
	// TODO implement here later
	return slog.Any("stack", "todo")
}

func ByteString(key string, val []byte) slog.Attr {
	return slog.Any(key, val)
}

func Binary(key string, val []byte) slog.Attr {
	return slog.Any(key, val)
}

func Reflect(key string, val interface{}) slog.Attr {
	return slog.Any(key, val)
}
