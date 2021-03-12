package util

import (
	"math"
)

// Version is the wrapper of TiKV's version.
type Version struct {
	Ver uint64
}

var (
	// MaxVersion is the maximum version, notice that it's not a valid version.
	MaxVersion = Version{Ver: math.MaxUint64}
	// MinVersion is the minimum version, it's not a valid version, too.
	MinVersion = Version{Ver: 0}
)

// NewVersion creates a new Version struct.
func NewVersion(v uint64) Version {
	return Version{
		Ver: v,
	}
}

// Cmp returns the comparison result of two versions.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (v Version) Cmp(another Version) int {
	if v.Ver > another.Ver {
		return 1
	} else if v.Ver < another.Ver {
		return -1
	}
	return 0
}

// PhysicalVer returns the physical version.
func (v Version) PhysicalVer() uint64 {
	return v.Ver
}
