package expression

import "time"

const maxRandValue = 0x3FFFFFFF

// MysqlRng is random number generator same as mysql.
type MysqlRng struct {
	seed1 uint32
	seed2 uint32
}

// NewWithSeed create a rng with random seed.
func NewWithSeed(seed int64) *MysqlRng {
	seed1 := uint32(seed*0x10001+55555555) % maxRandValue
	seed2 := uint32(seed*0x10000001) % maxRandValue
	return &MysqlRng{seed1: seed1, seed2: seed2}
}

// NewWithTime create a rng with time stamp.
func NewWithTime() *MysqlRng {
	return NewWithSeed(time.Now().UnixNano())
}

// Gen will generate random number.
func (rng *MysqlRng) Gen() float64 {
	rng.seed1 = (rng.seed1*3 + rng.seed2) % maxRandValue
	rng.seed2 = (rng.seed1 + rng.seed2 + 33) % maxRandValue
	return float64(rng.seed1) / float64(maxRandValue)
}
