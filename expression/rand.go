package expression

import "time"

const maxRandValue = 0x3FFFFFFF

type MySQLRng struct {
	seed1 uint32
	seed2 uint32
}

func NewWithSeed(seed int64) MySQLRng {
	seed1 := uint32(seed*0x10001+55555555) % maxRandValue
	seed2 := uint32(seed*0x10000001) % maxRandValue
	return MySQLRng{seed1: seed1, seed2: seed2}
}

func NewWithTime() MySQLRng {
	return NewWithSeed(time.Now().UnixNano())
}

func (rng *MySQLRng) Gen() float64 {
	rng.seed1 = (rng.seed1*3 + rng.seed2) % maxRandValue
	rng.seed2 = (rng.seed1 + rng.seed2 + 33) % maxRandValue
	return float64(rng.seed1) / float64(maxRandValue)
}
