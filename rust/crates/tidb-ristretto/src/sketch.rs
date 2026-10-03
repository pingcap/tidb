// Copyright 2026 PingCAP, Inc.
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

//! Ristretto sketch.go and the Bloom doorkeeper operations used from z/bbloom.go.
// Bloom arithmetic is derived from Andreas Briese's MIT-licensed bbloom.go;
// its license is retained in THIRD_PARTY_LICENSES.md.

pub(crate) struct Sketch {
    pub rows: [Vec<u8>; 4],
    pub seeds: [u64; 4],
    pub mask: u64,
}
impl Sketch {
    pub fn new(counters: usize) -> Self {
        assert!(counters > 0);
        let size = counters.next_power_of_two().max(2);
        Self {
            rows: std::array::from_fn(|_| vec![0; size / 2]),
            seeds: std::array::from_fn(|_| rand::random()),
            mask: (size - 1) as u64,
        }
    }
    pub fn increment(&mut self, key: u64) {
        for (row, seed) in self.rows.iter_mut().zip(self.seeds) {
            let index = ((key ^ seed) & self.mask) as usize;
            let shift = (index & 1) * 4;
            let cell = &mut row[index / 2];
            if (*cell >> shift) & 15 < 15 {
                *cell += 1 << shift;
            }
        }
    }
    pub fn estimate(&self, key: u64) -> i64 {
        self.rows
            .iter()
            .zip(self.seeds)
            .map(|(row, seed)| {
                let index = ((key ^ seed) & self.mask) as usize;
                ((row[index / 2] >> ((index & 1) * 4)) & 15) as i64
            })
            .min()
            .unwrap()
    }
    pub fn reset(&mut self) {
        for row in &mut self.rows {
            for cell in row {
                *cell = (*cell >> 1) & 0x77;
            }
        }
    }
    pub fn clear(&mut self) {
        for row in &mut self.rows {
            row.fill(0);
        }
    }
}

pub(crate) struct Bloom {
    bits: Vec<u64>,
    mask: u64,
    shift: u32,
    locations: u64,
}
impl Bloom {
    pub fn new(counters: usize) -> Self {
        // Preserve the pinned false-positive target and power-of-two rounding.
        let ln2 = 0.693_147_180_56_f64;
        let size = -(counters as f64) * 0.01_f64.ln() / ln2.powi(2);
        let locations = (ln2 * size / counters as f64).ceil() as u64;
        let bits = (size as usize).max(512).next_power_of_two();
        Self {
            bits: vec![0; bits / 64],
            mask: bits as u64 - 1,
            shift: 64 - bits.trailing_zeros(),
            locations,
        }
    }
    fn position(&self, hash: u64, index: u64) -> usize {
        let high = hash >> self.shift;
        let low = (hash << self.shift) >> self.shift;
        high.wrapping_add(index.wrapping_mul(low)) as usize & self.mask as usize
    }
    pub fn has(&self, hash: u64) -> bool {
        (0..self.locations).all(|i| {
            let p = self.position(hash, i);
            self.bits[p / 64] & (1 << (p % 64)) != 0
        })
    }
    pub fn add_if_absent(&mut self, hash: u64) -> bool {
        if self.has(hash) {
            return false;
        }
        for i in 0..self.locations {
            let p = self.position(hash, i);
            self.bits[p / 64] |= 1 << (p % 64);
        }
        true
    }
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }
}

pub(crate) struct TinyLfu {
    pub freq: Sketch,
    pub door: Bloom,
    pub increments: usize,
    reset_at: usize,
}
impl TinyLfu {
    pub fn new(counters: usize) -> Self {
        Self {
            freq: Sketch::new(counters),
            door: Bloom::new(counters),
            increments: 0,
            reset_at: counters,
        }
    }
    pub fn increment(&mut self, key: u64) {
        if !self.door.add_if_absent(key) {
            self.freq.increment(key);
        }
        self.increments += 1;
        if self.increments >= self.reset_at {
            self.increments = 0;
            self.door.clear();
            self.freq.reset();
        }
    }
    pub fn push(&mut self, keys: &[u64]) {
        for key in keys {
            self.increment(*key);
        }
    }
    pub fn estimate(&self, key: u64) -> i64 {
        self.freq.estimate(key) + i64::from(self.door.has(key))
    }
    pub fn clear(&mut self) {
        self.increments = 0;
        self.door.clear();
        self.freq.clear();
    }
}
