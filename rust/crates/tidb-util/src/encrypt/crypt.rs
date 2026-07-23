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

//! Legacy SQL ENCODE/DECODE codec from `pkg/util/encrypt/crypt.go`.

#[derive(Clone, Copy, Default)]
struct RandStruct {
    seed1: u32,
    seed2: u32,
    max_value: u32,
    max_value_double: f64,
}

impl RandStruct {
    fn random_init(&mut self, password: &[u8]) {
        let mut nr = 1_345_345_333_u32;
        let mut add = 7_u32;
        let mut nr2 = 0x1234_5671_u32;

        for password_byte in password {
            if matches!(*password_byte, b' ' | b'\t') {
                continue;
            }
            let value = u32::from(*password_byte);
            nr ^= (nr & 63)
                .wrapping_add(add)
                .wrapping_mul(value)
                .wrapping_add(nr << 8);
            nr2 = nr2.wrapping_add((nr2 << 8) ^ nr);
            add = add.wrapping_add(value);
        }

        let seed1 = nr & ((1_u32 << 31) - 1);
        let seed2 = nr2 & ((1_u32 << 31) - 1);
        self.max_value = 0x3fff_ffff;
        self.max_value_double = f64::from(self.max_value);
        self.seed1 = seed1 % self.max_value;
        self.seed2 = seed2 % self.max_value;
    }

    fn my_rand(&mut self) -> f64 {
        self.seed1 = self.seed1.wrapping_mul(3).wrapping_add(self.seed2) % self.max_value;
        self.seed2 = self.seed1.wrapping_add(self.seed2).wrapping_add(33) % self.max_value;
        f64::from(self.seed1) / self.max_value_double
    }
}

struct SqlCrypt {
    random: RandStruct,
    original_random: RandStruct,
    decode_buffer: [u8; 256],
    encode_buffer: [u8; 256],
    shift: u32,
}

impl Default for SqlCrypt {
    fn default() -> Self {
        Self {
            random: RandStruct::default(),
            original_random: RandStruct::default(),
            decode_buffer: [0; 256],
            encode_buffer: [0; 256],
            shift: 0,
        }
    }
}

impl SqlCrypt {
    fn init(&mut self, password: &[u8]) {
        self.random.random_init(password);
        for (index, value) in self.decode_buffer.iter_mut().enumerate() {
            *value = index as u8;
        }
        for index in 0..256 {
            let random_index = (self.random.my_rand() * 255.0) as usize;
            self.decode_buffer.swap(random_index, index);
        }
        for index in 0..256 {
            self.encode_buffer[usize::from(self.decode_buffer[index])] = index as u8;
        }
        self.original_random = self.random;
        self.shift = 0;
    }

    fn encode(&mut self, value: &mut [u8]) {
        for byte in value {
            self.shift ^= (self.random.my_rand() * 255.0) as u32;
            let index = u32::from(*byte);
            *byte = self.encode_buffer[index as usize] ^ self.shift as u8;
            self.shift ^= index;
        }
    }

    fn decode(&mut self, value: &mut [u8]) {
        for byte in value {
            self.shift ^= (self.random.my_rand() * 255.0) as u32;
            let index = u32::from(*byte ^ self.shift as u8);
            *byte = self.decode_buffer[index as usize];
            self.shift ^= u32::from(*byte);
        }
    }
}

/// Applies MySQL's historical `DECODE()` transformation to arbitrary bytes.
#[must_use]
pub fn sql_decode(value: &[u8], password: &[u8]) -> Vec<u8> {
    let mut crypt = SqlCrypt::default();
    crypt.init(password);
    let mut decoded = value.to_vec();
    crypt.decode(&mut decoded);
    decoded
}

/// Applies MySQL's historical `ENCODE()` inverse transformation.
#[must_use]
pub fn sql_encode(value: &[u8], password: &[u8]) -> Vec<u8> {
    let mut crypt = SqlCrypt::default();
    crypt.init(password);
    let mut encoded = value.to_vec();
    crypt.encode(&mut encoded);
    encoded
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;

    const CASES: [(&str, &str, &str); 10] = [
        ("", "", ""),
        ("pingcap", "1234567890123456", "2C35B5A4ADF391"),
        ("pingcap", "asdfjasfwefjfjkj", "351CC412605905"),
        (
            "pingcap123",
            "123456789012345678901234",
            "7698723DC6DFE7724221",
        ),
        ("pingcap#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449"),
        ("pingcap", "", "4A77B524BD2C5C"),
        (
            "分布式データベース",
            "pass1234@#$%%^^&",
            "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE",
        ),
        (
            "分布式データベース",
            "分布式7782734adgwy1242",
            "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC",
        ),
        ("pingcap", "密匙", "CE5C02A5010010"),
        (
            "pingcap数据库",
            "数据库passwd12345667",
            "36D5F90D3834E30E396BE3226E3B4ED3",
        ),
    ];

    fn to_hex(value: &[u8]) -> String {
        value.iter().map(|byte| format!("{byte:02X}")).collect()
    }

    #[test]
    fn TestSQLDecode() {
        for (value, password, expected) in CASES {
            assert_eq!(
                to_hex(&sql_decode(value.as_bytes(), password.as_bytes())),
                expected
            );
        }
    }

    #[test]
    fn TestSQLEncode() {
        for (value, password, _) in CASES {
            let encrypted = sql_decode(value.as_bytes(), password.as_bytes());
            assert_eq!(
                sql_encode(&encrypted, password.as_bytes()),
                value.as_bytes()
            );
        }
    }
}
