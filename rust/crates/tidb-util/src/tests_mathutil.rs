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

//! Ports of `pkg/util/mathutil` unit tests from Go (`math_test.go`,
//! `exponential_average_test.go`, `rand_test.go`).

use crate::mathutil::{
    clamp, divide_2_batches, next_power_of_two, str_len_of_uint64_fast, ExponentialMovingAverage,
    MysqlRng,
};

/// Go: pkg/util/mathutil/math_test.go TestStrLenOfUint64Fast (subtest ManualInput)
#[test]
fn str_len_of_uint64_fast_manual_input() {
    let nums: [u64; 22] = [
        0,
        1,
        12,
        123,
        1234,
        12345,
        123456,
        1_234_567,
        12_345_678,
        123_456_789,
        1_234_567_890,
        12_345_678_91,
        123_456_789_12,
        1_234_567_891_23,
        123_456_789_123_4,
        12_345_678_912_345,
        123_456_789_123_456,
        1_234_567_891_234_567,
        12_345_678_912_345_678,
        123_456_789_123_456_789,
        123_456_789_123_457_890,
        u64::MAX,
    ];
    for num in nums {
        let expected = num.to_string().len();
        assert_eq!(str_len_of_uint64_fast(num), expected);
    }
}

/// Go: pkg/util/mathutil/math_test.go TestClamp
#[test]
fn clamp_cases() {
    assert_eq!(clamp(100, 1, 3), 3);
    assert_eq!(clamp(2.0f64, 1.0, 3.0), 2.0);
    assert_eq!(clamp(0.0f32, 1.0, 3.0), 1.0);
    assert_eq!(clamp(0, 1, 1), 1);
    assert_eq!(clamp(100, 1, 1), 1);
    assert_eq!(clamp("aa", "ab", "xy"), "ab");
    assert_eq!(clamp("yy", "ab", "xy"), "xy");
    assert_eq!(clamp("ab", "ab", "ab"), "ab");
}

/// Go: pkg/util/mathutil/math_test.go TestNextPowerOfTwo
#[test]
fn next_power_of_two_cases() {
    assert_eq!(next_power_of_two(1), 1);
    assert_eq!(next_power_of_two(3), 4);
    assert_eq!(next_power_of_two(255), 256);
    assert_eq!(next_power_of_two(1024), 1024);
    assert_eq!(next_power_of_two(0xabcd_1234), 0x1_0000_0000);
}

fn divide_2_batches_i64(total: i64, batches: i64) -> Vec<i64> {
    divide_2_batches::<i64>(total, batches)
}

/// Go: pkg/util/mathutil/math_test.go TestDivide2Batches
#[test]
fn divide_2_batches_cases() {
    assert_eq!(divide_2_batches_i64(0, 1), Vec::<i64>::new());
    assert_eq!(divide_2_batches_i64(1, 1), vec![1]);
    assert_eq!(divide_2_batches_i64(1, 3), vec![1]);
    assert_eq!(divide_2_batches_i64(2, 2), vec![1, 1]);
    assert_eq!(divide_2_batches_i64(2, 10), vec![1, 1]);
    assert_eq!(divide_2_batches_i64(10, 1), vec![10]);
    assert_eq!(divide_2_batches_i64(10, 2), vec![5, 5]);
    assert_eq!(divide_2_batches_i64(10, 3), vec![4, 3, 3]);
    assert_eq!(divide_2_batches_i64(10, 4), vec![3, 3, 2, 2]);
    assert_eq!(divide_2_batches_i64(10, 5), vec![2, 2, 2, 2, 2]);
}

const SAMPLES: [f64; 100] = [
    1576.0, 1524.0, 6746.0, 6426.0, 9476.0, 1721.0, 8528.0, 7827.0, 8613.0, 6969.0, 4200.0, 4686.0,
    2408.0, 3956.0, 7105.0, 1341.0, 9938.0, 9789.0, 6199.0, 4868.0, 4280.0, 7738.0, 7219.0, 3388.0,
    2431.0, 1193.0, 1954.0, 2147.0, 7726.0, 3545.0, 8043.0, 2379.0, 4859.0, 4247.0, 2873.0, 6419.0,
    3114.0, 3132.0, 6534.0, 8515.0, 1632.0, 9710.0, 6699.0, 1552.0, 2412.0, 4679.0, 4499.0, 9577.0,
    7528.0, 8931.0, 7904.0, 5104.0, 8533.0, 7633.0, 4933.0, 1078.0, 3209.0, 1168.0, 1421.0, 4495.0,
    2333.0, 1439.0, 8584.0, 7814.0, 4320.0, 9569.0, 1370.0, 6635.0, 7870.0, 2828.0, 1599.0, 3592.0,
    1934.0, 5944.0, 9418.0, 4143.0, 2285.0, 6756.0, 2674.0, 7293.0, 4206.0, 5279.0, 9744.0, 2610.0,
    2760.0, 9176.0, 1731.0, 3877.0, 2084.0, 2016.0, 3505.0, 5951.0, 4797.0, 5948.0, 8287.0, 8641.0,
    9349.0, 2690.0, 3820.0, 3895.0,
];

/// Go: pkg/util/mathutil/exponential_average_test.go TestExponential
#[test]
fn exponential() {
    let mut win = ExponentialMovingAverage::new(0.8, 2);
    for &s in &SAMPLES {
        win.add(s);
    }
    assert_eq!(win.get() as i64, 3886);
}

/// Go: pkg/util/mathutil/rand_test.go TestRandWithTime
#[test]
fn rand_with_time() {
    std::thread::sleep(std::time::Duration::from_millis(1));
    let rng1 = MysqlRng::new_with_time();
    let rng2 = MysqlRng::new_with_time();
    let got1 = rng1.gen();
    let got2 = rng2.gen();
    assert!(got1 < 1.0);
    assert!(got1 >= 0.0);
    assert_ne!(got1, rng1.gen());
    assert!(got2 < 1.0);
    assert!(got2 >= 0.0);
    assert_ne!(got2, rng2.gen());
    // The two time-seeded RNGs must differ after a sleep of >= 1ms.
    assert_ne!(got1, got2);
}

/// Go: pkg/util/mathutil/rand_test.go TestRandWithSeed
#[test]
fn rand_with_seed() {
    let tests: [(i64, f64, f64); 4] = [
        (0, 0.155_220_427_694_935_74, 0.620_881_741_513_388),
        (1, 0.405_403_537_121_977_24, 0.871_614_180_385_707_1),
        (-1, 0.905_037_321_993_184_5, 0.370_149_321_267_520_37),
        (
            9_223_372_036_854_775_807,
            0.905_037_321_993_184_5,
            0.370_149_321_267_520_37,
        ),
    ];
    for (seed, once, twice) in tests {
        let rng = MysqlRng::new_with_seed(seed);
        assert_eq!(rng.gen(), once, "first gen mismatch for seed {seed}");
        assert_eq!(rng.gen(), twice, "second gen mismatch for seed {seed}");
    }
}

/// Go: pkg/util/mathutil/rand_test.go TestRandWithSeed1AndSeed2
#[test]
fn rand_with_seed1_and_seed2() {
    let seed1: u32 = 10_000_000;
    let seed2: u32 = 1_000_000;

    let rng = MysqlRng::new_with_time();
    rng.set_seed1(seed1);
    rng.set_seed2(seed2);

    assert_eq!(rng.gen(), 0.028_870_999_839_968_048);
    assert_eq!(rng.gen(), 0.116_415_352_669_000_02);
    assert_eq!(rng.gen(), 0.495_463_794_558_740_96);
    assert_eq!(rng.get_seed1(), 532_000_198);
    assert_eq!(rng.get_seed2(), 689_000_330);
}

/// Go: pkg/util/mathutil/math_test.go TestStrLenOfUint64Fast (subtest RandomInput)
#[test]
fn str_len_of_uint64_fast_random_input() {
    let mut state = 0x243F_6A88_85A3_08D3u64;
    for _ in 0..1_000_000 {
        // xorshift64* PRNG: deterministic stand-in for Go's math/rand.Uint64().
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let num = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        assert_eq!(str_len_of_uint64_fast(num), num.to_string().len());
    }
}
