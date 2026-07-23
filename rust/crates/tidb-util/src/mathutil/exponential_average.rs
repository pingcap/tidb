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

/// An exponential moving average measurement. Like the Go source, callers
/// need exclusive access while updating it.
#[derive(Clone, Debug)]
pub struct ExponentialMovingAverage {
    value: f64,
    sum: f64,
    factor: f64,
    warmup_window: isize,
    count: isize,
}

impl ExponentialMovingAverage {
    /// Creates a moving average with the source factor and warmup behavior.
    ///
    /// # Panics
    ///
    /// Panics when `factor` compares at or outside `(0, 1)`. The source admits
    /// NaN because both comparisons are false.
    #[must_use]
    pub fn new(factor: f64, warmup_window: isize) -> Self {
        assert!(!(factor >= 1.0 || factor <= 0.0), "factor must be (0, 1)");
        Self {
            value: 0.0,
            sum: 0.0,
            factor,
            warmup_window,
            count: 0,
        }
    }

    /// Adds one sample and updates the internal state.
    pub fn add(&mut self, value: f64) {
        if self.count < self.warmup_window {
            self.count += 1;
            self.sum += value;
            self.value = self.sum / self.count as f64;
        } else {
            self.value = self.value * (1.0 - self.factor) + value * self.factor;
        }
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> f64 {
        self.value
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    const SAMPLES: [f64; 100] = [
        1576.0, 1524.0, 6746.0, 6426.0, 9476.0, 1721.0, 8528.0, 7827.0, 8613.0, 6969.0, 4200.0,
        4686.0, 2408.0, 3956.0, 7105.0, 1341.0, 9938.0, 9789.0, 6199.0, 4868.0, 4280.0, 7738.0,
        7219.0, 3388.0, 2431.0, 1193.0, 1954.0, 2147.0, 7726.0, 3545.0, 8043.0, 2379.0, 4859.0,
        4247.0, 2873.0, 6419.0, 3114.0, 3132.0, 6534.0, 8515.0, 1632.0, 9710.0, 6699.0, 1552.0,
        2412.0, 4679.0, 4499.0, 9577.0, 7528.0, 8931.0, 7904.0, 5104.0, 8533.0, 7633.0, 4933.0,
        1078.0, 3209.0, 1168.0, 1421.0, 4495.0, 2333.0, 1439.0, 8584.0, 7814.0, 4320.0, 9569.0,
        1370.0, 6635.0, 7870.0, 2828.0, 1599.0, 3592.0, 1934.0, 5944.0, 9418.0, 4143.0, 2285.0,
        6756.0, 2674.0, 7293.0, 4206.0, 5279.0, 9744.0, 2610.0, 2760.0, 9176.0, 1731.0, 3877.0,
        2084.0, 2016.0, 3505.0, 5951.0, 4797.0, 5948.0, 8287.0, 8641.0, 9349.0, 2690.0, 3820.0,
        3895.0,
    ];

    #[test]
    fn TestExponential() {
        let mut window = ExponentialMovingAverage::new(0.8, 2);
        for sample in SAMPLES {
            window.add(sample);
        }
        assert_eq!(window.get() as i64, 3886);
    }

    #[test]
    fn source_nan_factor_is_admitted_by_comparison_shape() {
        let mut window = ExponentialMovingAverage::new(f64::NAN, 0);
        window.add(1.0);
        assert!(window.get().is_nan());
    }
}
