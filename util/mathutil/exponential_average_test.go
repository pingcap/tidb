// Copyright 2022 PingCAP, Inc.
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

package mathutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var samples = [100]float64{
	1576, 1524, 6746, 6426, 9476, 1721, 8528, 7827, 8613, 6969, 4200, 4686, 2408, 3956, 7105, 1341,
	9938, 9789, 6199, 4868, 4280, 7738, 7219, 3388, 2431, 1193, 1954, 2147, 7726, 3545, 8043, 2379,
	4859, 4247, 2873, 6419, 3114, 3132, 6534, 8515, 1632, 9710, 6699, 1552, 2412, 4679, 4499, 9577,
	7528, 8931, 7904, 5104, 8533, 7633, 4933, 1078, 3209, 1168, 1421, 4495, 2333, 1439, 8584, 7814,
	4320, 9569, 1370, 6635, 7870, 2828, 1599, 3592, 1934, 5944, 9418, 4143, 2285, 6756, 2674, 7293,
	4206, 5279, 9744, 2610, 2760, 9176, 1731, 3877, 2084, 2016, 3505, 5951, 4797, 5948, 8287, 8641,
	9349, 2690, 3820, 3895,
}

func TestExponential(t *testing.T) {
	win := NewExponentialMovingAverage(0.8, 2)
	for _, s := range samples {
		win.Add(s)
	}
	require.Equal(t, int64(3886), int64(win.Get()))
}
