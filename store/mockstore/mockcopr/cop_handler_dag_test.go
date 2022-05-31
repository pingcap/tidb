// Copyright 2018-present, PingCAP, Inc.
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

package mockcopr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstructTimezone(t *testing.T) {
	secondsEastOfUTC := int((8 * time.Hour).Seconds())
	loc, err := constructTimeZone("", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc := time.Date(2018, 8, 15, 20, 0, 0, 0, loc)
	timeInUTC := time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	secondsEastOfUTC = int((-8 * time.Hour).Seconds())
	loc, err = constructTimeZone("", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 12, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 20, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	secondsEastOfUTC = 0
	loc, err = constructTimeZone("", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 20, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 20, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	secondsEastOfUTC = int((23 * time.Hour).Seconds())
	loc, err = constructTimeZone("UTC", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 12, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	secondsEastOfUTC = int((-23 * time.Hour).Seconds())
	loc, err = constructTimeZone("UTC", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 12, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	loc, err = constructTimeZone("UTC", 0)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 12, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	secondsEastOfUTC = int((-23 * time.Hour).Seconds())
	loc, err = constructTimeZone("Asia/Shanghai", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 20, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	secondsEastOfUTC = int((23 * time.Hour).Seconds())
	loc, err = constructTimeZone("Asia/Shanghai", secondsEastOfUTC)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 20, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the seconds east of UTC is ignored by the function
	// constructTimeZone().
	loc, err = constructTimeZone("Asia/Shanghai", 0)
	require.NoError(t, err)
	timeInLoc = time.Date(2018, 8, 15, 20, 0, 0, 0, loc)
	timeInUTC = time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC)
	require.True(t, timeInLoc.Equal(timeInUTC))

	// test the timezone name is not existed.
	_, err = constructTimeZone("asia/not-exist", 0)
	require.EqualError(t, err, "invalid name for timezone asia/not-exist")
}
