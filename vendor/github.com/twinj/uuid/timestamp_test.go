package uuid

/****************
 * Date: 15/02/14
 * Time: 12:19 PM
 ***************/

import (
	"testing"
)

func TestUUID_Timestamp_now(t *testing.T) {
	sec, nsec := Now()
	if sec <= 1391463463 {
		t.Errorf("Expected a value greater than 02/03/2014 @ 9:37pm in UTC but got %d", sec)
	}
	if nsec == 0 {
		t.Error("Expected a value")
	}
}
