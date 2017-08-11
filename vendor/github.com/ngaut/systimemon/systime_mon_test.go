package systimemon

import (
	"testing"
	"time"
)

func TestSystimeMonitor(t *testing.T) {
	jumpForward := false
	trigged := false
	go StartMonitor(
		func() time.Time {
			if !trigged {
				trigged = true
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			jumpForward = true
		})

	time.Sleep(1 * time.Second)

	if !jumpForward {
		t.Error("should detect time error")
	}
}
