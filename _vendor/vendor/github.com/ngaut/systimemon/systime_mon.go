package systimemon

import (
	"time"

	"github.com/ngaut/log"
)

// Call systimeErrHandler if system time jump backward.
func StartMonitor(now func() time.Time, systimeErrHandler func()) {
	log.Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		last := now()
		select {
		case <-tick.C:
			if now().Sub(last) < 0 {
				log.Errorf("system time jump backward, last:%v", last)
				systimeErrHandler()
			}
		}
	}
}
