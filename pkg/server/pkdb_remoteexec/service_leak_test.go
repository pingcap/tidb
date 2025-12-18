package pkdbremoteexec

import (
	"testing"

	"go.uber.org/goleak"
)

func TestServerCloseStopsBackgroundGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)

	s := NewServer(nil, nil)
	s.Close()
}
