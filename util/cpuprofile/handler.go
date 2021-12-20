package cpuprofile

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GetCPUProfile uses to get the cpu profile data from GlobalCPUProfiler.
// You should use GetCPUProfile instead of `pprof.StartCPUProfile`, `pprof.StopCPUProfile`.
// Otherwise you may fail, or affect the TopSQL feature and pprof profile HTTP API .
func GetCPUProfile(seconds uint64, w io.Writer) error {
	pc := NewPprofAPIConsumer()
	profileData, err := pc.WaitProfilingFinish(seconds)
	if err != nil {
		return err
	}
	return profileData.Write(w)
}

// ProfileHTTPHandler is same as pprof.Profile.
// The difference is ProfileHTTPHandler uses cpuprofile.GetCPUProfile to fetch profile data.
func ProfileHTTPHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}

	if durationExceedsWriteTimeout(r, float64(sec)) {
		serveError(w, http.StatusBadRequest, "profile duration exceeds server's WriteTimeout")
		return
	}

	// Set Content Type assuming StartCPUProfile will work,
	// because if it does it starts writing.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="profile"`)

	// TODO: fix me.
	// This can be fixed by always starts a 1 second profiling one by one,
	// but to aggregate (merge) multiple profiles into one according to the precision.
	//  |<-- 1s -->|
	// -|----------|----------|----------|----------|----------|-----------|-----> Background profile task timeline.
	//                            |________________________________|
	//       (start cpu profile)  v                                v (stop cpu profile)    // expected profile timeline
	//                        |________________________________|                           // actual profile timeline
	err = GetCPUProfile(uint64(sec), w)
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
	}
}

func durationExceedsWriteTimeout(r *http.Request, seconds float64) bool {
	srv, ok := r.Context().Value(http.ServerContextKey).(*http.Server)
	return ok && srv.WriteTimeout != 0 && seconds >= srv.WriteTimeout.Seconds()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	if err != nil {
		logutil.BgLogger().Info("write http response error", zap.Error(err))
	}
}
