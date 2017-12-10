package api

import (
	"net/http"

	"github.com/unrolled/render"
)

type version struct {
	Version string `json:"version"`
}

type versionHandler struct {
	rd *render.Render
}

func newVersionHandler(rd *render.Render) *versionHandler {
	return &versionHandler{
		rd: rd,
	}
}

func (h *versionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version := &version{
		Version: "1.0.0",
	}
	h.rd.JSON(w, http.StatusOK, version)
}
