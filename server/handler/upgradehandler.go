package handler

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
)

// ClusterUpgradeHandler is the handler for upgrading cluster.
type ClusterUpgradeHandler struct {
	store kv.Storage
}

// NewClusterUpgradeHandler creates a new ClusterUpgradeHandler.
func NewClusterUpgradeHandler(store kv.Storage) *ClusterUpgradeHandler {
	return &ClusterUpgradeHandler{store: store}
}

// ServeHTTP handles request of ddl server info.
func (h ClusterUpgradeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)

	var err error
	var hasDone bool
	op := params[Operation]
	switch op {
	case "start":
		hasDone, err = h.startUpgrade()
	case "finish":
		hasDone, err = h.finishUpgrade()
	default:
		WriteError(w, errors.Errorf("wrong operation:%s", op))
		return
	}

	if err != nil {
		WriteError(w, err)
		return
	}
	if hasDone {
		switch op {
		case "start":
			WriteData(w, "Be upgrading.")
		case "finish":
			WriteData(w, "Be normal.")
		}
	}
	WriteData(w, "success!")
}

func (h ClusterUpgradeHandler) startUpgrade() (hasDone bool, err error) {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return false, err
	}
	defer se.Close()

	isUpgrading, err := session.IsUpgradingClusterState(se)
	if err != nil {
		return false, err
	}
	if isUpgrading {
		return true, nil
	}

	err = session.SyncUpgradeState(se)
	return false, err
}

func (h ClusterUpgradeHandler) finishUpgrade() (hasDone bool, err error) {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return false, err
	}
	defer se.Close()

	isUpgrading, err := session.IsUpgradingClusterState(se)
	if err != nil {
		return false, err
	}
	if !isUpgrading {
		return true, nil
	}

	err = session.SyncNormalRunning(se)
	return false, err
}
