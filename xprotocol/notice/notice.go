package notice

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Notice"
)

// notice message sent to client.
type notice struct {
	noticeType noticeType
	value      []byte
	pkt        *xpacketio.XPacketIO
}

type noticeType uint32

const (
	noticeWarning                noticeType = 1
	noticeSessionVariableChanged            = 2
	noticeSessionStateChanged               = 3
)

func (n *notice) sendLocalNotice(forceFlush bool) error {
	return n.sendNotice(Mysqlx_Notice.Frame_LOCAL, forceFlush)
}

func (n *notice) sendNotice(scope Mysqlx_Notice.Frame_Scope, forceFlush bool) error {
	frameType := uint32(n.noticeType)
	msg := Mysqlx_Notice.Frame{
		Type:    &frameType,
		Scope:   &scope,
		Payload: n.value,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	if err := n.pkt.WritePacket(Mysqlx.ServerMessages_NOTICE, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SendNoticeOK send notice message 'ok' to client, this is different from server message ok and
// sql statement exec message ok.
func SendNoticeOK(pkt *xpacketio.XPacketIO, content string) error {
	msg := Mysqlx.Ok{
		Msg: &content,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n := notice{
		noticeType: noticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return n.sendLocalNotice(false)
}

// SendWarnings is used to send warnings after query.
func SendWarnings() {

}

// SendRowsAffected is used to number of rows which are affected.
func SendRowsAffected(pkt *xpacketio.XPacketIO, numRows uint64) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_ROWS_AFFECTED)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &numRows,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n := notice{
		noticeType: noticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return n.sendLocalNotice(false)
}

// SendMessage is used to send server a message.
func SendMessage() {

}

// SendLastInsertID send a notice which contains last insert ID.
func SendLastInsertID(pkt *xpacketio.XPacketIO, lastID uint64) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_GENERATED_INSERT_ID)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	id := lastID
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &id,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n := notice{
		noticeType: noticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return n.sendLocalNotice(false)
}

// SendClientID send client id to client
func SendClientID(pkt *xpacketio.XPacketIO, sessionID uint32) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_CLIENT_ID_ASSIGNED)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	id := uint64(sessionID)
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &id,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n := notice{
		noticeType: noticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return n.sendLocalNotice(false)
}
