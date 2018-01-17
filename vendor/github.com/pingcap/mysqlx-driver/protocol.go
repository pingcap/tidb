// Go driver for MySQL X Protocol
// Based heavily on Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Notice"
	"github.com/pingcap/tipb/go-mysqlx/Session"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

var (
	// this seems not to be defined in the protobuf specification
	mysqlxNoticeTypeName = map[uint32]string{
		1: "Warning",
		2: "SessionVariableChanged",
		3: "SessionStateChanged",
	}
)

// netProtobuf holds the protobuf message type and the network bytes from a protobuf message
// - see docs at ....
type netProtobuf struct {
	msgType int
	payload []byte
}

// Read a raw netProtobuf packet from the network and return a pointer to the structure
func (mc *mysqlXConn) readMsg() (*netProtobuf, error) {
	// Read packet header
	data, err := mc.buf.readNext(4)
	if err != nil {
		if err = mc.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, driver.ErrBadConn
	}

	// Packet Length [32 bit]
	pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)

	if pktLen < minPacketSize {
		if err = mc.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, driver.ErrBadConn
	}

	// Read body which is 1-byte msg type and 0+ bytes payload
	data, err = mc.buf.readNext(pktLen)
	if err != nil {
		if err := mc.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, driver.ErrBadConn
	}

	pb := &netProtobuf{
		msgType: int(data[0]),
	}
	if len(data) > 1 {
		pb.payload = data[1:]
	}

	return pb, nil
}

// Write packet buffer 'data'
func (mc *mysqlXConn) writeProtobufPacket(pb *netProtobuf) error {
	if pb == nil || pb.msgType > 255 {
		if err := mc.Close(); err != nil {
			return errors.Trace(err)
		}
		return ErrMalformPkt
	}

	pktLen := len(pb.payload) + 1

	if pktLen > mc.maxPacketAllowed {
		return ErrPktTooLarge
	}

	// setup initial header
	data := make([]byte, 5)

	var size int
	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	data[3] = byte(pktLen >> 24)
	data[4] = byte(pb.msgType)
	size = pktLen - 1

	// Write header
	n, err := mc.netConn.Write(data)
	if err != nil {
		return errors.Trace(err)
	} else if n != 5 {
		return errors.Errorf("Expect write 5 bytes but write %d bytes", n)
	}

	// Write payload
	n, err = mc.netConn.Write(pb.payload)
	if err != nil {
		return errors.Trace(err)
	} else if n != size {
		return errors.Errorf("Expect write %d bytes but write %d bytes", size, n)
	}
	return nil
}

/******************************************************************************
*                           Initialisation Process                            *
******************************************************************************/

// helper function - check this is a a scalar type
func isScalar(value *Mysqlx_Datatypes.Any) bool {
	return value != nil && *value.Type == Mysqlx_Datatypes.Any_SCALAR
}

// helper function - check this is a scalar VString
func isScalarString(value *Mysqlx_Datatypes.Any) bool {
	return value != nil && *value.Type == Mysqlx_Datatypes.Any_SCALAR && *value.Scalar.Type == Mysqlx_Datatypes.Scalar_V_STRING
}

// helper function - check this is a a scalar VBool
func isScalarBool(value *Mysqlx_Datatypes.Any) bool {
	return value != nil && *value.Type == Mysqlx_Datatypes.Any_SCALAR && *value.Scalar.Type == Mysqlx_Datatypes.Scalar_V_BOOL
}

// helper function - return the scalar VString as a string
func scalarString(value *Mysqlx_Datatypes.Any) string {
	if !isScalarString(value) {
		return ""
	}
	return string(value.Scalar.VString.Value)
}

// helper function - return the scalar VBool as a bool
func scalarBool(value *Mysqlx_Datatypes.Any) bool {
	if !isScalarBool(value) {
		return false
	}
	return bool(*value.Scalar.VBool)
}

// helper function - check this is an array of VString
func isArrayString(value *Mysqlx_Datatypes.Any) bool {
	if value == nil {
		return false
	}
	if value.GetType() != Mysqlx_Datatypes.Any_ARRAY {
		return false
	}
	for i := range value.GetArray().GetValue() {
		if value.GetArray().GetValue()[i].GetType() != Mysqlx_Datatypes.Any_SCALAR {
			return false
		}
		if value.GetArray().GetValue()[i].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
			return false
		}
	}

	return true
}

// helper function - return the array of scalar VString as a []string
func arrayString(value *Mysqlx_Datatypes.Any) []string {
	if !isArrayString(value) {
		return nil
	}
	values := []string{}

	for i := range value.GetArray().GetValue() {
		values = append(values, scalarString(value.GetArray().GetValue()[i]))
	}

	return values
}

// Request the getCapabilities
// see: http://.....
func (mc *mysqlXConn) getCapabilities() error {

	if err := mc.writeConnCapabilitiesGet(); err != nil {
		return errors.Trace(err)
	}

	var pb *netProtobuf
	var err error
	done := false
	// wait for the answer CONN_CAPABILITIES, but handle ERROR or NOTICE
	for !done {
		if pb, err = mc.readMsg(); err != nil {
			return err
		}
		if pb == nil {
			return errors.Errorf("getCapabilities() pb = nil (not expected to happen ever)")
		}

		switch Mysqlx.ServerMessages_Type(pb.msgType) {
		case Mysqlx.ServerMessages_ERROR:
			return errors.Errorf("getCapabilities returned: %+v", errorMsg(pb.payload))
		case Mysqlx.ServerMessages_CONN_CAPABILITIES:
			done = true
		case Mysqlx.ServerMessages_NOTICE: // we don't expect a notice here so just print it.
			mc.pb = pb // hack though maybe should always use mc
			// process the notice message
			if err := mc.processNotice("getCapabilities"); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if pb == nil {
		return errors.Errorf("BUG: Empty pb")
	}
	if pb.payload == nil {
		return errors.Errorf("BUG: Empty pb.payload")
	}

	// get the capabilities info
	capabilities := &Mysqlx_Connection.Capabilities{}
	if err := proto.Unmarshal(pb.payload, capabilities); err != nil {
		return errors.Trace(err)
	}

	for i := range capabilities.GetCapabilities() {
		name := capabilities.GetCapabilities()[i].GetName()
		value := capabilities.GetCapabilities()[i].GetValue()
		if isScalar(value) {
			if isScalarString(value) {
				scalar := scalarString(value)
				if err := mc.capabilities.AddScalarString(name, scalar); err != nil {
					return errors.Trace(err)
				}
			} else if isScalarBool(value) {
				scalar := scalarBool(value)
				if err := mc.capabilities.AddScalarBool(name, scalar); err != nil {
					return errors.Trace(err)
				}
			}
		} else if isArrayString(value) {
			values := arrayString(value)
			if err := mc.capabilities.AddArrayString(name, values); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// return a new boolean scalar of the given type
func newBoolScalar(value bool) *Mysqlx_Datatypes.Scalar {
	vBool := new(Mysqlx_Datatypes.Scalar_Type)
	*vBool = Mysqlx_Datatypes.Scalar_V_BOOL

	_value := new(bool)
	*_value = value

	// generate the value we want to use
	return &Mysqlx_Datatypes.Scalar{
		Type:  vBool,
		VBool: _value,
	}
}

// return a datatype any of type scalar
func newAnyScalar(anyType Mysqlx_Datatypes.Any_Type, scalar *Mysqlx_Datatypes.Scalar) *Mysqlx_Datatypes.Any {
	someType := new(Mysqlx_Datatypes.Any_Type)
	*someType = anyType

	return &Mysqlx_Datatypes.Any{
		Type:   someType,
		Scalar: scalar,
	}
}

// set a boolean scalar capability (tls probably)
func (mc *mysqlXConn) setScalarBoolCapability(name string, value bool) error {

	// Wow this is long-winded and harder than I'd expect (even
	// for a trivial setup like this)
	// - definitely need some helper routines above the basic stuff that Go provides.
	// - and good we don't need to free up all this stuff
	//   ourselves afterwards and can leave it to Go's garbage
	//   collector.

	any := newAnyScalar(Mysqlx_Datatypes.Any_SCALAR, newBoolScalar(value))

	// setup capability structure from what we've just created
	capability := &Mysqlx_Connection.Capability{
		Name:  proto.String(name),
		Value: any,
	}

	var capabilitiesArray []*Mysqlx_Connection.Capability
	capabilitiesArray = append(capabilitiesArray, capability)

	capabilities := &Mysqlx_Connection.Capabilities{
		Capabilities: capabilitiesArray,
	}

	// setup CapabilitiesSet structure from what we've just created
	capabilitiesSet := &Mysqlx_Connection.CapabilitiesSet{
		Capabilities: capabilities,
	}

	var err error
	pb := new(netProtobuf)
	pb.msgType = int(Mysqlx.ClientMessages_CON_CAPABILITIES_SET)
	if pb.payload, err = proto.Marshal(capabilitiesSet); err != nil {
		return errors.Trace(err)
	}

	// Send the message
	if err = mc.writeProtobufPacket(pb); err != nil {
		return errors.Trace(err)
	}

	// wait for the answer (I expect it to be OK / ERROR)
	done := false
	// wait for the answer OK, ERROR or NOTICE
	for !done {
		if pb, err = mc.readMsg(); err != nil {
			return err
		}
		if pb == nil {
			return errors.Errorf("setScalarBoolCapability() pb = nil (not expected to happen ever)")
		}

		switch Mysqlx.ServerMessages_Type(pb.msgType) {
		case Mysqlx.ServerMessages_OK:
			return nil
		case Mysqlx.ServerMessages_ERROR:
			return errors.Errorf("setScalarBoolCapability failed: %v", mc.processErrorMsg())
		case Mysqlx.ServerMessages_NOTICE:
			// we don't expect a notice here so just print it.
			mc.pb = pb // should use just mc.pb ??
			if err := mc.processNotice("setScalarBoolCapability"); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if pb == nil {
		return errors.Errorf("BUG: Empty pb")
	}
	if pb.payload == nil {
		return errors.Errorf("BUG: Empty pb.payload")
	}

	return nil
}

// generate an error based on the Mysql.Error message
func errorText(e *Mysqlx.Error) error {
	if e == nil {
		return errors.Errorf("errorText: ERROR e == nil")
	}
	return errors.Errorf("%v: %04d [%s] %s", e.Severity, *(e.Code), *(e.SqlState), *(e.Msg))
}

// return an error message type as an error
// - pb MUST BE a protobuf message of type Error
func (pb *netProtobuf) errorMsg() error {
	if pb == nil {
		return errors.Errorf("errorMsg: ERROR pb == nil")
	}
	if pb.msgType != int(Mysqlx.ServerMessages_ERROR) {
		return errors.Errorf("errorMsg: ERROR msgType = %d, expecting ERROR (%d)", pb.msgType, Mysqlx.ServerMessages_ERROR)
	}
	e := new(Mysqlx.Error)
	if err := proto.Unmarshal(pb.payload, e); err != nil {
		return errors.Trace(err)
	}
	return errorText(e)
}

// return an error message type as an error
// - input is a protobuf message of type Error
func errorMsg(data []byte) error {
	e := new(Mysqlx.Error)
	if err := proto.Unmarshal(data, e); err != nil {
		return errors.Trace(err)
	}
	return errorText(e)
}

func printAuthenticateOk(data []byte) {
	ok := &Mysqlx_Session.AuthenticateOk{}
	if err := proto.Unmarshal(data, ok); err != nil {
		log.Fatal("unmarshaling error with AuthenticateOk: ", err)
	}
}

func (mc *mysqlXConn) processNotice(where string) error {
	if mc == nil {
		return errors.Errorf("mysqlXConn.processNotice(%q): mc == nil", where)
	}
	if mc.pb == nil {
		return errors.Errorf("mysqlXConn.processNotice(%q): mc.pb == nil", where)
	}

	f := new(Mysqlx_Notice.Frame)
	if err := proto.Unmarshal(mc.pb.payload, f); err != nil {
		log.Fatalf("error unmarshaling Notice f: %v", err)
	}

	switch f.GetType() {
	case 1: // warning
		w := new(Mysqlx_Notice.Warning)
		if err := proto.Unmarshal(f.Payload, w); err != nil {
			return errors.Trace(err)
		}
	case 2: // session variable change
		s := new(Mysqlx_Notice.SessionVariableChanged)
		if err := proto.Unmarshal(f.Payload, s); err != nil {
			return errors.Trace(err)
		}
	case 3: // SessionStateChanged
		s := new(Mysqlx_Notice.SessionStateChanged)
		if err := proto.Unmarshal(f.Payload, s); err != nil {
			return errors.Trace(err)
		}
	}
	mc.pb = nil // reset message (as now processed)
	return nil
}

func (mc *mysqlXConn) writeConnCapabilitiesGet() error {
	pb := new(netProtobuf)
	pb.msgType = int(Mysqlx.ClientMessages_CON_CAPABILITIES_GET)
	// EMPTY PAYLOAD
	return mc.writeProtobufPacket(pb)
}

func (mc *mysqlXConn) writeSessAuthenticateStart(m *Mysqlx_Session.AuthenticateStart) error {
	var err error

	pb := new(netProtobuf)
	pb.msgType = int(Mysqlx.ClientMessages_SESS_AUTHENTICATE_START)
	pb.payload, err = proto.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return mc.writeProtobufPacket(pb)
}

func (mc *mysqlXConn) writeSessAuthenticateContinue(m *Mysqlx_Session.AuthenticateContinue) error {
	var err error

	pb := new(netProtobuf)
	pb.msgType = int(Mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE)
	pb.payload, err = proto.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return mc.writeProtobufPacket(pb)
}

func readSessAuthenticateContinue(pb *netProtobuf) *Mysqlx_Session.AuthenticateContinue {
	authenticateContinue := &Mysqlx_Session.AuthenticateContinue{}
	if err := proto.Unmarshal(pb.payload, authenticateContinue); err != nil {
		log.Fatal("unmarshaling error with authenticateContinue: ", err)
	}

	return authenticateContinue
}

// AuthenticateMySQL41 uses MYSQL41 authentication method
func (mc *mysqlXConn) AuthenticateMySQL41() error {
	// ------------------------------------------------------------------------
	// C -> S   SESS_AUTHENTICATE_START
	// ------------------------------------------------------------------------
	authInfo := NewMySQL41(mc.cfg.dbname, mc.cfg.user, mc.cfg.passwd) // copy me into AuthData: ... (and adjust)

	// create the protobuf message (AuthenticateStart)
	msg := &Mysqlx_Session.AuthenticateStart{
		MechName: proto.String("MYSQL41"),
		AuthData: []byte(authInfo.GetInitialAuthData()),
	}
	if err := mc.writeSessAuthenticateStart(msg); err != nil {
		return errors.Trace(err)
	}

	// ------------------------------------------------------------------------
	// S -> C   SESS_AUTHENTICATE_CONTINUE
	// ------------------------------------------------------------------------

	// wait for the answer
	pb, err := mc.readMsg()
	if err != nil {
		return err
	}

	if Mysqlx.ServerMessages_Type(pb.msgType) != Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE {
		return errors.Errorf("Got unexpected message type back: %s, expecting: %s",
			printableMsgTypeIn(Mysqlx.ServerMessages_Type(pb.msgType)),
			printableMsgTypeIn(Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE))
	}

	authenticateContinue := readSessAuthenticateContinue(pb)
	authData := []byte(authenticateContinue.GetAuthData())
	if len(authData) != 20 {
		return errors.Errorf("Received %d bytes from server, expecting: 20", len(authData))
	}

	// ------------------------------------------------------------------------
	// C -> S   SESS_AUTHENTICATE_CONTINUE with scrambled password
	// ------------------------------------------------------------------------
	{
		authenticateContinue := &Mysqlx_Session.AuthenticateContinue{}
		response, err := authInfo.GetNextAuthData(authData)
		if err != nil {
			return errors.Trace(err)
		}
		authenticateContinue.AuthData = []byte(response)

		if err := mc.writeSessAuthenticateContinue(authenticateContinue); err != nil {
			return errors.Trace(err)
		}
	}

	// ------------------------------------------------------------------------
	// S -> C   SESS_AUTHENTICATE_OK / ERROR / NOTICE
	// ------------------------------------------------------------------------
	if err := mc.waitingForAuthenticateOk(); err != nil {
		return errors.Trace(err)
	}

	printAuthenticateOk(mc.pb.payload)
	mc.pb = nil // treat the incoming message as processsed
	return nil  // supposedly we have done the right thing
}

// waitingForAuthenticateOk is expecting to receive SESS_AUTHENTICATE_OK indicating success.
// We may get an error (of the form: 1045 [HY000] Invalid user or password which needs
// to be passed to the caller so that the connection is closed.
func (mc *mysqlXConn) waitingForAuthenticateOk() error {
	var err error
	done := false
	for !done {
		mc.pb, err = mc.readMsg()
		if err != nil {
			return errors.Trace(err)
		}

		switch Mysqlx.ServerMessages_Type(mc.pb.msgType) {
		case Mysqlx.ServerMessages_SESS_AUTHENTICATE_OK:
			done = true /* fall through */
		case Mysqlx.ServerMessages_ERROR:
			return errorMsg(mc.pb.payload)
		case Mysqlx.ServerMessages_NOTICE:
			// Not currently documented (explicitly) but we always get this type of message prior to SESS_AUTHENTICATE_OK
			if err := mc.processNotice("waitingForAuthenticateOk"); err != nil {
				return errors.Trace(err)
			}
		default:
			log.Fatalf("mysqlXConn.waitingfor_SESS_AUTHENTICATE_OK: Received unexpected message type: %s, expecting: %s",
				printableMsgTypeIn(Mysqlx.ServerMessages_Type(mc.pb.msgType)),
				printableMsgTypeIn(Mysqlx.ServerMessages_OK))
		}
	}
	return nil
}

func printableMsgTypeIn(i Mysqlx.ServerMessages_Type) string {
	return fmt.Sprintf("%d [%s]", i, Mysqlx.ServerMessages_Type_name[int32(i)])
}

func printableMsgTypeOut(i Mysqlx.ClientMessages_Type) string {
	return fmt.Sprintf("%d [%s]", i, Mysqlx.ClientMessages_Type_name[int32(i)])
}

// FIXME - there must be a protobuf function I can call - FIXME

func noticeTypeToName(t uint32) string {
	if name, found := mysqlxNoticeTypeName[t]; found {
		return name
	}
	return "?"
}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func getSystemVarXProtocol(name string, mc *mysqlXConn) ([]byte, error) {
	return nil, nil
}

// write a StmtExecute packet with the given query
func (mc *mysqlXConn) writeStmtExecute(stmtExecute *Mysqlx_Sql.StmtExecute) error {
	var err error

	pb := new(netProtobuf)
	pb.msgType = int(Mysqlx.ClientMessages_SQL_STMT_EXECUTE)
	pb.payload, err = proto.Marshal(stmtExecute)

	if err != nil {
		log.Fatalf("Failed to marshall message: %+v: %v", stmtExecute, err)
	}

	err = mc.writeProtobufPacket(pb)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Send a close message - no data to send so don't expose the protobuf info
func (mc *mysqlXConn) writeClose() error {
	payload, err := proto.Marshal(new(Mysqlx_Session.Close))
	if err != nil {
		return errors.Trace(err)
	}
	pb := &netProtobuf{
		msgType: int(Mysqlx.ClientMessages_SESS_CLOSE),
		payload: payload,
	}

	err = mc.writeProtobufPacket(pb)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// show the error msg and eat it up
func (mc *mysqlXConn) processErrorMsg() error {
	if mc == nil {
		return errors.Errorf("processErrorMsg mc == nil")
	}
	if mc.pb == nil {
		return errors.Errorf("processErrorMsg mc.pb == nil")
	}
	if mc.pb.payload == nil {
		return errors.Errorf("processErrorMsg mc.pb.payload == nil")
	}
	e := new(Mysqlx.Error)
	if err := proto.Unmarshal(mc.pb.payload, e); err != nil {
		return errors.Trace(err)
	}
	mc.pb = nil
	return nil
}

// is this data printable?
func isPrintable(b []byte) bool {
	p := true
	for i := range b {
		if b[i] < 32 || b[i] > 126 {
			return false
		}
	}
	return p
}
