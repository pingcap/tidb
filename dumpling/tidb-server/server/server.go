package server

import (
	"crypto/rand"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/arena"
	"github.com/ngaut/log"
	"github.com/ngaut/tokenlimiter"
	"github.com/pingcap/tidb/mysqldef"
)

var (
	baseConnID uint32 = 10000
)

// Server is the MySQL protocol server
type Server struct {
	cfg               *Config
	driver            IDriver
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *tokenlimiter.TokenLimiter
	clients           map[uint32]*clientConn
}

func (s *Server) getToken() *tokenlimiter.Token {
	return s.concurrentLimiter.Get()
}

func (s *Server) releaseToken(token *tokenlimiter.Token) {
	s.concurrentLimiter.Put(token)
}

func (s *Server) newConn(conn net.Conn) (cc *clientConn, err error) {
	log.Info("newConn", conn.RemoteAddr().String())
	cc = &clientConn{
		conn:         conn,
		pkg:          newPacketIO(conn),
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysqldef.DefaultCollationID,
		charset:      mysqldef.DefaultCharset,
		alloc:        arena.NewArenaAllocator(32 * 1024),
	}
	cc.salt = make([]byte, 20)
	io.ReadFull(rand.Reader, cc.salt)
	for i, b := range cc.salt {
		if b == 0 {
			cc.salt[i] = '0'
		}
	}
	return
}

func (s *Server) skipAuth() bool {
	return s.cfg.SkipAuth
}

func (s *Server) cfgGetPwd(user string) string {
	return s.cfg.Password //TODO support multiple users
}

// NewServer creates a new Server.
func NewServer(cfg *Config, driver IDriver) (*Server, error) {
	log.Warningf("%#v", cfg)
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: tokenlimiter.NewTokenLimiter(100),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]*clientConn),
	}

	var err error
	s.listener, err = net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("Server run MySql Protocol Listen at [%s]", s.cfg.Addr)
	return s, nil
}

// Run runs the server.
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept error %s", err.Error())
			return err
		}

		go s.onConn(conn)
	}
}

// Close closes the server.
func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

func (s *Server) onConn(c net.Conn) {
	conn, err := s.newConn(c)
	if err != nil {
		log.Errorf("newConn error %s", errors.ErrorStack(err))
		return
	}
	if err := conn.handshake(); err != nil {
		log.Errorf("handshake error %s", errors.ErrorStack(err))
		c.Close()
		return
	}
	conn.ctx, err = s.driver.OpenCtx(conn.capability, uint8(conn.collation), conn.dbname)
	if err != nil {
		log.Errorf("open ctx error %s", errors.ErrorStack(err))
		c.Close()
		return
	}

	const key = "connections"

	defer func() {
		log.Infof("close %s", conn)
	}()

	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	s.rwlock.Unlock()

	conn.Run()
}
