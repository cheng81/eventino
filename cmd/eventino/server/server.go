package server

import (
	"fmt"
	"net"

	"github.com/cheng81/eventino/cmd/eventino/common/command"

	"github.com/cheng81/eventino/cmd/eventino/common"
	"github.com/cheng81/eventino/internal/eventino/schema/schemaavro"
	"github.com/cheng81/eventino/pkg/eventino"
	"github.com/dgraph-io/badger"
)

type Server interface {
	Start() error
	Stop() error
}

type srv struct {
	port   int
	lst    net.Listener
	closed bool

	db  *badger.DB
	svc eventino.Eventino
}

func (s *srv) handleCommand(cmd map[string]interface{}) (rsp []byte, err error) {
	if (&command.CreateEntityType{}).Is(cmd) {
		c := new(command.CreateEntityType)
		c.Decode(cmd)
		var vsn uint64
		if vsn, err = s.svc.CreateEntityType(c.Name); err != nil {
			return common.NetCodec.BinaryFromNative(nil, command.NewErrorMessage(err).Encode())
		}
		return common.NetCodec.BinaryFromNative(nil, (&command.SchemaResponse{Operation: "createEntityType", VSN: vsn}).Encode())
	}
	return
}

func (s *srv) Start() (err error) {
	if s.lst, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port)); err != nil {
		return
	}
	s.accept()
	return
}

func (s *srv) accept() {
	var conn net.Conn
	var err error
	for {
		if s.closed {
			return
		}
		if conn, err = s.lst.Accept(); err != nil {
			fmt.Println("srv.accept - accept failed", err)
			return
		}
		go s.handle(conn)
	}
}

func (s *srv) handle(conn net.Conn) {
	fmt.Println("handle conn")
	defer conn.Close()

	buf := make([]byte, 256*1024)
	for {
		var err error
		var wrote int
		fmt.Println("handle.wait conn read")
		if wrote, err = conn.Read(buf); err != nil {
			fmt.Println("handle.error reading", err)
			break
		}
		fmt.Println("handle.read", wrote, buf[0:wrote])
		var rsp []byte
		var cmd interface{}
		if cmd, _, err = common.NetCodec.NativeFromBinary(buf[0:wrote]); err != nil {
			fmt.Println("handle.cannot decode", err)
			break
		}
		if rsp, err = s.handleCommand(cmd.(map[string]interface{})); err != nil {
			fmt.Println("handle.exec failed", err)
			break
		}
		fmt.Println("handle.write response")
		if _, err = conn.Write(rsp); err != nil {
			break
		}
	}
	fmt.Println("closing connection")
}

func (s *srv) Stop() (err error) {
	s.closed = true
	s.lst.Close()
	s.db.Close()
	return
}

func NewServer(port int, opts badger.Options) (Server, error) {
	var db *badger.DB
	var err error
	if db, err = badger.Open(opts); err != nil {
		return nil, err
	}
	return &srv{
		port: port,
		db:   db,
		svc:  eventino.NewEventino(db, schemaavro.Factory()),
	}, nil
}
