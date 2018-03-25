package server

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/cheng81/eventino/cmd/eventino/common/command"
	"github.com/linkedin/goavro"

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

	db *badger.DB

	svc   eventino.Eventino
	codec *goavro.Codec
}

func (s *srv) handleCommand(cmd map[string]interface{}) (rsp []byte, err error) {
	if (&command.CreateEntityType{}).Is(cmd) {
		c := new(command.CreateEntityType)
		c.Decode(cmd)
		var vsn uint64
		if vsn, err = s.svc.CreateEntityType(c.Name); err != nil {
			return wrapErr(err)
		}
		return s.codec.BinaryFromNative(nil, (&command.SchemaResponse{Operation: "createEntityType", VSN: vsn}).Encode())
	} else if (&command.CreateEntityEventType{}).Is(cmd) {
		c := new(command.CreateEntityEventType)
		c.Decode(cmd)
		var vsn uint64
		if vsn, err = s.svc.CreateEventType(c.EntityType, c.EventName, c.MetaSchema); err != nil {
			return wrapErr(err)
		}
		return s.codec.BinaryFromNative(nil, (&command.SchemaResponse{Operation: "createEventType", VSN: vsn}).Encode())
	} else if (&command.LoadSchema{}).Is(cmd) {
		c := new(command.LoadSchema)
		c.Decode(cmd)
		var loadedVsn uint64
		var encoded []byte
		if loadedVsn, encoded, err = s.svc.LoadSchema(c.VSN); err != nil {
			return wrapErr(err)
		}
		// switch network codec
		var dataSchema map[string]interface{}
		if err = json.Unmarshal(encoded, &dataSchema); err != nil {
			return wrapErr(err)
		}
		var cdc *goavro.Codec
		if cdc, err = common.NetCodecWithSchema(dataSchema); err != nil {
			return wrapErr(err)
		}
		s.codec = cdc
		fmt.Println("loadSchema, new codec", s.codec.Schema())
		return s.codec.BinaryFromNative(nil, (&command.LoadSchemaReply{VSN: loadedVsn, Encoded: encoded}).Encode())
	} else if (&command.CreateEntity{}).Is(cmd) {
		c := new(command.CreateEntity)
		c.Decode(cmd)
		if err = s.svc.NewEntity(c.Type, c.ID); err != nil {
			return wrapErr(err)
		}
		return s.codec.BinaryFromNative(nil, map[string]interface{}{"boolean": true})
	} else if (&command.LoadEntity{}).Is(cmd) {
		c := new(command.LoadEntity)
		c.Decode(cmd)
		ent, err := s.svc.GetEntity(c.Type, c.ID, c.VSN)
		if err != nil {
			return wrapErr(err)
		}
		evts := make([]map[string]interface{}, len(ent.Events))
		for i, evt := range ent.Events {
			evtTypeID := evt.Type.ToString()
			evtNat := map[string]interface{}{
				evtTypeID: map[string]interface{}{
					"ts":   evt.Timestamp.UnixNano(),
					"data": evt.Payload,
				},
			}
			evts[i] = evtNat
		}
		entNative := map[string]interface{}{
			"id":         ent.ID,
			"schema_vsn": int64(ent.Type.VSN),
			"vsn":        int64(ent.VSN),
			"latest_vsn": int64(ent.LatestVSN),
			"events":     evts,
		}

		reply := map[string]interface{}{"data": map[string]interface{}{
			"entity_event": nil,
			"entity_load": map[string]interface{}{
				c.Type: entNative,
			},
		}}
		//replyDbg, _ := json.Marshal(reply)
		//fmt.Println("get.entity", string(replyDbg))
		return s.codec.BinaryFromNative(nil, reply) //map[string]interface{}{"null": nil}
	} else if command.IsCommand("string", cmd) && cmd["string"].(string) == "schema_vsn" {
		// load latest schema vsn
		v, err := s.svc.SchemaVSN()
		if err != nil {
			return wrapErr(err)
		}
		return s.codec.BinaryFromNative(nil, map[string]interface{}{"long": int64(v)})
	} else if command.IsData(cmd) {
		// put
		// get only key in map, to get the entity
		// get first non-nil fields in entity map
		cmd := cmd["data"].(map[string]interface{})["entity_event"].(map[string]interface{})
		var entName string
		var entMap map[string]interface{}
		for k, v := range cmd {
			entName = k
			entMap = v.(map[string]interface{})
			break
		}
		var entID []byte
		entID = entMap["id"].([]byte)

		evtMap := entMap["event"].(map[string]interface{})
		var evtIDenc string
		var evt interface{}
		for k, v := range evtMap {
			evtIDenc = k
			evt = v.(map[string]interface{})["data"]
			break
		}
		vsn, err := s.svc.Put(entName, entID, evtIDenc, evt)
		if err != nil {
			return wrapErr(err)
		}
		return s.codec.BinaryFromNative(nil, map[string]interface{}{"long": int64(vsn)})
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
		if cmd, _, err = s.codec.NativeFromBinary(buf[0:wrote]); err != nil {
			fmt.Println("handle.cannot decode", err)
			break
		}
		if rsp, err = s.handleCommand(cmd.(map[string]interface{})); err != nil {
			fmt.Println("handle.exec failed", err)
			break
		}
		fmt.Println("DBG - schema", s.codec.Schema())
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
		port:  port,
		db:    db,
		svc:   eventino.NewEventino(db, schemaavro.Factory()),
		codec: common.NetCodec,
	}, nil
}

func wrapErr(err error) ([]byte, error) {
	return common.NetCodec.BinaryFromNative(nil, command.NewErrorMessage(err).Encode())
}
