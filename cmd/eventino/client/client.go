package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cheng81/eventino/internal/eventino/entity"

	"github.com/linkedin/goavro"

	"github.com/cheng81/eventino/cmd/eventino/common/command"

	"github.com/cheng81/eventino/cmd/eventino/common"

	"github.com/cheng81/eventino/pkg/eventino"
)

type Client interface {
	AvroSchema() string
	Eventino() eventino.Eventino
	Start() error
	Stop() error
}

func NewClient(addr string, port int) Client {
	return &client{addr: addr, port: port, codec: common.NetCodec}
}

type client struct {
	port int
	addr string
	conn net.Conn

	codec *goavro.Codec
}

func (c *client) AvroSchema() string {
	return c.codec.Schema()
}

func (c *client) Eventino() eventino.Eventino {
	return c
}

func (c *client) Start() (err error) {
	c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.addr, c.port))
	return
}

func (c *client) Stop() error {
	return c.conn.Close()
}

func (c *client) exec(cmd interface{}) (map[string]interface{}, error) {
	fmt.Println("exec.called", cmd)
	var b []byte
	var err error
	if b, err = c.codec.BinaryFromNative(nil, cmd); err != nil {
		fmt.Println("exec.encode failed", err)
		return nil, err
	}
	if _, err = c.conn.Write(b); err != nil {
		fmt.Println("exec.write req failed", err)
		return nil, err
	}
	var out interface{}
	// fmt.Println("exec.read reply")
	b = make([]byte, 256*1024)
	var wrote int
	if wrote, err = c.conn.Read(b); err != nil {
		fmt.Println("exec.read reply failed", err)
		return nil, err
	}
	if out, _, err = c.codec.NativeFromBinary(b[0:wrote]); err != nil {
		fmt.Println("circuf.callback - err decode", err)
		return nil, err
	}

	// fmt.Println("exec, io ended", out, err)
	return out.(map[string]interface{}), nil
}

func (c *client) CreateEntityType(name string) (uint64, error) {
	cmd := (&command.CreateEntityType{Name: name}).Encode()
	rsp, err := c.exec(cmd)
	if err != nil {
		return 0, err
	}
	fmt.Println("CreateEntityType.rsp", rsp)
	rsp1 := &command.SchemaResponse{}
	if rsp1.Is(rsp) {
		rsp1.Decode(rsp)
		vsn := rsp1.VSN
		return uint64(vsn), nil
	}
	return 0, decodeError(rsp)
}

func (c *client) CreateEventType(entName, name string, specs interface{}) (uint64, error) {
	cmd := (&command.CreateEntityEventType{
		EntityType: entName,
		EventName:  name,
		MetaSchema: specs.(map[string]interface{}),
	}).Encode()
	rsp, err := c.exec(cmd)
	if err != nil {
		return 0, err
	}
	fmt.Println("CreateEventType.rsp", rsp)
	rsp1 := &command.SchemaResponse{}
	if rsp1.Is(rsp) {
		rsp1.Decode(rsp)
		vsn := rsp1.VSN
		return uint64(vsn), nil
	}
	return 0, decodeError(rsp)
}

func (c *client) LoadSchema(vsn uint64) (uint64, []byte, error) {
	cmd := (&command.LoadSchema{VSN: vsn}).Encode()
	rsp, err := c.exec(cmd)
	if err != nil {
		fmt.Println("cannot load schema", err)
		return 0, nil, err
	}
	rsp1 := &command.LoadSchemaReply{}
	if rsp1.Is(rsp) {
		rsp1.Decode(rsp)
		var dataSchema map[string]interface{}
		err = json.Unmarshal(rsp1.Encoded, &dataSchema)
		if err != nil {
			return 0, nil, err
		}
		c.codec, err = common.NetCodecWithSchema(dataSchema)
		if err != nil {
			return 0, nil, err
		}
		return rsp1.VSN, rsp1.Encoded, nil
	}
	return 0, nil, decodeError(rsp)
}

func (c *client) NewEntity(entName string, ID []byte) error {
	cmd := (&command.CreateEntity{Type: entName, ID: ID}).Encode()
	rsp, err := c.exec(cmd)
	if err != nil {
		return err
	}
	if _, ok := rsp["boolean"]; ok {
		fmt.Println("Entity created.")
		return nil
	}
	return decodeError(rsp)
}

func (c *client) Put(entName string, entID []byte, evtIDenc string, evt interface{}) (uint64, error) {
	cmd := map[string]interface{}{
		"data": map[string]interface{}{
			"entity_event": map[string]interface{}{
				entName: map[string]interface{}{
					"id": entID,
					"event": map[string]interface{}{
						evtIDenc: map[string]interface{}{
							"data": evt,
						},
					},
				},
			},
		},
	}
	b, _ := json.Marshal(cmd)
	fmt.Println("data command: ", string(b))
	rsp, err := c.exec(cmd)
	if err != nil {
		return 0, err
	}
	if vsn, ok := rsp["long"]; ok {
		fmt.Println("Event saved.")
		return uint64(vsn.(int64)), nil
	}
	return 0, decodeError(rsp)
}

func (c *client) GetEntity(entName string, entID []byte, vsn uint64) (entity.Entity, error) {
	cmd := (&command.LoadEntity{Type: entName, ID: entID, VSN: vsn}).Encode()
	rsp, err := c.exec(cmd)
	out := entity.Entity{}
	if err != nil {
		return out, err
	}
	var ent map[string]interface{}
	for _, v := range rsp["data"].(map[string]interface{})["entity_load"].(map[string]interface{}) {
		ent = v.(map[string]interface{})
		break
	}

	out.Type = entity.EntityType{Name: entName, VSN: uint64(ent["schema_vsn"].(int64))}
	out.ID = ent["id"].([]byte)

	out.VSN = uint64(ent["vsn"].(int64))
	out.LatestVSN = uint64(ent["latest_vsn"].(int64))

	evts := ent["events"].([]interface{})
	out.Events = make([]entity.EntityEvent, len(evts))
	for i, v := range evts {
		var eName string
		var eVal interface{}
		var ts int64
		for k, vv := range v.(map[string]interface{}) {
			eName = k
			ts = vv.(map[string]interface{})["ts"].(int64)
			eVal = vv.(map[string]interface{})["data"]
			break
		}
		entEvt := entity.EntityEvent{
			Type:      entity.EventNameIDFromString(eName),
			Timestamp: time.Unix(0, ts),
			Payload:   eVal,
		}
		out.Events[i] = entEvt
	}

	// fmt.Printf("GOT ENTITY %+v\n", out)
	return out, nil

}

func (c *client) SchemaVSN() (uint64, error) {
	cmd := map[string]interface{}{"string": "schema_vsn"}
	rsp, err := c.exec(cmd)
	if err != nil {
		return 0, err
	}
	if vsn, ok := rsp["long"]; ok {
		return uint64(vsn.(int64)), nil
	}
	return 0, decodeError(rsp)
}

func decodeError(m map[string]interface{}) error {
	errorMsg := &command.ErrorResponse{}
	errorMsg.Decode(m)
	fmt.Println(">> ERROR >>", errorMsg.Message)
	return errors.New(errorMsg.Message)
}
