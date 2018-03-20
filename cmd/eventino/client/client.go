package client

import (
	"errors"
	"fmt"
	"net"

	"github.com/cheng81/eventino/cmd/eventino/common/command"

	"github.com/cheng81/eventino/cmd/eventino/common"

	"github.com/cheng81/eventino/pkg/eventino"
)

type Client interface {
	Eventino() eventino.Eventino
	Start() error
	Stop() error
}

func NewClient(addr string, port int) Client {
	return &client{addr: addr, port: port}
}

type client struct {
	port int
	addr string
	conn net.Conn
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
	if b, err = common.NetCodec.BinaryFromNative(nil, cmd); err != nil {
		return nil, err
	}
	if _, err = c.conn.Write(b); err != nil {
		fmt.Println("exec.write req failed", err)
		return nil, err
	}
	var out interface{}
	fmt.Println("exec.read reply")
	b = make([]byte, 256*1024)
	var wrote int
	if wrote, err = c.conn.Read(b); err != nil {
		fmt.Println("exec.read reply failed", err)
		return nil, err
	}
	if out, _, err = common.NetCodec.NativeFromBinary(b[0:wrote]); err != nil {
		fmt.Println("circuf.callback - err decode", err)
		return nil, err
	}

	fmt.Println("exec, io ended", out, err)
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
	// must be an error?
	errorMsg := &command.ErrorResponse{}
	errorMsg.Decode(rsp)
	return 0, errors.New(errorMsg.Message)
}
