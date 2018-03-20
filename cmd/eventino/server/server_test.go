package server

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/linkedin/goavro"
)

var scm *goavro.Codec

func init() {
	var err error
	scm, err = goavro.NewCodec(`
		{
			"type": "record",
			"name": "myrecord",
			"fields": [
				{"name": "foo", "type": "string"},
				{"name": "bar", "type": "int"}
			]
		}`)
	if err != nil {
		panic(err)
	}
}

func TestSrc(t *testing.T) {
	if scm == nil {
		t.Fatal("ups")
	}
	go server(t)
	time.Sleep(500 * time.Millisecond)
	client()
	// if err != nil {
	// 	t.Fatal("client failed", err)
	// }
}

func client() {
	fmt.Println("client")
	var err error
	var conn net.Conn

	if conn, err = net.Dial("tcp", ":7890"); err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("client.connected")

	r := map[string]interface{}{
		"foo": "the answer!",
		"bar": 42,
	}

	var b []byte
	if b, err = scm.BinaryFromNative(nil, r); err != nil {
		panic(err)
	}

	d := 50 * time.Millisecond
	sendM(1, conn, b, 0, 0)
	sendM(2, conn, b, 1, d)
	sendM(3, conn, b, 2, d)
	sendM(4, conn, b, 3, d)
	sendM(5, conn, b, 4, d)
	sendM(6, conn, b, 5, d)
	sendM(7, conn, b, 6, 0)
	sendM(8, conn, b, 7, d)
	sendM(9, conn, b, 8, d)
	sendM(10, conn, b, 9, d)

	fmt.Println("client.done")
}

func sendM(id int, conn net.Conn, b []byte, cut int, d time.Duration) {
	var err error
	sig := make([]byte, 1)
	if cut != 0 {
		if _, err = conn.Write(b[0:cut]); err != nil {
			panic(err)
		}
		fmt.Println("client.sent-a", id)
		time.Sleep(d)
		if _, err = conn.Write(b[cut:]); err != nil {
			panic(err)
		}
		fmt.Println("client.sent-b", id)
	} else {
		if _, err = conn.Write(b); err != nil {
			panic(err)
		}
		fmt.Println("client.sent", id)
	}
	if _, err = conn.Read(sig); err != nil {
		panic(err)
	}

	fmt.Println("client.recv.ack", id)
}

func server(t *testing.T) {
	fmt.Println("server")
	var err error
	var l net.Listener
	if l, err = net.Listen("tcp", ":7890"); err != nil {
		panic(err)
	}
	fmt.Println("server.listen")
	defer l.Close()

	for {
		conn, err := l.Accept()
		fmt.Println("server.accepted")

		if err != nil {
			panic(err)
		}
		cb := func(buf *circbuf) error {
			fmt.Println("server.circbuf.cb called")

			b, err := buf.ReadAll()
			if err != nil {
				return err
			}
			fmt.Println("server.circbuf.cb read", b)
			res, newb, err := scm.NativeFromBinary(b)
			if err != nil {
				fmt.Println("server.circbuf.cb - continue", err)
				return nil
			}
			buf.Consume(len(b) - len(newb))
			resM := res.(map[string]interface{})
			if resM["foo"].(string) != "the answer!" {
				t.Fatal("foo is not the answer!", resM)
			}
			if resM["bar"].(int32) != 42 {
				t.Fatal("bar is not 42", resM)
			}
			fmt.Printf("server.recvd: %+v\n", resM)
			conn.Write([]byte{0})
			return nil
		}
		buf := NewCircbuf(15, cb)
		_, err = io.Copy(buf, conn)
		return
	}

}
