package schemagob

import (
	"bytes"
	"encoding/gob"

	"github.com/cheng81/eventino/internal/eventino/schema"
)

type GenericGobEncoder func(interface{}) ([]byte, error)

func (enc GenericGobEncoder) Encode(v interface{}) ([]byte, error) {
	return enc(v)
}

type GenericGobDecoder func([]byte) (interface{}, error)

func (dec GenericGobDecoder) Decode(b []byte) (interface{}, error) {
	return dec(b)
}

func genericDecoder(factory func() interface{}) schema.DataDecoder {
	return GenericGobDecoder(func(b []byte) (interface{}, error) {
		v := factory()
		err := decode(b, &v)
		return v, err
	})
}

func encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(b []byte, v interface{}) error {
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	return dec.Decode(v)
}
