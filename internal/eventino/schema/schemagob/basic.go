package schemagob

import (
	"github.com/cheng81/eventino/internal/eventino/schema"
)

type basicSchema struct {
	t schema.DataType
}

func (b *basicSchema) SchemaDecoder() schema.SchemaDecoder {
	return gobSchemaDecoder{}
}
func (b *basicSchema) EncodeSchema() ([]byte, error) {
	return []byte{byte(b.t)}, nil
}
func (b *basicSchema) Encoder() schema.DataEncoder {
	return GenericGobEncoder(encode)
}
func (b *basicSchema) Decoder() schema.DataDecoder {
	switch b.t {
	case schema.Null:
		return GenericGobDecoder(func(b []byte) (interface{}, error) {
			return nil, nil
		})
	case schema.Bool:
		return GenericGobDecoder(func(b []byte) (interface{}, error) {
			var v bool
			err := decode(b, &v)
			return v, err
		})
	case schema.String:
		return GenericGobDecoder(func(b []byte) (interface{}, error) {
			var v string
			err := decode(b, &v)
			return v, err
		})
	}
	return nil
	// factory := func() interface{} {
	// 	switch b.t {
	// 	case schema.Bool:
	// 		return false
	// 	case schema.String:
	// 		return ""
	// 	}
	// 	return nil
	// }
	// return genericDecoder(factory)
}
func (b *basicSchema) Valid(v interface{}) (out bool) {
	switch b.t {
	case schema.Null:
		out = v == nil
	case schema.Bool:
		_, out = v.(bool)
	case schema.String:
		_, out = v.(string)
	}
	return
}

var nilSchema *basicSchema
var boolSchema *basicSchema
var stringSchema *basicSchema

func init() {
	nilSchema = &basicSchema{t: schema.Null}
	boolSchema = &basicSchema{t: schema.Bool}
	stringSchema = &basicSchema{t: schema.String}
}
