package schemaavro

import (
	"encoding/json"

	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/linkedin/goavro"
)

var basicSchemaCodec *goavro.Codec

type basicSchema struct {
	t    schema.DataType
	jScm map[string]interface{}
	scm  *goavro.Codec
}

func (b *basicSchema) AvroNativeMeta() map[string]interface{} {
	var ts string
	switch b.t {
	case schema.Null:
		ts = "NULL"
	case schema.Bool:
		ts = "BOOLEAN"
	case schema.String:
		ts = "STRING"
	case schema.Int64:
		ts = "LONG"
	case schema.Float64:
		ts = "DOUBLE"
	case schema.Bytes:
		ts = "BYTES"
	}
	return map[string]interface{}{
		"Simple": ts,
	}
}

func (b *basicSchema) AvroNative() map[string]interface{} {
	return b.jScm
}

func (b *basicSchema) SchemaDecoder() schema.SchemaDecoder {
	return avroSchemaDecoder{}
}

func (b *basicSchema) EncodeSchema() ([]byte, error) {
	return avroSchemaCodec.BinaryFromNative(nil, b.AvroNativeMeta())
}

func (b *basicSchema) EncodeSchemaNative() interface{} {
	return b.AvroNativeMeta()
}

func (b *basicSchema) Encoder() schema.DataEncoder {
	return b
}

func (b *basicSchema) Decoder() schema.DataDecoder {
	return b
}

func (b *basicSchema) Valid(v interface{}) (out bool) {
	switch b.t {
	case schema.Null:
		out = v == nil
	case schema.Bool:
		_, out = v.(bool)
	case schema.String:
		_, out = v.(string)
	case schema.Int64:
		_, out = v.(int64)
	case schema.Float64:
		_, out = v.(float64)
	case schema.Bytes:
		_, out = v.([]byte)
	}
	return
}

// DataEncoder
func (b *basicSchema) Encode(v interface{}) ([]byte, error) {
	return b.scm.BinaryFromNative(nil, v)
}

// DataDecoder
func (b *basicSchema) Decode(buf []byte) (interface{}, error) {
	out, _, err := b.scm.NativeFromBinary(buf)
	return out, err
}

func newBasicSchema(t schema.DataType, schemaSpecs string) *basicSchema {
	codec, err := goavro.NewCodec(schemaSpecs)
	if err != nil {
		panic(err)
	}
	var jsonScm map[string]interface{}
	json.Unmarshal([]byte(schemaSpecs), &jsonScm)
	return &basicSchema{t: t, scm: codec, jScm: jsonScm}
}

var nilSchema *basicSchema
var boolSchema *basicSchema
var stringSchema *basicSchema
var longSchema *basicSchema
var doublueSchema *basicSchema
var bytesSchema *basicSchema

func init() {
	nilSchema = newBasicSchema(schema.Null, `{"type":"null"}`)
	boolSchema = newBasicSchema(schema.Bool, `{"type":"boolean"}`)
	stringSchema = newBasicSchema(schema.String, `{"type":"string"}`)
	longSchema = newBasicSchema(schema.String, `{"type":"long"}`)
	doublueSchema = newBasicSchema(schema.String, `{"type":"double"}`)
	bytesSchema = newBasicSchema(schema.String, `{"type":"bytes"}`)
}
