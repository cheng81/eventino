package schemaavro

import (
	"encoding/json"
	"reflect"

	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/linkedin/goavro"
)

type avroArraySchema struct {
	items schema.DataSchema
	scm   *goavro.Codec
	jScm  map[string]interface{}
}

func newArray(items schema.DataSchema) schema.DataSchema {
	jScm := map[string]interface{}{
		"type":  "array",
		"items": items.(avroSchema).AvroNative(),
	}
	b, err := json.Marshal(jScm)
	if err != nil {
		panic(err)
	}
	scm, err := goavro.NewCodec(string(b))
	if err != nil {
		panic(err)
	}
	return &avroArraySchema{items: items, scm: scm, jScm: jScm}
}

func (*avroArraySchema) SchemaDecoder() schema.SchemaDecoder {
	return avroSchemaDecoder{}
}

func (s *avroArraySchema) EncodeSchemaNative() interface{} {
	return s.AvroNativeMeta()
}

func (s *avroArraySchema) EncodeSchema() ([]byte, error) {
	return avroSchemaCodec.BinaryFromNative(nil, s.AvroNativeMeta())
}

func (s *avroArraySchema) Encoder() schema.DataEncoder {
	return s
}

func (s *avroArraySchema) Decoder() schema.DataDecoder {
	return s
}

func (s *avroArraySchema) Valid(v interface{}) bool {
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.Slice {
		if vv.Len() > 0 {
			return s.items.Valid(vv.Index(0).Interface())
		}

		// ugh.. I guess?
		return true

	}
	return false
}

func (s *avroArraySchema) Encode(v interface{}) ([]byte, error) {
	return s.scm.BinaryFromNative(nil, v)
}

func (s *avroArraySchema) Decode(buf []byte) (interface{}, error) {
	out, _, err := s.scm.NativeFromBinary(buf)
	return out, err
}

func (s *avroArraySchema) AvroNative() map[string]interface{} {
	return s.jScm
}

func (s *avroArraySchema) AvroNativeMeta() map[string]interface{} {
	return map[string]interface{}{
		"Complex": map[string]interface{}{
			"type": map[string]interface{}{
				"ARRAY": map[string]interface{}{
					"items": s.items.(avroSchema).AvroNativeMeta(),
				},
			},
		},
	}
}
