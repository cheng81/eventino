package schemagob

import (
	"github.com/cheng81/eventino/internal/eventino/schema"
)

type gobRecordSchema struct {
	Name   string
	Fields map[string]schema.DataSchema
}

func (r *gobRecordSchema) SetName(name string) schema.RecordSchemaBuilder {
	r.Name = name
	return r
}
func (r *gobRecordSchema) SetField(name string, typ schema.DataSchema) schema.RecordSchemaBuilder {
	r.Fields[name] = typ
	return r
}
func (r *gobRecordSchema) ToDataSchema() schema.DataSchema {
	return r
}

func (_ *gobRecordSchema) SchemaDecoder() schema.SchemaDecoder {
	return gobSchemaDecoder{}
}

func (r *gobRecordSchema) EncodeSchema() ([]byte, error) {
	var b []byte
	var err error

	w := recWire{Name: r.Name, Fields: make(map[string][]byte, len(r.Fields))}
	for k, v := range r.Fields {
		enc, err := v.EncodeSchema()
		if err != nil {
			return nil, err
		}
		w.Fields[k] = enc
	}

	b, err = encode(w)
	if err != nil {
		return nil, err
	}

	var out = make([]byte, 1+len(b))
	out[0] = byte(schema.Record)
	copy(out[1:], b[:])
	return out, nil
}

func (r *gobRecordSchema) Encoder() schema.DataEncoder {
	return GenericGobEncoder(encode)
}

func (r *gobRecordSchema) Decoder() schema.DataDecoder {
	return GenericGobDecoder(func(b []byte) (interface{}, error) {
		var v map[string]interface{}
		err := decode(b, &v)
		return v, err
	})
}

func (r *gobRecordSchema) Valid(obj interface{}) bool {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return false
	}
	for k, v := range m {
		f, ok := r.Fields[k]
		if !ok || !f.Valid(v) {
			return false
		}
	}
	return true
}

type recWire struct {
	Name   string
	Fields map[string][]byte
}

func decodeRecordSchema(b []byte) (r *gobRecordSchema, err error) {
	var w recWire
	err = decode(b, &w)
	r = &gobRecordSchema{Name: w.Name, Fields: make(map[string]schema.DataSchema, len(w.Fields))}
	for k, v := range w.Fields {
		scm, err := r.SchemaDecoder().Decode(v)
		if err != nil {
			return nil, err
		}
		r.Fields[k] = scm
	}
	return
}
