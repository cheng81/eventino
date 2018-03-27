package schemaavro

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/linkedin/goavro"
)

type avroRecordSchemaBuilder struct {
	Name   string
	Fields map[string]schema.DataSchema
}

func (r *avroRecordSchemaBuilder) SetName(name string) schema.RecordSchemaBuilder {
	r.Name = name
	return r
}
func (r *avroRecordSchemaBuilder) SetField(name string, typ schema.DataSchema) schema.RecordSchemaBuilder {
	r.Fields[name] = typ
	return r
}

// ensure fields are ordered,
// since order matters in avro schema
// of record fields
type byFieldName []map[string]interface{}

func (a byFieldName) Len() int      { return len(a) }
func (a byFieldName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byFieldName) Less(i, j int) bool {
	var x string
	var y string
	x = a[i]["name"].(string)
	y = a[j]["name"].(string)
	fmt.Println("byFieldName", x, y, strings.Compare(x, y) > 0, strings.Compare(x, y))
	return strings.Compare(x, y) > 0
}

func (r *avroRecordSchemaBuilder) ToDataSchema() schema.DataSchema {
	jFields := make([]map[string]interface{}, 0, len(r.Fields))
	jScm := map[string]interface{}{
		"type": "record",
		"name": r.Name,
	}
	for name, field := range r.Fields {
		jField := map[string]interface{}{
			"name": name,
			"type": field.(avroSchema).AvroNative(),
		}
		jFields = append(jFields, jField)
	}
	sort.Sort(byFieldName(jFields))
	jScm["fields"] = jFields
	b, err := json.Marshal(jScm)
	fmt.Printf("record %s avro schema -> %s\n", r.Name, string(b))
	if err != nil {
		panic(err)
	}
	scm, err := goavro.NewCodec(string(b))
	if err != nil {
		panic(err)
	}
	return &avroRecordSchema{
		jScm:   jScm,
		name:   r.Name,
		fields: r.Fields,
		scm:    scm,
	}
}

type avroRecordSchema struct {
	jScm   map[string]interface{}
	name   string
	fields map[string]schema.DataSchema
	scm    *goavro.Codec
}

func (r *avroRecordSchema) SchemaDecoder() schema.SchemaDecoder {
	return avroSchemaDecoder{}
}

func (r *avroRecordSchema) EncodeSchema() ([]byte, error) {
	return avroSchemaCodec.BinaryFromNative(nil, r.AvroNativeMeta())
}
func (r *avroRecordSchema) EncodeSchemaNative() interface{} {
	return r.AvroNativeMeta()
}

func (r *avroRecordSchema) Encoder() schema.DataEncoder {
	return r
}

func (r *avroRecordSchema) Decoder() schema.DataDecoder {
	return r
}

// DataEncoder
func (r *avroRecordSchema) Encode(v interface{}) ([]byte, error) {
	// out, err := r.scm.TextualFromNative(nil, v)
	out, err := r.scm.BinaryFromNative(nil, v)
	fmt.Println("encoded record", v, out, err, r.AvroNative(), r.AvroNativeMeta())
	return out, err
	// return r.scm.BinaryFromNative(nil, v)
}

// DataDecoder
func (r *avroRecordSchema) Decode(buf []byte) (interface{}, error) {
	out, _, err := r.scm.NativeFromBinary(buf)
	// out, _, err := r.scm.NativeFromTextual(buf)
	fmt.Println("decoded record", buf, out, err, r.AvroNative(), r.AvroNativeMeta())
	return out, err
}

func (r *avroRecordSchema) Valid(obj interface{}) bool {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return false
	}
	for k, v := range m {
		f, ok := r.fields[k]
		if !ok || !f.Valid(v) {
			return false
		}
	}
	return true
}

func (r *avroRecordSchema) AvroNativeMeta() map[string]interface{} {
	fields := map[string]interface{}{}
	for name, field := range r.fields {
		fields[name] = field.(avroSchema).AvroNativeMeta()
	}
	return map[string]interface{}{
		"Complex": map[string]interface{}{
			"type": map[string]interface{}{
				"RECORD": map[string]interface{}{
					"name":   r.name,
					"fields": fields,
				},
			},
		},
	}
}

func (r *avroRecordSchema) AvroNative() map[string]interface{} {
	return r.jScm
}
