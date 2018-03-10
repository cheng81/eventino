package schemaavro

import (
	"errors"
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/schema"

	"github.com/linkedin/goavro"
)

var avroSchemaCodec *goavro.Codec

// TODO: encode also our truly "OPTIONAL",
// which underlying is mapped to a union:
// optional(T) -> union(nil, T)
const avroSchemaSchema = `
{
	"type": [
		{"type": "record",
		 "name": "Enum",
		 "fields": [{
			 "name": "name", "type": "string"
		 },{
			 "name": "values", "type": {"type": "array", "items": "string"}
		 }]},
		{"type": "record",
		 "name": "Ref",
		 "fields": [{"name": "typename", "type": "string"}]},
		{"type": "enum",
		 "name": "Simple",
		 "symbols": ["INT", "LONG", "STRING", "BOOLEAN", "FLOAT", "DOUBLE", "NULL", "BYTES"]},
		{"type": "record",
		 "name": "Complex",
		 "fields": [{
			 "name": "type",
			 "type": {
				 "type": "enum",
				 "name": "Type",
				 "symbols": ["ARRAY", "UNION", "RECORD"]
			 	}
			 },
			 {
				 "name": "name",
				 "type": "string"
			 },
			 {
				"name": "specs",
				"type": [
					["Simple", "Complex", "Enum", "Ref"],
					{"type": "array", "items": ["Simple", "Complex", "Enum", "Ref"]},
					{"type": "map", "values": ["Simple", "Complex", "Enum", "Ref"]}
				]
				}
			]
		}
	]
}
`

func init() {
	var err error
	avroSchemaCodec, err = goavro.NewCodec(avroSchemaSchema)
	if err != nil {
		panic(err)
	}
}

func Factory() schema.SchemaFactory {
	return avroSchemaFactory{}
}

type avroSchemaFactory struct{}

func (_ avroSchemaFactory) SimpleType(t schema.DataType) schema.DataSchema {
	switch t {
	case schema.Null:
		return nilSchema
	case schema.Bool:
		return boolSchema
	case schema.String:
		return stringSchema
	}
	return nil

}

func (_ avroSchemaFactory) NewRecord() schema.RecordSchemaBuilder {
	return &avroRecordSchemaBuilder{Fields: map[string]schema.DataSchema{}}
}

func (_ avroSchemaFactory) Decoder() schema.SchemaDecoder {
	return avroSchemaDecoder{}
}

type avroSchemaDecoder struct{}

func (_ avroSchemaDecoder) Decode(b []byte) (dec schema.DataSchema, err error) {
	var descr interface{}
	if descr, _, err = avroSchemaCodec.NativeFromBinary(b); err != nil {
		return nil, err
	}
	var descrMap = descr.(map[string]interface{})
	dec, err = decodeNative(descrMap)
	return
}

func decodeNative(descrMap map[string]interface{}) (dec schema.DataSchema, err error) {
	if _, ok := descrMap["Simple"]; ok {
		// simple t
		switch descrMap["Simple"].(string) {
		case "NULL":
			dec = nilSchema
		case "BOOLEAN":
			dec = boolSchema
		case "STRING":
			dec = stringSchema
		default:
			err = errors.New("NOT IMPLEMENTED")
		}
	}
	if c, ok := descrMap["Complex"]; ok {
		cMap := c.(map[string]interface{})
		switch cMap["type"].(string) {
		case "RECORD":
			dec = decodeRecord(cMap)
		default:
			err = errors.New("NOT IMPLEMENTED")
		}
	}
	if dec == nil {
		panic(fmt.Sprintf("cannot decode %+v", descrMap))
	}
	return
}

func decodeRecord(d map[string]interface{}) schema.DataSchema {
	mFields := d["specs"].(map[string]interface{})["map"].(map[string]interface{})
	fields := map[string]schema.DataSchema{}
	fmt.Printf("decodeRecord-specs %+v\n", mFields)
	for name, spec := range mFields {
		fmt.Printf("decodeRecord-field %s %+v\n", name, spec)
		dec, err := decodeNative(spec.(map[string]interface{}))
		if err != nil {
			panic(err)
		}
		fields[name] = dec
	}

	return (&avroRecordSchemaBuilder{Name: d["name"].(string), Fields: fields}).ToDataSchema()
}

type avroSchema interface {
	AvroNativeMeta() map[string]interface{}
	AvroNative() map[string]interface{}
}
