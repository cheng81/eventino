package schemaavro

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

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

var MetaSchema map[string]interface{}

func init() {
	var err error
	avroSchemaCodec, err = goavro.NewCodec(avroSchemaSchema)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal([]byte(avroSchemaSchema), &MetaSchema)
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

type byName []struct {
	name  string
	specs schema.DataSchema
}

func (a byName) Len() int      { return len(a) }
func (a byName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool {
	var x string
	var y string
	x = a[i].name
	y = a[j].name
	return strings.Compare(x, y) > 0
}

type byNameMap []map[string]interface{}

func (a byNameMap) Len() int      { return len(a) }
func (a byNameMap) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byNameMap) Less(i, j int) bool {
	x := a[i]["name"].(string)
	y := a[j]["name"].(string)
	return strings.Compare(x, y) > 0
}

func (_ avroSchemaFactory) EncodeNetwork(s *schema.Schema) []byte {
	ents := []map[string]interface{}{}
	for name, typ := range s.Entities {
		if len(typ.Events) == 0 {
			continue
		}
		ordered := []struct {
			name  string
			specs schema.DataSchema
		}{}
		for evtID, specs := range typ.Events {
			evt := struct {
				name  string
				specs schema.DataSchema
			}{
				name:  evtID.ToString(),
				specs: specs,
			}
			ordered = append(ordered, evt)
		}
		sort.Sort(byName(ordered))
		evts := make([]map[string]interface{}, len(typ.Events))
		for i, evt := range ordered {
			evtAvro := map[string]interface{}{
				"type": "record",
				"name": evt.name,
				"fields": []map[string]interface{}{
					map[string]interface{}{
						"name": "data",
						"type": evt.specs.(avroSchema).AvroNative(),
					},
				},
			}
			evts[i] = evtAvro
		}

		ent := map[string]interface{}{
			"name": name,
			"type": "record",
			"fields": []map[string]interface{}{
				map[string]interface{}{
					"type": "bytes",
					"name": "id",
				},
				map[string]interface{}{
					"name": "event",
					"type": evts,
				},
			},
		}
		ents = append(ents, ent)
	}
	sort.Sort(byNameMap(ents))
	out, err := json.Marshal(ents)
	if err != nil {
		panic(err)
	}
	fmt.Println("[AVRO JSON SCHEMA ENCODED]", string(out))
	return out
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

func (_ avroSchemaDecoder) DecodeNative(descrMap interface{}) (dec schema.DataSchema, err error) {
	dec, err = decodeNative(descrMap.(map[string]interface{}))
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
