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
			"type": [
				{"type": "record",
					"name": "UNION",
					"fields": [{"name": "types", "type": {"type": "array", "items": ["Simple", "Complex", "Enum", "Ref"]}}]},
				{"type": "record",
					"name": "ARRAY",
					"fields": [{"name": "items", "type": ["Simple", "Complex", "Enum", "Ref"]}]},
				{"type": "record",
					"name": "RECORD",
					"fields": [{"name": "name", "type": "string"},
										 {"name": "fields", "type": {"type": "map", "values": ["Simple", "Complex", "Enum", "Ref"]}}]}
			]
		}]}
	]
}
`

// additional interface for schema.DataSchema which are avro schemas
type avroSchema interface {
	AvroNativeMeta() map[string]interface{}
	AvroNative() map[string]interface{}
}

// MetaSchema is the native avro schema to encode avro-like schemas
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

// Factory returns a schema.SchemaFactory
func Factory() schema.SchemaFactory {
	return avroSchemaFactory{}
}

type avroSchemaFactory struct{}

func (avroSchemaFactory) SimpleType(t schema.DataType) schema.DataSchema {
	switch t {
	case schema.Null:
		return nilSchema
	case schema.Bool:
		return boolSchema
	case schema.String:
		return stringSchema
	case schema.Int64:
		return longSchema
	case schema.Float64:
		return doublueSchema
	case schema.Bytes:
		return bytesSchema
	}
	return nil

}

func (avroSchemaFactory) NewRecord() schema.RecordSchemaBuilder {
	return &avroRecordSchemaBuilder{Fields: map[string]schema.DataSchema{}}
}

func (avroSchemaFactory) NewArray(items schema.DataSchema) schema.DataSchema {
	return newArray(items)
}

func (avroSchemaFactory) Decoder() schema.SchemaDecoder {
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

func (avroSchemaFactory) EncodeNetwork(s *schema.Schema) []byte {
	entsEvt := []map[string]interface{}{}
	entsLoad := []map[string]interface{}{}
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
		evtsTs := make([]map[string]interface{}, len(typ.Events))
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

			evtAvroTs := map[string]interface{}{
				"type": "record",
				"name": evt.name,
				"fields": []map[string]interface{}{
					map[string]interface{}{
						"name": "ts",
						"type": "long",
					},
					map[string]interface{}{
						"name": "data",
						"type": evt.specs.(avroSchema).AvroNative(),
					},
				},
			}
			evtsTs[i] = evtAvroTs
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
		entsEvt = append(entsEvt, ent)

		entLoad := map[string]interface{}{
			"name": name, "type": "record",
			"fields": []map[string]interface{}{
				map[string]interface{}{"name": "id", "type": "bytes"},
				map[string]interface{}{"name": "schema_vsn", "type": "long"},
				map[string]interface{}{"name": "vsn", "type": "long"},
				map[string]interface{}{"name": "latest_vsn", "type": "long"},
				map[string]interface{}{"name": "events", "type": map[string]interface{}{"type": "array", "items": evtsTs}},
			},
		}
		entsLoad = append(entsLoad, entLoad)
	}
	sort.Sort(byNameMap(entsEvt))

	null := map[string]interface{}{"type": "null"}
	entsEvt = append(entsEvt, null)
	entsLoad = append(entsLoad, null)
	wrapper := map[string]interface{}{
		"name": "data", "type": "record",
		"fields": []map[string]interface{}{
			map[string]interface{}{"name": "entity_event", "type": entsEvt},
			map[string]interface{}{"name": "entity_load", "type": entsLoad},
		},
	}
	out, err := json.Marshal(wrapper)
	if err != nil {
		panic(err)
	}
	fmt.Println("[AVRO JSON SCHEMA ENCODED]", string(out))
	return out
}

type avroSchemaDecoder struct{}

func (avroSchemaDecoder) Decode(b []byte) (dec schema.DataSchema, err error) {
	var descr interface{}
	if descr, _, err = avroSchemaCodec.NativeFromBinary(b); err != nil {
		return nil, err
	}
	var descrMap = descr.(map[string]interface{})
	dec, err = decodeNative(descrMap)
	return
}

func (avroSchemaDecoder) DecodeNative(descrMap interface{}) (dec schema.DataSchema, err error) {
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
		case "LONG":
			dec = longSchema
		case "DOUBLE":
			dec = doublueSchema
		case "BYTES":
			dec = bytesSchema
		default:
			err = errors.New("NOT IMPLEMENTED")
		}
	}
	if c, ok := descrMap["Complex"]; ok {
		cMap := c.(map[string]interface{})["type"].(map[string]interface{})
		var t string
		var val interface{}
		for k, v := range cMap {
			t = k
			val = v
			break
		}
		fmt.Println("decode COMPLEX", t, val)
		switch t {
		case "RECORD":
			dec = decodeRecord(val.(map[string]interface{}))
		case "ARRAY":
			itemsJScm := val.(map[string]interface{})["items"].(map[string]interface{})
			itemsDec, err := decodeNative(itemsJScm)
			if err == nil {
				dec = newArray(itemsDec)
			}
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
	mFields := d["fields"].(map[string]interface{})
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
