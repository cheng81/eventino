package schemaavro

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/linkedin/goavro"
)

func TestAvro(t *testing.T) {
	src := `{
		"type": "record",
		"name": "event",
		"fields": [{
			"name": "data",
			"type": [{
				"type": "record",
				"name": "foo",
				"fields": [{
					"name": "data", "type": {"type": "string"}
				}]
		}
		]
	}]}`
	codec, _ := goavro.NewCodec(src)
	t.Log(codec.Schema())

	native := map[string]interface{}{
		"data": map[string]interface{}{
			"foo": map[string]interface{}{"data": "helo"},
		},
	}
	bin, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatal("should encode", err)
	}
	native2, _, err := codec.NativeFromBinary(bin)
	if err != nil {
		t.Fatal("should decode", err)
	}
	if native["data"].(map[string]interface{})["foo"].(map[string]interface{})["data"] !=
		native2.(map[string]interface{})["data"].(map[string]interface{})["foo"].(map[string]interface{})["data"] {
		t.Fatal("data should match")
	}
}

func TestAllSchema(t *testing.T) {
	src := `
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
	codec, err := goavro.NewCodec(src)
	if err != nil {
		t.Fatal("cannot build codec", err)
	}
	t.Log(codec.Schema())

	boolSchema := map[string]interface{}{
		"Simple": "BOOLEAN",
	}
	var bin []byte
	var decoded interface{}
	if bin, err = codec.BinaryFromNative(nil, boolSchema); err != nil {
		t.Fatal("cannot encode boolSchema", err)
	}
	if decoded, _, err = codec.NativeFromBinary(bin); err != nil {
		t.Fatal("cannot decode boolSchema", err)
	}

	arrayBoolSchema := map[string]interface{}{
		"Complex": map[string]interface{}{
			"type":  "ARRAY",
			"specs": map[string]interface{}{"union": map[string]interface{}{"Simple": "BOOLEAN"}},
		},
	}
	if bin, err = codec.BinaryFromNative(nil, arrayBoolSchema); err != nil {
		t.Fatal("cannot encode arrayBoolSchema", err)
	}
	if decoded, _, err = codec.NativeFromBinary(bin); err != nil {
		t.Fatal("cannot decode arrayBoolSchema", err)
	}
	decodedMap := decoded.(map[string]interface{})
	if _, ok := decodedMap["Complex"]; !ok {
		t.Fatal("decoded type should be Comples", decodedMap)
	}
	complex := decodedMap["Complex"].(map[string]interface{})
	if complex["type"].(string) != "ARRAY" {
		t.Fatal("complex not ARRAY", decodedMap)
	}
	arrItem := complex["specs"].(map[string]interface{})
	if _, ok := arrItem["union"]; !ok {
		t.Fatal("arrItem not union[simplex,complex,enum]", arrItem)
	}
	simple := arrItem["union"].(map[string]interface{})
	if _, ok := simple["Simple"]; !ok {
		t.Fatal("simple not Simple", simple)
	}
	boolT := simple["Simple"].(string)
	t.Log("it's a bool!", boolT)

	textualEncoded, err := codec.TextualFromNative(nil, arrayBoolSchema)
	if err != nil {
		t.Fatal("cannot encode textual", err)
	}
	t.Log("textualEncoded", string(textualEncoded))
}

func TestBasicSchemas(t *testing.T) {
	t.Run("bool", basicSchemaTester(boolSchema, true))
	t.Run("string", basicSchemaTester(stringSchema, "foobar"))
	t.Run("long", basicSchemaTester(longSchema, int64(42)))
	t.Run("double", basicSchemaTester(doublueSchema, float64(101.666)))
	t.Run("null", basicSchemaTester(nilSchema, nil))
	t.Run("bytes", basicSchemaTester(bytesSchema, []byte("foobar")))
}

func basicSchemaTester(scm *basicSchema, v interface{}) func(t *testing.T) {
	return func(t *testing.T) {
		testBasicSchema(t, scm, v)
	}
}

func testBasicSchema(t *testing.T, scm *basicSchema, v interface{}) {
	tName := scm.AvroNative()["type"].(string)
	if !scm.Valid(v) {
		t.Fatal(fmt.Sprintf("%s schema - a %s should be a valid value", tName, reflect.TypeOf(v).String()))
	}
	b, err := scm.EncodeSchema()
	if err != nil {
		t.Fatal("should not fail on encodeSchema", err)
	}

	dec := scm.SchemaDecoder()
	loadedScm, err := dec.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decodeSchema", err)
	}

	if scm != loadedScm {
		t.Fatal("decoded schema should be stringschema")
	}

	encoder := scm.Encoder()
	decoder := scm.Decoder()

	var v0 interface{}

	b, err = encoder.Encode(v)
	if err != nil {
		t.Fatal("should not fail on encode", err)
	}
	v0, err = decoder.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode", err)
	}

	if _, ok := v.([]byte); ok {
		if string(v.([]byte)) != string(v0.([]byte)) {
			t.Fatal("v and v0 should match", v, v0)
		}
	} else {
		if v != v0 {
			t.Fatal("v and v0 should match", v, v0)
		}
	}
}
