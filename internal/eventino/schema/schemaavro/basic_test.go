package schemaavro

import (
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

func TestBoolSchema(t *testing.T) {
	if !boolSchema.Valid(true) {
		t.Fatal("a bool should be a valid value")
	}

	b, err := boolSchema.EncodeSchema()
	if err != nil {
		t.Fatal("should not fail on encodeSchema", err)
	}

	dec := avroSchemaDecoder{}
	scm, err := dec.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decodeSchema", err)
	}

	if scm != boolSchema {
		t.Fatal("decoded schema should be boolschema")
	}

	encoder := boolSchema.Encoder()
	decoder := boolSchema.Decoder()

	var aBool bool
	var bIntf interface{}

	aBool = true
	b, err = encoder.Encode(aBool)
	if err != nil {
		t.Fatal("should not fail on encode", err)
	}
	bIntf, err = decoder.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode", err)
	}
	if aBool != bIntf.(bool) {
		t.Fatal("a and b bools should match", aBool, bIntf)
	}
}

func TestStringSchema(t *testing.T) {
	if !stringSchema.Valid("foobar") {
		t.Fatal("a string should be a valid value")
	}
	b, err := stringSchema.EncodeSchema()
	if err != nil {
		t.Fatal("should not fail on encodeSchema", err)
	}

	dec := avroSchemaDecoder{}
	scm, err := dec.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decodeSchema", err)
	}

	if scm != stringSchema {
		t.Fatal("decoded schema should be stringschema")
	}

	encoder := stringSchema.Encoder()
	decoder := stringSchema.Decoder()

	var aStr string
	var bIntf interface{}

	aStr = "foobar"
	b, err = encoder.Encode(aStr)
	if err != nil {
		t.Fatal("should not fail on encode", err)
	}
	bIntf, err = decoder.Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode", err)
	}
	if aStr != bIntf.(string) {
		t.Fatal("a and b strs should match", aStr, bIntf)
	}
}
