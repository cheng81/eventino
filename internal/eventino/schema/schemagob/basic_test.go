package schemagob

import "testing"

func TestBoolSchema(t *testing.T) {
	if !boolSchema.Valid(true) {
		t.Fatal("a bool should be a valid value")
	}

	b, err := boolSchema.EncodeSchema()
	if err != nil {
		t.Fatal("should not fail on encodeSchema", err)
	}

	dec := gobSchemaDecoder{}
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

	dec := gobSchemaDecoder{}
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
