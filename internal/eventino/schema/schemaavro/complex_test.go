package schemaavro

import (
	"reflect"
	"testing"

	"github.com/cheng81/eventino/internal/eventino/schema"
)

func TestArrayMeta(t *testing.T) {
	f := Factory()
	arrT := f.NewArray(f.SimpleType(schema.Int64))

	n := arrT.(avroSchema).AvroNativeMeta()

	arrTDec, err := f.Decoder().DecodeNative(n)
	if err != nil {
		t.Fatal("should decode avro native")
	}

	_, ok := arrTDec.(*avroArraySchema)
	if !ok {
		t.Fatal("decoded schema should be array")
	}

	b, err := arrT.EncodeSchema()
	if err != nil {
		t.Fatal("should not fail on encode")
	}

	arrTDec, err = f.Decoder().Decode(b)
	if err != nil {
		t.Fatal("should decode avro bytes")
	}

	_, ok = arrTDec.(*avroArraySchema)
	if !ok {
		t.Fatal("decoded schema should be array")
	}

}

func TestArray(t *testing.T) {
	f := Factory()
	arrT := f.NewArray(f.SimpleType(schema.Int64))

	v := []int64{1, 2, 3}
	if !arrT.Valid(v) {
		t.Fatal("int slice should be valid")
	}

	b, err := arrT.Encoder().Encode(v)
	if err != nil {
		t.Fatal("should not fail on encode")
	}

	v0, err := arrT.Decoder().Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode")
	}

	v0Ar, ok := v0.([]interface{}) // uhrm
	if !ok {
		t.Fatal("decodec value should be slice of correct type", v0, reflect.TypeOf(v0), reflect.ValueOf(v0).Kind())
	}
	if len(v0Ar) != len(v) {
		t.Fatal("decoded len should be same as original")
	}
	for i, val := range v {
		if v0Ar[i] != val {
			t.Fatal("all items should be the same", v0Ar, v)
		}
	}
}

func TestArrayComplex(t *testing.T) {
	f := Factory()
	rec := f.NewRecord().SetName("foo").SetField("a", f.SimpleType(schema.Bool)).SetField("b", f.SimpleType(schema.String)).ToDataSchema()

	eq := func(a interface{}, b interface{}) bool {
		am := a.(map[string]interface{})
		bm := b.(map[string]interface{})
		return am["a"] == bm["a"] && am["b"] == bm["b"]
	}

	arrT := f.NewArray(rec)

	v := []interface{}{
		map[string]interface{}{
			"a": true,
			"b": "foobar",
		},
		map[string]interface{}{
			"a": false,
			"b": "qoox baz",
		},
	}

	if !arrT.Valid(v) {
		t.Fatal("int slice should be valid")
	}

	b, err := arrT.Encoder().Encode(v)
	if err != nil {
		t.Fatal("should not fail on encode")
	}

	v0, err := arrT.Decoder().Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode")
	}

	v0Ar, ok := v0.([]interface{}) // uhrm
	if !ok {
		t.Fatal("decodec value should be slice of correct type", v0, reflect.TypeOf(v0), reflect.ValueOf(v0).Kind())
	}
	if len(v0Ar) != len(v) {
		t.Fatal("decoded len should be same as original")
	}
	for i, val := range v {
		if !eq(v0Ar[i], val) {
			t.Fatal("all items should be the same", v0Ar, v)
		}
	}
}
