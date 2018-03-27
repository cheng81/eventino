package schemaavro

import (
	"testing"

	"github.com/cheng81/eventino/internal/eventino/schema"
)

func TestRecordBuilder(t *testing.T) {
	f := Factory()
	bldr := f.NewRecord()
	bldr.SetName("foo")
	bldr.SetField("aLong", f.SimpleType(schema.Int64))
	bldr.SetField("aDouble", f.SimpleType(schema.Float64))
	ds := bldr.ToDataSchema()

	var ok bool
	var rec *avroRecordSchema
	if rec, ok = ds.(*avroRecordSchema); !ok {
		t.Fatal("data schema should be avroRecordSchema")
	}

	if rec.name != "foo" {
		t.Fatal("record name should be foo")
	}

	var fds schema.DataSchema
	if fds, ok = rec.fields["aLong"]; !ok {
		t.Fatal("should have aLong field")
	}
	if fds.(*basicSchema).jScm["type"].(string) != "long" {
		t.Fatal("aLong field should be of type long")
	}

	if fds, ok = rec.fields["aDouble"]; !ok {
		t.Fatal("should have aLong field")
	}
	if fds.(*basicSchema).jScm["type"].(string) != "double" {
		t.Fatal("aDouble field should be of type double")
	}

}

func TestRecordEncodeDecode(t *testing.T) {
	f := Factory()
	bldr := f.NewRecord()
	bldr.SetName("foo").
		SetField("aLong", f.SimpleType(schema.Int64)).
		SetField("aDouble", f.SimpleType(schema.Float64))
	ds := bldr.ToDataSchema()

	v := map[string]interface{}{
		"aLong":   int64(42),
		"aDouble": float64(101.66),
	}

	if !ds.Valid(v) {
		t.Fatal("test record should be valid")
	}

	b, err := ds.Encoder().Encode(v)
	if err != nil {
		t.Fatal("should not fail on encode", err)
	}

	v0, err := ds.Decoder().Decode(b)
	if err != nil {
		t.Fatal("should not fail on decode", err)
	}

	v0Map, ok := v0.(map[string]interface{})
	if !ok {
		t.Fatal("v0 should be map", v0)
	}

	for k, fv := range v {
		var fv0 interface{}
		var ok bool
		fv0, ok = v0Map[k]
		if !ok {
			t.Fatal("should have field", k, v0Map)
		}
		if fv != fv0 {
			t.Fatal("field values should be the same", fv, fv0)
		}
	}
}
