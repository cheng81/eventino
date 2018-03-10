package schemagob

import (
	"github.com/cheng81/eventino/internal/eventino/schema"
)

func Factory() schema.SchemaFactory {
	return gobSchemaFactory{}
}

type gobSchemaFactory struct{}

// TODO: should probably return DataSchema, error and errors is data type is not simple
func (_ gobSchemaFactory) SimpleType(t schema.DataType) schema.DataSchema {
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

func (_ gobSchemaFactory) NewRecord() schema.RecordSchemaBuilder {
	return &gobRecordSchema{Fields: map[string]schema.DataSchema{}}
}

func (_ gobSchemaFactory) Decoder() schema.SchemaDecoder {
	return gobSchemaDecoder{}
}
