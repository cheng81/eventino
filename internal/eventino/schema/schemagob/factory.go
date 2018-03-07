package schemagob

import (
	"github.com/cheng81/eventino/internal/eventino/schema"
)

func Factory() schema.SchemaFactory {
	return gobSchemaFactory{}
}

type gobSchemaFactory struct{}

func (_ gobSchemaFactory) For(t schema.DataType) schema.DataSchema {
	switch t {
	case schema.Bool:
		return boolSchema
	case schema.String:
		return stringSchema
	case schema.Record:
		return &gobRecordSchema{Fields: map[string]schema.DataSchema{}}
	}
	return nil
}

func (_ gobSchemaFactory) Decoder() schema.SchemaDecoder {
	return gobSchemaDecoder{}
}
