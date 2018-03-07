package schemagob

import (
	"errors"

	"github.com/cheng81/eventino/internal/eventino/schema"
)

type gobSchemaDecoder struct{}

func (d gobSchemaDecoder) Decode(b []byte) (dec schema.DataSchema, err error) {
	var t schema.DataType
	if len(b) == 0 {
		return dec, errors.New("cannot decode schema - empty []byte to decode")
	}
	t = schema.DataType(b[0])
	switch t {
	case schema.Bool:
		dec = boolSchema
	case schema.String:
		dec = stringSchema
	case schema.Record:
		dec, err = decodeRecordSchema(b[1:])
		// dec = &gobRecordSchema{}
		// err = decode(b[1:], dec)
	default:
		err = errors.New("not implemented")
	}

	return
}
