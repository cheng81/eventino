package schema

type EventSchemaID struct {
	Name string
	VSN  uint64
}

type EntityType struct {
	Name   string
	VSN    uint64
	Events map[EventSchemaID]DataSchema
}

func (et EntityType) Latest(evtName string) (DataSchema, uint64, bool) {
	var latestVSN uint64
	for evtID := range et.Events {
		if evtID.Name == evtName && evtID.VSN > latestVSN {
			latestVSN = evtID.VSN
		}
	}
	scm, exists := et.Events[EventSchemaID{Name: evtName, VSN: latestVSN}]
	return scm, latestVSN, exists
}

type DataDecoder interface {
	Decode([]byte) (interface{}, error)
}

type DataEncoder interface {
	Encode(interface{}) ([]byte, error)
}

type DataSchema interface {
	SchemaDecoder() SchemaDecoder
	EncodeSchema() ([]byte, error)
	// DecodeSchema([]byte) error

	Encoder() DataEncoder
	Decoder() DataDecoder

	Valid(interface{}) bool
}

type SchemaDecoder interface {
	Decode([]byte) (DataSchema, error)
}

type SchemaFactory interface {
	For(DataType) DataSchema
	Decoder() SchemaDecoder
}

type DataType byte

const (
	Bool DataType = iota
	Int64
	Float64
	String
	Bytes
	Optional
	Array
	Union
	Record
)

type RecordSchemaBuilder interface {
	SetName(string) RecordSchemaBuilder
	SetField(string, DataSchema) RecordSchemaBuilder
}

const (
	EventKindSchema byte = 4
)

// type BoolSchema struct{}

// func (b BoolSchema) Valid(v interface{}) error {
// 	switch v := v.(type) {
// 	case bool:
// 		return nil
// 	default:
// 		return NewInvalidType("bool", reflect.TypeOf(v).Name())
// 	}
// }

// type IntSchema struct{}

// func (i IntSchema) Valid(v interface{}) error {
// 	switch v := v.(type) {
// 	case int64:
// 		return nil
// 	default:
// 		return NewInvalidType("int64", reflect.TypeOf(v).Name())
// 	}
// }

// type FloatSchema struct{}

// func (f FloatSchema) Valid(v interface{}) error {
// 	switch v := v.(type) {
// 	case float64:
// 		return nil
// 	default:
// 		return NewInvalidType("float64", reflect.TypeOf(v).Name())
// 	}
// }

// type VecSchema struct {
// 	ItemSchema EventSchema
// }

// func (s VecSchema) Valid(v interface{}) error {
// 	t := reflect.TypeOf(v)
// 	switch v := v.(type) {
// 	case []interface{}:
// 		return s.ItemSchema.Valid(v[0])
// 	default:
// 		return NewInvalidType("array", reflect.TypeOf(v).Name())
// 	}
// }
