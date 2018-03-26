package schema

import (
	"fmt"
	"strings"

	"github.com/cheng81/eventino/internal/eventino/item"
)

type Schema struct {
	VSN      uint64
	Records  map[EventSchemaID]DataSchema
	Enums    map[EventSchemaID]DataSchema
	Entities map[string]EntityType
}

// TODO: consider renaming, since it is used
// as index on basically any schema item, e.g.
// SchemaItemID
type EventSchemaID struct {
	Name string
	VSN  uint64
}

func (e EventSchemaID) ToString() string {
	return fmt.Sprintf("%s_%d", e.Name, e.VSN)
}

func EventSchemaIDFromString(encoded string) EventSchemaID {
	toks := strings.Split(encoded, "_")
	var vsn uint64
	fmt.Sscanf(toks[len(toks)-1], "%d", &vsn)
	name := strings.Join(toks[0:len(toks)-1], "_")
	return NewEventSchemaID(name, vsn)
}

func NewEventSchemaID(name string, vsn uint64) EventSchemaID {
	return EventSchemaID{name, vsn}
}

type EntityType struct {
	Name   string
	VSN    uint64
	Events map[EventSchemaID]DataSchema
}

func (typ EntityType) EntityID(ID []byte) item.ItemID {
	typName := []byte(typ.Name)
	l := len(typName)
	b := make([]byte, l+len(ID)+1)
	b[l] = byte(':')
	copy(b[0:], typName)
	copy(b[l+1:], ID)
	return item.NewItemID(1, b)
}

// func (typ EntityType) AliasIndex(idx uint64) item.ItemID {
// 	typName := []byte(typ.Name)
// 	l := len(typName)
// 	b := make([]byte, l+3)
// 	b[l] = byte(':')
// 	b[l+1] = byte('I')
// 	b[l+2] = byte(':')
// 	copy(b[0:], typName)
// 	binary.BigEndian.PutUint64(b[l+3], entIntID)
// 	return item.NewItemID(2, b)
// }

// possibly not needed/bad practice
// func (et EntityType) Latest(evtName string) (DataSchema, uint64, bool) {
// 	var latestVSN uint64
// 	for evtID := range et.Events {
// 		if evtID.Name == evtName && evtID.VSN > latestVSN {
// 			latestVSN = evtID.VSN
// 		}
// 	}
// 	scm, exists := et.Events[EventSchemaID{Name: evtName, VSN: latestVSN}]
// 	return scm, latestVSN, exists
// }

type SchemaFactory interface {
	SimpleType(DataType) DataSchema
	NewRecord() RecordSchemaBuilder
	Decoder() SchemaDecoder
	EncodeNetwork(s *Schema) []byte
	// TODO suport other complex types
	// NewEnum(items ...string) DataSchema
	// NewOptional(DataSchema) DataSchema
	// NewArray(DataSchema) DataSchema
	// NewUnion(types ...DataSchema) DataSchema
}

type RecordSchemaBuilder interface {
	SetName(string) RecordSchemaBuilder
	SetField(string, DataSchema) RecordSchemaBuilder
	ToDataSchema() DataSchema
}

type DataDecoder interface {
	Decode([]byte) (interface{}, error)
}

type DataEncoder interface {
	Encode(interface{}) ([]byte, error)
}

type DataSchema interface {
	SchemaDecoder() SchemaDecoder
	EncodeSchemaNative() interface{}
	EncodeSchema() ([]byte, error)

	Encoder() DataEncoder
	Decoder() DataDecoder

	Valid(interface{}) bool
}

type SchemaDecoder interface {
	DecodeNative(interface{}) (DataSchema, error)
	Decode([]byte) (DataSchema, error)
}

type DataType byte

// TODO: I feel like we only need this
// enum for basic types.
// Complex type should be returned
// with a builder from the Factory
const (
	Null DataType = iota
	Bool
	Int64
	Float64
	String
	Bytes
	Enum
	Optional
	Array
	Union
	Record
)

func (dt DataType) IsSimple() bool {
	switch dt {
	case Null:
		fallthrough
	case Bool:
		fallthrough
	case Int64:
		fallthrough
	case Float64:
		fallthrough
	case String:
		fallthrough
	case Bytes:
		return true
	}
	return false
}

func (dt DataType) IsEnum() bool {
	return dt == Enum
}

func (dt DataType) IsComplex() bool {
	switch dt {
	case Optional:
		fallthrough
	case Array:
		fallthrough
	case Union:
		fallthrough
	case Record:
		return true
	}
	return false
}

// TODO: make complex types namespace
// so that a record is defined there,
// and an entity event data schema
// refers to a, e.g., record using its name?

// TODO: views requires their schema too,
// though this is complex since it'll need
// to support an entire language!
// a view materializes an entity to a record type,
// or actually to any type? anyway, then the
// view specs take an entity, a type,
// and a function that takes an event and an instance
// of the view record and updates the record with the
// info of the event
// e.g.
// type UserViewRec Record with
//		ID string
//		username string
//		email string
//		isPaying bool
//		lastUpdate timestamp
// view UserView on User returns UserViewRec as
//	-- specify fold function!
//	(e:Created, rec) {
//		rec.ID = e.UserID,
//		rec.isPaying = e.Paying
//		rec.lastUpdate = timestamp(e)
//	}
//  (e:Updated, rec) {
//		rec.email = e.Email
//		rec.username = e.Username
//		rec.lastUpdate = timestamp(e)
//  }
//
// Also: a view should be tied to a specific schema version,
// so that, e.g., events added/updated afterwards should be
// not passed to the fold fun
