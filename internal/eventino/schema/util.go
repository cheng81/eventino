package schema

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/cheng81/eventino/internal/eventino/item"
)

// const eventIndexKind byte = 8
// const eventIndexType string = "NEWENT"

var schemaID item.ItemID

var EntityTypeNotFound error
var EntityExists error

func init() {
	schemaID = item.NewItemID(0, []byte("SCHEMA"))
	EntityTypeNotFound = errors.New("entity type not found")
	EntityExists = errors.New("entity already exists")
}

// func EntityIndexID(entityType string) item.ItemID {
// 	return item.NewItemID(0, []byte(fmt.Sprintf("schema:index:%s", entityType)))
// }

// func NewEntityItemIndex(entityID []byte) item.Event {
// 	return item.NewEvent(eventIndexKind, []byte(eventIndexType), entityID)
// }

// func EventID(entityType, eventType string) item.ItemID {
// 	return item.NewItemID(0, []byte(fmt.Sprintf("schema:%s:%s", entityType, eventType)))
// }

type schemaWire struct {
	Name   string
	Events map[EventSchemaID][]byte
}

func schemaToSchemaWire(entType EntityType) (schemaWire, error) {
	out := schemaWire{
		Name:   entType.Name,
		Events: make(map[EventSchemaID][]byte, len(entType.Events)),
	}
	for id, scm := range entType.Events {
		b, err := scm.EncodeSchema()
		if err != nil {
			return out, err
		}
		out.Events[id] = b
	}
	return out, nil
}

func schemaWireToSchema(vsn uint64, w schemaWire, sdec SchemaDecoder) (EntityType, error) {
	out := EntityType{
		Name:   w.Name,
		VSN:    vsn,
		Events: make(map[EventSchemaID]DataSchema, len(w.Events)),
	}
	for id, b := range w.Events {
		scm, err := sdec.Decode(b)
		if err != nil {
			return out, err
		}
		out.Events[id] = scm
	}
	return out, nil
}

// import (
// 	"fmt"
// )

// func NewInvalidType(expected, actual string) error {
// 	return InvalidType{expected: expected, actual: actual}
// }

// type InvalidType struct {
// 	expected string
// 	actual   string
// }

// func (it InvalidType) Error() string {
// 	return fmt.Sprintf("Invalid type. Expected %s, got %s", it.expected, it.actual)
// }

func encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(b []byte, v interface{}) error {
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	return dec.Decode(v)
}
