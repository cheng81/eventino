package schema

import (
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/item"
	"github.com/dgraph-io/badger"
)

const (
	entCreated string = "ENT:CREATED"
	entDeleted string = "ENT:DELETED"
	evtCreated string = "EVT:CREATED"
	evtUpdated string = "EVT:UPDATED"
	evtDeleted string = "EVT:DELETED"
)

func newEntityCreated(name string) (out item.Event, err error) {
	var b []byte
	if b, err = encode(entityTypeCreated{Name: name}); err != nil {
		return
	}
	out = item.NewEvent(EventKindSchema, []byte(entCreated), b)
	return
}

func newEntityDeleted(name string) (out item.Event, err error) {
	var b []byte
	if b, err = encode(entityTypeDeleted{Name: name}); err != nil {
		return
	}
	out = item.NewEvent(EventKindSchema, []byte(entDeleted), b)
	return
}

func newEventTypeCreated(entName, name string, schema DataSchema) (out item.Event, err error) {
	var b []byte
	var schemaBin []byte

	if schemaBin, err = schema.EncodeSchema(); err != nil {
		return
	}
	if b, err = encode(entityEventTypeCreated{Entity: entName, Name: name, SchemaBin: schemaBin}); err != nil {
		return
	}
	out = item.NewEvent(EventKindSchema, []byte(evtCreated), b)
	return
}

func newEventTypeUpdated(entName, name string, schema DataSchema) (out item.Event, err error) {
	var b []byte
	var schemaBin []byte

	if schemaBin, err = schema.EncodeSchema(); err != nil {
		return
	}
	if b, err = encode(entityEventTypeUpdated{Entity: entName, Name: name, SchemaBin: schemaBin}); err != nil {
		return
	}
	out = item.NewEvent(EventKindSchema, []byte(evtUpdated), b)
	return
}

func newEventTypeDeleted(entName, name string) (out item.Event, err error) {
	var b []byte
	if b, err = encode(entityEventTypeDeleted{Entity: entName, Name: name}); err != nil {
		return
	}
	out = item.NewEvent(EventKindSchema, []byte(evtDeleted), b)
	return
}

type entityTypeCreated struct {
	Name string
}

type entityTypeDeleted struct {
	Name string
}

type entityEventTypeCreated struct {
	Entity    string
	Name      string
	SchemaBin []byte
}

type entityEventTypeUpdated struct {
	Entity    string
	Name      string
	SchemaBin []byte
}

type entityEventTypeDeleted struct {
	Entity string
	Name   string
}

type recordTypeCreated struct {
	Name      string
	SchemaBin []byte
}

type recordTypeUpdated struct {
	Name      string
	SchemaBin []byte
}

type recordTypeDeleted struct {
	Name string
}

type enumTypeCreated struct {
	Name   string
	Values []string
}

type enumTypeUpdated struct {
	Name   string
	Values []string
}

type enumTypeDeleted struct {
	Name string
}

func getSchema(txn *badger.Txn, schemaDec SchemaDecoder, stopper func(Schema) bool) (Schema, error) {
	scm := Schema{VSN: 0, Entities: map[string]EntityType{}}
	res, itemVsn, err := item.View(txn, schemaID, 0, schemaFolder(stopper, schemaDec), scm)
	fmt.Println("getSchema vsn", res.(Schema).VSN, itemVsn)
	return res.(Schema), err
}

func schemaFolder(stopper func(Schema) bool, schemaDec SchemaDecoder) item.ViewFoldFunc {
	return func(acc interface{}, evt item.Event, _ uint64) (out interface{}, stop bool, err error) {
		// skip non-schema events (e.g. created)
		fmt.Println("schemaFolder", evt.Kind, EventKindSchema, acc.(Schema).VSN)
		if evt.Kind != EventKindSchema {
			return acc, false, nil
		}
		scm := acc.(Schema)
		scm.VSN++
		fmt.Println("schemaFolder - kind right", scm.VSN, scm)
		stop = stopper(scm)

		switch string(evt.Type) {
		case entCreated:
			e := &entityTypeCreated{}
			if err = decode(evt.Payload, e); err != nil {
				return
			}
			scm.Entities[e.Name] = EntityType{Name: e.Name, VSN: 0, Events: map[EventSchemaID]DataSchema{}}
		case entDeleted:
			e := &entityTypeDeleted{}
			if err = decode(evt.Payload, e); err != nil {
				return
			}
			delete(scm.Entities, e.Name)
		case evtCreated:
			e := &entityEventTypeCreated{}
			if err = decode(evt.Payload, e); err != nil {
				return
			}
			et := scm.Entities[e.Entity]
			et.VSN++
			var evtSchema DataSchema
			if evtSchema, err = schemaDec.Decode(e.SchemaBin); err != nil {
				return
			}
			et.Events[EventSchemaID{Name: e.Name, VSN: 0}] = evtSchema
			scm.Entities[e.Entity] = et
		case evtUpdated:
			e := &entityEventTypeUpdated{}
			if err = decode(evt.Payload, e); err != nil {
				return
			}
			et := scm.Entities[e.Entity]
			et.VSN++
			var evtSchema DataSchema
			if evtSchema, err = schemaDec.Decode(e.SchemaBin); err != nil {
				return
			}
			latestVsn := uint64(0)
			for k := range et.Events {
				if k.Name == e.Name && k.VSN > latestVsn {
					latestVsn = k.VSN
				}
			}
			et.Events[NewEventSchemaID(e.Name, latestVsn+1)] = evtSchema
			scm.Entities[e.Entity] = et
		case evtDeleted:
			e := &entityEventTypeDeleted{}
			if err = decode(evt.Payload, e); err != nil {
				return
			}
			et := scm.Entities[e.Entity]
			et.VSN++
			for k := range et.Events {
				if k.Name == e.Name {
					delete(et.Events, k)
				}
			}
			scm.Entities[e.Entity] = et
		default:
		}

		fmt.Println("schemaFolder - out ", scm)
		return scm, false, nil
	}
}
