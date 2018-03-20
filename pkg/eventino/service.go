package eventino

import (
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/schema"

	"github.com/dgraph-io/badger"
)

type Eventino interface {
	CreateEntityType(name string) (uint64, error)
	// DeleteEntityType(name string) (uint64, error)
	// GetEntityType(name string, vsn uint64) (schema.EntityType, error)

	// CreateEventType(entName, name string, specs schema.DataSchema) (uint64, error)
	// UpdateEventType(entName, name string, specs schema.DataSchema) (uint64, uint64, error)
	// GetEventType(entName, name string, vsn uint64) (schema.DataSchema, error)
	// DeleteEventType(entName, name string) (uint64, error)

	// NewEntity(typ schema.EntityType, entID []byte) error
	// Put(typ schema.EntityType, entID []byte, evtID schema.EventSchemaID, evt interface{}) (uint64, error)
}

func NewEventino(db *badger.DB, factory schema.SchemaFactory) Eventino {
	return &eventino{db: db, factory: factory}
}

type eventino struct {
	db      *badger.DB
	factory schema.SchemaFactory
}

func (e *eventino) CreateEntityType(name string) (vsn uint64, err error) {
	dec := e.factory.Decoder()
	err = e.db.Update(func(txn *badger.Txn) (err error) {
		if err = schema.CreateEntityType(txn, dec, name); err != nil {
			return
		}
		if vsn, err = schema.SchemaVSN(txn, dec); err != nil {
			return
		}
		fmt.Println("loaded vsn", vsn)
		return
	})
	return
}
