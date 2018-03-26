package eventino

import (
	"errors"
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/entity"

	"github.com/cheng81/eventino/internal/eventino/schema"

	"github.com/dgraph-io/badger"
)

type Eventino interface {
	LoadSchema(vsn uint64) (uint64, []byte, error)
	SchemaVSN() (uint64, error)

	CreateEntityType(name string) (uint64, error)
	// DeleteEntityType(name string) (uint64, error)
	// GetEntityType(name string, vsn uint64) (schema.EntityType, error)

	CreateEventType(entName, name string, specs interface{}) (uint64, error)
	// UpdateEventType(entName, name string, specs schema.DataSchema) (uint64, uint64, error)
	// GetEventType(entName, name string, vsn uint64) (schema.DataSchema, error)
	// DeleteEventType(entName, name string) (uint64, error)

	NewEntity(entName string, entID []byte) error
	Put(entName string, entID []byte, evtIDenc string, evt interface{}) (uint64, error)
	GetEntity(entName string, entID []byte, vsn uint64) (entity.Entity, error)
}

func NewEventino(db *badger.DB, factory schema.SchemaFactory) Eventino {
	// init schema if necessary
	_ = db.Update(func(txn *badger.Txn) error {
		return schema.EnsureSchema(txn)
	})
	return &eventino{db: db, factory: factory}
}

type eventino struct {
	db      *badger.DB
	scm     *schema.Schema
	factory schema.SchemaFactory
}

func (e *eventino) SchemaVSN() (uint64, error) {
	dec := e.factory.Decoder()
	var latestVSN uint64
	err := e.db.View(func(txn *badger.Txn) (err error) {
		latestVSN, err = schema.SchemaVSN(txn, dec)
		return
	})
	return latestVSN, err
}

func (e *eventino) LoadSchema(vsn uint64) (loadedVsn uint64, encoded []byte, err error) {
	dec := e.factory.Decoder()
	// dptr := &descr
	err = e.db.View(func(txn *badger.Txn) (err error) {
		var scm schema.Schema
		if scm, err = schema.GetSchema(txn, vsn, dec); err != nil {
			return
		}
		e.scm = &scm
		return
	})
	if err != nil {
		return
	}
	loadedVsn = e.scm.VSN
	encoded = e.factory.EncodeNetwork(e.scm)
	return
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

func (e *eventino) CreateEventType(entName, name string, specsNative interface{}) (vsn uint64, err error) {
	dec := e.factory.Decoder()
	var specs schema.DataSchema
	if specs, err = dec.DecodeNative(specsNative); err != nil {
		return 0, err
	}
	err = e.db.Update(func(txn *badger.Txn) (err error) {
		if err = schema.CreateEntityEventType(txn, entName, name, specs); err != nil {
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

func (e *eventino) NewEntity(entName string, entID []byte) error {
	typ, ok := e.scm.Entities[entName]
	if !ok {
		return errors.New("entity-type-not-found")
	}
	return e.db.Update(func(txn *badger.Txn) error {
		return entity.NewEntity(txn, typ, entID)
	})
}

func (e *eventino) Put(entName string, entID []byte, evtIDenc string, evt interface{}) (uint64, error) {
	typ, ok := e.scm.Entities[entName]
	if !ok {
		return 0, errors.New("entity-type-not-found")
	}
	evtID := schema.EventSchemaIDFromString(evtIDenc)
	var vsn uint64
	err := e.db.Update(func(txn *badger.Txn) (err error) {
		vsn, err = entity.Put(txn, typ, entID, evtID, evt)
		return
	})
	return vsn, err
}

func (e *eventino) GetEntity(entName string, entID []byte, vsn uint64) (entity.Entity, error) {
	typ, ok := e.scm.Entities[entName]
	if !ok {
		return entity.Entity{}, errors.New("entity-type-not-found")
	}
	var ent entity.Entity
	err := e.db.View(func(txn *badger.Txn) (err error) {
		ent, err = entity.Get(txn, typ, entID, vsn)
		return
	})
	return ent, err
}
