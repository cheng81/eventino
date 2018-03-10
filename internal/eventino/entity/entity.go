package entity

import (
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/item"
	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/dgraph-io/badger"
)

// NewEntity initializes a new entity with the given schema
func NewEntity(txn *badger.Txn, entType schema.EntityType, ID []byte) (err error) {
	entID := entityID(entType, ID)
	err = item.Create(txn, entID)
	return
}

// Put adds the given event to the entity
func Put(txn *badger.Txn, entType schema.EntityType, ID []byte, evtID schema.EventSchemaID, evt interface{}) (vsn uint64, err error) {
	var itemEvt item.Event
	entID := entityID(entType, ID)

	if itemEvt, err = entityEvt(entType, evtID, evt); err != nil {
		return
	}
	if vsn, err = item.Put(txn, entID, itemEvt); err != nil {
		return
	}
	return
}

// Get retrieves an entity
func Get(txn *badger.Txn, entType schema.EntityType, ID []byte, vsn uint64) (ent Entity, err error) {
	var itm item.Item
	var mappedEvts []EntityEvent
	if itm, err = item.Get(txn, entityID(entType, ID), 0, vsn); err != nil {
		return
	}
	if mappedEvts, err = mapEvents(entType, itm.Events); err != nil {
		return
	}
	ent = Entity{
		Type:   EntityType{entType.Name, entType.VSN},
		ID:     ID,
		Events: mappedEvts,
	}
	return
}

// Delete deletes an entity
func Delete(txn *badger.Txn, entType schema.EntityType, ID []byte) error {
	return item.Delete(txn, entityID(entType, ID))
}

func View(txn *badger.Txn,
	entType schema.EntityType,
	ID []byte,
	fromVsn uint64,
	fold ViewFoldFunc,
	initial interface{}) (interface{}, uint64, error) {
	itemFold := func(acc interface{}, evt item.Event, vsn uint64) (interface{}, bool, error) {
		fmt.Println("entity.View", acc, evt)
		if evt.Kind == EventKindEntity {
			entEvt, err := mapEvent(entType, evt)
			if err != nil {
				fmt.Println("entity.View cannot mapEvent", err)
				return nil, true, err
			}
			return fold(acc, entEvt, vsn)
		}
		// TODO: perhaps handle system events too
		return acc, false, nil
	}
	fmt.Println("about to call item.View")
	return item.View(txn, entityID(entType, ID), fromVsn, itemFold, initial)
}
