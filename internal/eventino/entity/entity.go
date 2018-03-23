package entity

import (
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/item"
	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/dgraph-io/badger"
)

// NewEntity initializes a new entity with the given schema
func NewEntity(txn *badger.Txn, entType schema.EntityType, ID []byte) (err error) {
	entID := entType.EntityID(ID)
	if err = item.Create(txn, entID); err != nil {
		return
	}
	// add to index
	// index := schema.EntityIndexID(entType.Name)
	// evt := schema.NewEntityItemIndex(ID)
	// var entIntID uint64
	// if entIntID, err = item.Put(txn, index, evt); err != nil {
	// 	return
	// }
	// TODO: alias the entity instead of adding an event!
	// b := make([]byte, 8)
	// binary.BigEndian.PutUint64(b, entIntID)
	// err = item.Alias(txn, entID, entType.AliasIndexID(entIntID))
	// _, err = item.Put(txn, entID, item.NewEvent(2, []byte("INDEX"), b))

	return
}

// Put adds the given event to the entity
func Put(txn *badger.Txn, entType schema.EntityType, ID []byte, evtID schema.EventSchemaID, evt interface{}) (vsn uint64, err error) {
	var itemEvt item.Event
	entID := entType.EntityID(ID)

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
	if itm, err = item.Get(txn, entType.EntityID(ID), 0, vsn); err != nil {
		return
	}
	if mappedEvts, err = mapEvents(entType, itm.Events); err != nil {
		return
	}
	ent = Entity{
		Type:      EntityType{entType.Name, entType.VSN},
		ID:        ID,
		LatestVSN: itm.LatestVsn,
		VSN:       itm.LoadedVsn,
		Events:    mappedEvts,
	}
	return
}

// Delete deletes an entity
func Delete(txn *badger.Txn, entType schema.EntityType, ID []byte) error {
	return item.Delete(txn, entType.EntityID(ID))
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
			if err != nil && err != EventVSNNotFound {
				return nil, true, err
			} else if err == nil {
				return fold(acc, entEvt, vsn)
			}
			return acc, false, nil
		}
		// TODO: perhaps handle system events too
		return acc, false, nil
	}
	fmt.Println("about to call item.View")
	return item.View(txn, entType.EntityID(ID), fromVsn, itemFold, initial)
}
