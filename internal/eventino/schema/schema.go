package schema

import (
	"errors"

	"github.com/cheng81/eventino/internal/eventino/item"
	"github.com/dgraph-io/badger"
)

// EnsureSchema ensures that the item "SCHEMA" exists
func EnsureSchema(txn *badger.Txn) (err error) {
	var exists bool
	if exists, err = item.Exists(txn, schemaID); err != nil {
		return
	}
	if !exists {
		err = item.Create(txn, schemaID)
	}
	return
}

// GetSchema returns the schema at the given version
func GetSchema(txn *badger.Txn, vsn uint64, dec SchemaDecoder) (out []EntityType, err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var scm schema
	if scm, err = getSchema(txn, dec, func(s schema) bool { return s.vsn > vsn }); err != nil {
		return
	}
	out = make([]EntityType, 0, len(scm.entities))
	for _, ent := range scm.entities {
		out = append(out, ent)
	}
	return out, nil
}

// SchemaVSN returns the latest version of the schema
func SchemaVSN(txn *badger.Txn, dec SchemaDecoder) (vsn uint64, err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var scm schema
	if scm, err = getSchema(txn, dec, func(_ schema) bool { return false }); err != nil {
		return
	}
	vsn = scm.vsn
	return
}

// CreateEntityType initializes a new entity type
func CreateEntityType(txn *badger.Txn, name string) (err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var evt item.Event
	if evt, err = newEntityCreated(name); err != nil {
		return
	}
	_, err = item.Put(txn, schemaID, evt)
	return
}

// DeleteEntityType removes an entity type from the schema
func DeleteEntityType(txn *badger.Txn, name string) (err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var evt item.Event
	if evt, err = newEntityDeleted(name); err != nil {
		return
	}
	_, err = item.Put(txn, schemaID, evt)
	return
}

// GetEntityType returns the entity schema at the given version
func GetEntityType(txn *badger.Txn, dec SchemaDecoder, name string, vsn uint64) (out EntityType, err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var scm schema
	stopper := func(s schema) bool {
		if entS, ok := s.entities[name]; ok {
			return entS.VSN > vsn
		}
		return false
	}
	if scm, err = getSchema(txn, dec, stopper); err != nil {
		return
	}
	var ok bool
	if out, ok = scm.entities[name]; !ok {
		return out, errors.New("entity type not found")
	}
	return
}

// CreateEntityEventType creates a new event type for the entity with the given data schema
func CreateEntityEventType(txn *badger.Txn, entName, evtName string, schema DataSchema) (err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var evt item.Event
	if evt, err = newEventTypeCreated(entName, evtName, schema); err != nil {
		return
	}
	_, err = item.Put(txn, schemaID, evt)
	return
}

// UpdateEventType updates the event type with a new data schema
func UpdateEventType(txn *badger.Txn, entName, evtName string, schema DataSchema) (vsn uint64, err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}

	// add event
	var evt item.Event
	if evt, err = newEventTypeUpdated(entName, evtName, schema); err != nil {
		return
	}
	if _, err = item.Put(txn, schemaID, evt); err != nil {
		return
	}

	// load latest schema
	dec := schema.SchemaDecoder()
	var schemaLatestVSN uint64
	if schemaLatestVSN, err = SchemaVSN(txn, dec); err != nil {
		return
	}

	var scm EntityType
	// we load the latest version by using schemaLastestVSN as VSN:
	// it is not the precise version, but an upperbound, since the
	// overall schema is guaranteed to have a higher VSN
	if scm, err = GetEntityType(txn, dec, entName, schemaLatestVSN); err != nil {
		return
	}

	// get latest event type VSN
	for evtID := range scm.Events {
		if evtID.Name == evtName && evtID.VSN > vsn {
			vsn = evtID.VSN
		}
	}
	return
}

// DeleteEventType removes the event from the entity schema
func DeleteEventType(txn *badger.Txn, entName, evtName string) (err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var evt item.Event
	if evt, err = newEventTypeDeleted(entName, evtName); err != nil {
		return
	}
	_, err = item.Put(txn, schemaID, evt)
	return
}

// GetEventType returns the event schema at the given version
func GetEventType(txn *badger.Txn, dec SchemaDecoder, entName, evtName string, vsn uint64) (out DataSchema, err error) {
	if err = EnsureSchema(txn); err != nil {
		return
	}
	var scm schema
	evtKey := EventSchemaID{Name: evtName, VSN: vsn}
	stopper := func(s schema) bool {
		if entS, ok := s.entities[entName]; ok {
			if _, ok := entS.Events[evtKey]; ok {
				return true
			}
		}
		return false
	}
	if scm, err = getSchema(txn, dec, stopper); err != nil {
		return
	}
	var ok bool
	var entity EntityType
	if entity, ok = scm.entities[entName]; ok {
		if out, ok = entity.Events[evtKey]; ok {
			return
		}
		// TODO: perhaps we should return the latest
		// known version instead of error out?
		err = errors.New("event version not found")
	} else {
		err = errors.New("entity type not found")
	}
	return
}
