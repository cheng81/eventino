package item

import (
	"bytes"
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/log"
	"github.com/dgraph-io/badger"
)

// Exists checks whether the given ID exists
func Exists(txn *badger.Txn, ID ItemID) (bool, error) {
	return itemExists(txn, ID)
}

// Create initializes an Item with the system event CREATED
func Create(txn *badger.Txn, ID ItemID) (err error) {
	// errors out if ID exists
	if exists, err := itemExists(txn, ID); err != nil || exists {
		if err != nil {
			return err
		}
		return ItemExistsError
	}
	// initilize item
	if err = initItem(txn, ID); err != nil {
		return
	}

	_, err = Put(txn, ID, CREATED)
	return
}

// Put adds an event to the item
func Put(txn *badger.Txn, ID ItemID, evt Event) (vsn uint64, err error) {
	// wrap event into log.Event
	logEvent, err := wrapLogEvent(ID, evt)
	if err != nil {
		return
	}
	// store in log
	logEventID, err := log.Put(txn, ID.Type, logEvent)
	if err != nil {
		return
	}
	// add created event ptr
	return putItem(txn, ID, logEventID)
}

// Get retrieves an item matching from-to versions
func Get(txn *badger.Txn, ID ItemID, fromVsn uint64, toVsn uint64) (out Item, err error) {
	out = Item{
		ID:     ID,
		Events: []Event{},
	}

	var exists bool
	if exists, err = itemExists(txn, ID); err != nil || !exists {
		if err != nil {
			return
		}
		return out, ItemNotFoundError
	}

	var nextVsn uint64
	if nextVsn, err = itemVsn(txn, ID); err != nil {
		return
	}
	out.LatestVsn = nextVsn - 1

	var event Event
	var evVSN uint64
	var val []byte

	pfx := ID.KeyEventsBase()
	start := ID.KeyEventVsn(fromVsn)
	hasStop := toVsn > fromVsn

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(start); iter.ValidForPrefix(pfx); iter.Next() {
		item := iter.Item()
		if evVSN, err = ID.VSNFromEventKey(item.Key()); err != nil {
			return
		}
		if hasStop && evVSN > toVsn {
			break
		}
		if val, err = item.Value(); err != nil {
			return
		}
		var logEventID log.EventID
		if err = log.DecodeEventID(val, &logEventID); err != nil {
			return
		}
		var logEvt log.Event
		if logEvt, err = log.Get(txn, logEventID); err != nil {
			return
		}
		if event, err = unwrapLogEvent(logEvt); err != nil {
			return
		}
		out.Events = append(out.Events, event)
		out.LoadedVsn = evVSN
	}

	return
}

// Delete removes an item (puts system event DELETED and cleans up other keys)
func Delete(txn *badger.Txn, ID ItemID) (err error) {
	// add system event DELETED
	if _, err = Put(txn, ID, DELETED); err != nil {
		return
	}

	return deleteItem(txn, ID)
}

// Alias links the alias ItemID to the src ItemID
// errors out if the alias already exists.
// Can be used to enforce unique constraints
func Alias(txn *badger.Txn, src ItemID, alias ItemID) (err error) {
	var exists bool
	var item *badger.Item

	if exists, err = aliasExists(txn, alias); err != nil || exists {
		if err != nil {
			return
		}
		return AliasExistsError
	}

	if _, err = Put(txn, src, NewAliasEvent(alias)); err != nil {
		return
	}

	if err = txn.Set(alias.AliasKey(), src.Encode()); err != nil {
		return
	}

	var aliases aliasesWire
	var val []byte
	if item, err = txn.Get(src.KeyAliases()); err != nil {
		return
	}
	if val, err = item.Value(); err != nil {
		return
	}
	if err = decode(val, &aliases); err != nil {
		return
	}
	aliases.Aliases = append(aliases.Aliases, alias)
	if val, err = encode(aliases); err != nil {
		return
	}
	err = txn.Set(src.KeyAliases(), val)

	return
}

// AliasDelete removes an item alias
func AliasDelete(txn *badger.Txn, src, alias ItemID) (err error) {
	var aliasSrcID ItemID
	if aliasSrcID, err = aliasResolve(txn, alias); err != nil {
		return
	}
	if bytes.Compare(src.ID, aliasSrcID.ID) != 0 || src.Type != aliasSrcID.Type {
		err = AliasNotFoundInItemError
		return
	}

	if _, err = Put(txn, src, NewAliasDeleteEvent(alias)); err != nil {
		return
	}

	if err = txn.Delete(alias.AliasKey()); err != nil {
		return
	}

	var item *badger.Item
	var val []byte
	var aliases aliasesWire
	if item, err = txn.Get(src.KeyAliases()); err != nil {
		return
	}
	if val, err = item.Value(); err != nil {
		return
	}
	if err = decode(val, &aliases); err != nil {
		return
	}

	aliasID := alias.ID
	for i := len(aliases.Aliases) - 1; i >= 0; i-- {
		if bytes.Compare(aliases.Aliases[i].ID, aliasID) == 0 && aliases.Aliases[i].Type == alias.Type {
			aliases.Aliases = append(aliases.Aliases[:i], aliases.Aliases[i+1:]...)
			break
		}
	}

	if val, err = encode(aliases); err != nil {
		return
	}
	err = txn.Set(src.KeyAliases(), val)

	return
}

// LatestVSN returns the latest version of the item
func LatestVSN(txn *badger.Txn, ID ItemID) (out uint64, err error) {
	return itemVsn(txn, ID)
}

// GetByAlias retrieves an item from the given aliasID
func GetByAlias(txn *badger.Txn, aliasID ItemID, fromVsn uint64, toVsn uint64) (out Item, err error) {
	var srcID ItemID
	if srcID, err = aliasResolve(txn, aliasID); err != nil {
		return
	}
	return Get(txn, srcID, fromVsn, toVsn)
}

// GetView returns the current state of the requested persistent view
func GetView(txn *badger.Txn, ID ItemID, stateName []byte, view PersistentViewFold) (uint64, interface{}, error) {
	k := ID.KeyView(stateName)
	var item *badger.Item
	var val []byte
	var err error
	if item, err = txn.Get(k); err != nil {
		return 0, nil, err
	}
	if val, err = item.Value(); err != nil {
		return 0, nil, err
	}
	var wire viewWire
	if err := decode(val, &wire); err != nil {
		return 0, nil, err
	}
	var decoded interface{}
	decoded, err = view.DecodeState(wire.View)
	return wire.Vsn, decoded, err
}

// SyncPersistentView applies the fold function to the item events,
// and persist the result
func SyncPersistentView(txn *badger.Txn, ID ItemID, stateName []byte,
	view PersistentViewFold, initial interface{}) (err error) {
	var k []byte
	var item *badger.Item
	var val []byte
	var state interface{}
	var vsn uint64
	var wire viewWire

	k = ID.KeyView(stateName)
	if item, err = txn.Get(k); err != nil && err != badger.ErrKeyNotFound {
		return
	}
	if err == badger.ErrKeyNotFound {
		if item, err = txn.Get(ID.KeyViews()); err != nil {
			return
		}
		if val, err = item.Value(); err != nil {
			return
		}
		var views [][]byte
		if err = decode(val, &views); err != nil {
			return
		}
		views = append(views, stateName)
		if val, err = encode(views); err != nil {
			return
		}
		if err = txn.Set(ID.KeyViews(), val); err != nil {
			return
		}
		wire.Vsn = 0
		wire.View = view.EncodeState(initial)
	} else {
		if val, err = item.Value(); err != nil {
			return
		}
		if err = decode(val, &wire); err != nil {
			return
		}
		wire.Vsn++
	}

	fmt.Println("loading state")
	vsn = wire.Vsn
	if state, err = view.DecodeState(wire.View); err != nil {
		return
	}

	if state, vsn, err = View(txn, ID, vsn, view.Fold, state); err != nil {
		return
	}

	wire.Vsn = vsn
	wire.View = view.EncodeState(state)
	if val, err = encode(wire); err != nil {
		return
	}
	return txn.Set(k, val)
}

// View applies the fold function to all events of the item
func View(txn *badger.Txn, ID ItemID, fromVsn uint64, fold ViewFoldFunc, initial interface{}) (out interface{}, vsn uint64, err error) {
	var val []byte
	var event Event
	var stop bool

	out = initial
	init := ID.KeyEventVsn(fromVsn)
	pfx := ID.KeyEventsBase()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	fmt.Println("item.View start")
	for iter.Seek(init); iter.ValidForPrefix(pfx); iter.Next() {
		item := iter.Item()
		if vsn, err = ID.VSNFromEventKey(item.Key()); err != nil {
			return
		}
		if val, err = item.Value(); err != nil {
			return
		}
		var logEventID log.EventID
		if err = log.DecodeEventID(val, &logEventID); err != nil {
			return
		}
		var logEvt log.Event
		if logEvt, err = log.Get(txn, logEventID); err != nil {
			return
		}
		if event, err = unwrapLogEvent(logEvt); err != nil {
			return
		}
		fmt.Println("item.View call fold")
		if out, stop, err = fold(out, event, vsn); err != nil {
			fmt.Println("outch!", err)
			return
		}
		if stop {
			break
		}
	}

	return
}

// RangePrefix loads from the log a chunk of item events, matching a given item prefix
func RangePrefix(txn *badger.Txn, itemPfx ItemID, from, to log.EventID, max int) ([]IDEvent, *log.EventID, error) {
	// make a filter & map function
	folder := log.EventFolder(func(acc interface{}, lEvtID log.EventID, lEvt log.Event) (interface{}, error) {
		evt, err := unwrapLogEventWire(lEvt)
		if err != nil {
			return nil, err
		}
		id := evt.ID
		if id.Type == itemPfx.Type && bytes.HasPrefix(id.ID, itemPfx.ID) {
			elm := IDEvent{
				ID:    evt.ID,
				Event: Event{LogID: lEvt.ID, Kind: lEvt.Meta, Type: evt.EventType, Payload: evt.Payload},
			}
			return append(acc.([]IDEvent), elm), nil
		}
		return acc, nil
	})

	acc, lastEvtID, err := log.Fold(txn, from, to, max, folder, []IDEvent{})
	if err != nil {
		return nil, nil, err
	}
	return acc.([]IDEvent), lastEvtID, err
}

// Replicate applies the changes specified in the log.EventReplica
// to the item layer. It will not add the event to the log,
// caller should ensure to write the event to the log.
func Replicate(txn *badger.Txn, evt log.Event) error {
	eventWire, err := unwrapLogEventWire(evt)
	if err != nil {
		return err
	}
	ID := eventWire.ID
	event := Event{
		LogID:   evt.ID,
		Kind:    evt.Meta,
		Type:    eventWire.EventType,
		Payload: eventWire.Payload,
	}
	if IsCreatedEvent(event) {
		if err = initItem(txn, ID); err != nil {
			return err
		}
		if _, err = putItem(txn, ID, evt.ID); err != nil {
			return err
		}
		return nil
	}
	if IsDeletedEvent(event) {
		if _, err = putItem(txn, ID, evt.ID); err != nil {
			return err
		}
		return deleteItem(txn, ID)
	}
	if IsAliasEvent(event) {
		if _, err = putItem(txn, ID, evt.ID); err != nil {
			return err
		}
		aliasID, err := AliasFromEvent(event)
		if err != nil {
			return err
		}
		if err = txn.Set(aliasID.AliasKey(), ID.Encode()); err != nil {
			return err
		}

		var item *badger.Item
		var aliases aliasesWire
		var val []byte
		if item, err = txn.Get(ID.KeyAliases()); err != nil {
			return err
		}
		if val, err = item.Value(); err != nil {
			return err
		}
		if err = decode(val, &aliases); err != nil {
			return err
		}
		aliases.Aliases = append(aliases.Aliases, aliasID)
		if val, err = encode(aliases); err != nil {
			return err
		}
		return txn.Set(ID.KeyAliases(), val)
	}
	if IsAliasDeleteEvent(event) {
		if _, err = putItem(txn, ID, evt.ID); err != nil {
			return err
		}
		aliasID, err := AliasFromEvent(event)
		if err != nil {
			return err
		}
		if err = txn.Delete(aliasID.AliasKey()); err != nil {
			return err
		}
		var item *badger.Item
		var aliases aliasesWire
		var val []byte
		if item, err = txn.Get(ID.KeyAliases()); err != nil {
			return err
		}
		if val, err = item.Value(); err != nil {
			return err
		}
		if err = decode(val, &aliases); err != nil {
			return err
		}
		aliasIDID := aliasID.ID
		for i := len(aliases.Aliases) - 1; i >= 0; i-- {
			if bytes.Compare(aliases.Aliases[i].ID, aliasIDID) == 0 && aliases.Aliases[i].Type == aliasID.Type {
				aliases.Aliases = append(aliases.Aliases[:i], aliases.Aliases[i+1:]...)
				break
			}
		}

		if val, err = encode(aliases); err != nil {
			return err
		}
		return txn.Set(ID.KeyAliases(), val)
	}

	// generic event
	_, err = putItem(txn, ID, evt.ID)

	return err
}

func deleteItem(txn *badger.Txn, ID ItemID) (err error) {
	// delete VSN key
	if err = txn.Delete(ID.KeyVSN()); err != nil {
		return
	}

	var item *badger.Item

	// delete ALIASES
	if item, err = txn.Get(ID.KeyAliases()); err != nil {
		return
	}
	var val []byte
	if val, err = item.Value(); err != nil {
		return
	}
	var aliases aliasesWire
	if err = decode(val, &aliases); err != nil {
		return
	}
	for _, alias := range aliases.Aliases {
		if err = txn.Delete(alias.AliasKey()); err != nil {
			return
		}
	}
	// delete alias list
	if err = txn.Delete(ID.KeyAliases()); err != nil {
		return
	}

	// delete VIEWS
	if item, err = txn.Get(ID.KeyViews()); err != nil {
		return
	}
	if val, err = item.Value(); err != nil {
		return
	}
	var views [][]byte
	if err = decode(val, &views); err != nil {
		return
	}
	for _, view := range views {
		if err = txn.Delete(ID.KeyView(view)); err != nil {
			return
		}
	}
	// delete view list
	if err = txn.Delete(ID.KeyViews()); err != nil {
		return
	}

	// delete events
	pfx := ID.KeyEventsBase()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()
	for iter.Seek(pfx); iter.ValidForPrefix(pfx); iter.Next() {
		item = iter.Item()
		// calling txn.Delete(it.Key())
		// has some weird results, as under the hood
		// badger re-uses *badger.Item, and it doesn't
		// delete right away. When the time comes to delete,
		// what's inside the []byte is pretty much garbage
		var key []byte
		key = append(key[:0], item.Key()...)
		if err = txn.Delete(key); err != nil {
			return
		}
	}
	return
}
