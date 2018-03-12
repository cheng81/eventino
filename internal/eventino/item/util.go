package item

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"

	"github.com/cheng81/eventino/internal/eventino/log"
	"github.com/dgraph-io/badger"
)

const (
	EventKindSystem byte = 2
)

const (
	itemItemPfx    byte = 105 // byte('i')
	aliasItemPfx   byte = 97  // a
	itemKeyVSN     byte = 118 // v
	itemKeyAliases byte = 97  // a
	itemKeyEvents  byte = 101 // e
	itemKeyView    byte = 115 // s
)

var NotAliasEvent error

// NoItemIDError is returned when trying to decode
// a []byte as ItemID that does not encode an ItemID
var NoItemIDError error

var ItemExistsError error

var KeyError error

var ItemNotFoundError error

var AliasExistsError error

var AliasNotFoundError error

var AliasNotFoundInItemError error

func init() {
	NotAliasEvent = errors.New("Not an alias event")
	NoItemIDError = errors.New("Not an itemId")
	ItemExistsError = errors.New("Item exists")
	KeyError = errors.New("Key error")
	ItemNotFoundError = errors.New("Item not found")
	AliasExistsError = errors.New("Alias exists")
	AliasNotFoundError = errors.New("Alias not found")
	AliasNotFoundInItemError = errors.New("Alias not found in item")
}

func NewItemID(itemType uint8, id []byte) ItemID {
	return ItemID{Type: itemType, ID: id}
}

func NewEvent(kind byte, eType []byte, payload []byte) Event {
	return Event{Kind: kind, Type: eType, Payload: payload}
}

func IsCreatedEvent(e Event) bool {
	return len(e.Type) == len(createdEventType) && len(e.Type) == 1 && e.Type[0] == createdEventType[0]
}

func IsDeletedEvent(e Event) bool {
	return len(e.Type) == len(deletedEventType) && len(e.Type) == 1 && e.Type[0] == deletedEventType[0]
}

func IsAliasEvent(e Event) bool {
	return len(e.Type) == len(aliasEventType) && len(e.Type) == 1 && e.Type[0] == aliasEventType[0]
}

func IsAliasDeleteEvent(e Event) bool {
	return len(e.Type) == len(aliasDeleteEventType) && len(e.Type) == 1 && e.Type[0] == aliasDeleteEventType[0]
}

func itemExists(txn *badger.Txn, id ItemID) (bool, error) {
	_, err := txn.Get(id.KeyVSN())
	if err == nil {
		return true, nil
	}
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return false, err
}

func aliasExists(txn *badger.Txn, alias ItemID) (bool, error) {
	_, err := txn.Get(alias.AliasKey())
	if err == nil {
		return true, nil
	}
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return false, err
}

func aliasResolve(txn *badger.Txn, aliasID ItemID) (srcID ItemID, err error) {
	var exists bool
	var item *badger.Item
	if exists, err = aliasExists(txn, aliasID); !exists || err != nil {
		if err != nil {
			return
		}
		return ItemID{}, AliasNotFoundError
	}
	if item, err = txn.Get(aliasID.AliasKey()); err != nil {
		return
	}
	var val []byte
	if val, err = item.Value(); err != nil {
		return
	}
	srcID, err = DecodeItemID(val)

	return
}

func wrapLogEvent(ID ItemID, evt Event) (log.Event, error) {
	out := log.Event{Meta: evt.Kind}
	b, err := encode(eventWire{ID: ID, EventType: evt.Type, Payload: evt.Payload})
	if err != nil {
		return out, err
	}
	out.Payload = b
	return out, nil
}

func unwrapLogEvent(evt log.Event) (out Event, err error) {
	out = Event{
		Kind:  evt.Meta,
		LogID: evt.ID,
	}
	wire := eventWire{}
	if err = decode(evt.Payload, &wire); err != nil {
		return
	}
	out.Type = wire.EventType
	out.Payload = wire.Payload
	return
}

func unwrapLogEventWire(evt log.Event) (out eventWire, err error) {
	out = eventWire{}
	err = decode(evt.Payload, &out)
	return
}

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

func set(txn *badger.Txn, k []byte, v interface{}) (err error) {
	var b []byte
	if b, err = encode(v); err != nil {
		return
	}
	return txn.Set(k, b)
}

func setUint64(txn *badger.Txn, k []byte, v uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return txn.Set(k, b)
}

func initItem(txn *badger.Txn, ID ItemID) (err error) {
	var k []byte

	// init vsn to 0
	k = ID.KeyVSN()
	err = setUint64(txn, k, 0)

	// init aliases to empty array
	k = ID.KeyAliases()
	err = set(txn, k, aliasesWire{})

	// init views
	k = ID.KeyViews()
	err = set(txn, k, [][]byte{})
	return
}

func putItem(txn *badger.Txn, ID ItemID, eid log.EventID) (vsn uint64, err error) {
	// get the vsn
	vsn, err = itemVsn(txn, ID)
	// set the current version to the event pointer
	k := ID.KeyEventVsn(vsn)
	if err = txn.Set(k, eid.Encode()); err != nil {
		return
	}
	// set next version
	k = ID.KeyVSN()
	err = setUint64(txn, k, vsn+1)
	return
}

func itemVsn(txn *badger.Txn, ID ItemID) (out uint64, err error) {
	k := ID.KeyVSN()

	item, err := txn.Get(k)
	if err != nil {
		return
	}
	b, err := item.Value()
	if err != nil {
		return
	}
	return binary.BigEndian.Uint64(b), nil
}
