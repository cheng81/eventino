package item

import (
	"encoding/binary"

	"github.com/cheng81/eventino/internal/eventino/log"
)

// Event is an item event
type Event struct {
	Kind    byte
	LogID   log.EventID
	Type    []byte
	Payload []byte
}

// ItemID is an item ID (duh)
type ItemID struct {
	Type uint8
	ID   []byte
}

// Item is an event-sourcing entity
type Item struct {
	LatestVsn uint64
	LoadedVsn uint64
	ID        ItemID
	Events    []Event
}

// IDEvent is an event in a Range* query
type IDEvent struct {
	ID    ItemID
	Event Event
}

// ViewFoldFunc func type fold events into an accumulator
// and return the final result. The output boolean can be
// used to return earlier
type ViewFoldFunc func(interface{}, Event, uint64) (interface{}, bool, error)

type PersistentViewFold interface {
	DecodeState([]byte) (interface{}, error)
	EncodeState(interface{}) []byte
	Fold(interface{}, Event, uint64) (interface{}, bool, error)
}

var createdEventType []byte
var deletedEventType []byte
var aliasEventType []byte
var aliasDeleteEventType []byte

// CREATED is the shared Item Created event
var CREATED Event

// DELETED is the shared Item Deleted event
var DELETED Event

func init() {
	createdEventType = []byte{99}
	deletedEventType = []byte{100}
	aliasEventType = []byte{97}
	aliasDeleteEventType = []byte{120}
	CREATED = Event{Kind: EventKindSystem, Type: createdEventType, Payload: []byte{}}
	DELETED = Event{Kind: EventKindSystem, Type: deletedEventType, Payload: []byte{}}
}

// NewAliasEvent returns an alias event
func NewAliasEvent(aliasID ItemID) Event {
	return Event{
		Kind:    EventKindSystem,
		Type:    aliasEventType,
		Payload: aliasID.Encode(),
	}
}

// NewAliasDeleteEvent returns an alias delete event
func NewAliasDeleteEvent(aliasID ItemID) Event {
	return Event{
		Kind:    EventKindSystem,
		Type:    aliasDeleteEventType,
		Payload: aliasID.Encode(),
	}
}

// Encode encodes the ItemID into a []byte
func (id ItemID) Encode() []byte {
	var out = make([]byte, 2+len(id.ID))
	id.encodeInto(out)
	return out
}
func (id ItemID) encodeInto(b []byte) {
	binary.BigEndian.PutUint16(b, uint16(id.Type))
	copy(b[2:], id.ID)
}
func (id ItemID) BaseKey() []byte {
	var out = make([]byte, 3+len(id.ID))
	out[0] = itemItemPfx
	id.encodeInto(out[1:])
	return out
}
func (id ItemID) baseKeyInto(b []byte) {
	b[0] = itemItemPfx
	id.encodeInto(b[1:])
}
func (id ItemID) keyOf(b byte) []byte {
	l := len(id.ID) + 4
	out := make([]byte, l)
	id.keyOfInto(b, out)
	return out
}
func (id ItemID) keyOfInto(b byte, bs []byte) {
	l := len(id.ID) + 4
	id.baseKeyInto(bs)
	bs[l-1] = b
}
func (id ItemID) KeyVSN() []byte {
	return id.keyOf(itemKeyVSN)
}
func (id ItemID) KeyAliases() []byte {
	return id.keyOf(itemKeyAliases)
}
func (id ItemID) KeyEventsBase() []byte {
	return id.keyOf(itemKeyEvents)
}
func (id ItemID) KeyEventVsn(vsn uint64) []byte {
	out := make([]byte, 12+len(id.ID))
	id.keyOfInto(itemKeyEvents, out)
	binary.BigEndian.PutUint64(out[4+len(id.ID):], vsn)
	return out
}
func (id ItemID) KeyViews() []byte {
	return id.keyOf(itemKeyView)
}
func (id ItemID) KeyView(name []byte) []byte {
	out := make([]byte, len(id.ID)+len(name)+4)
	id.keyOfInto(itemKeyView, out[0:len(id.ID)+4])
	copy(out[len(id.ID)+4:], name)
	return out
}
func (id ItemID) AliasKey() []byte {
	var out = make([]byte, 3+len(id.ID))
	out[0] = aliasItemPfx
	id.encodeInto(out[1:])
	return out
}
func (id ItemID) VSNFromEventKey(k []byte) (uint64, error) {
	if len(k) != 12+len(id.ID) ||
		k[0] != itemItemPfx ||
		k[len(id.ID)+3] != itemKeyEvents {
		return 0, KeyError
	}
	return binary.BigEndian.Uint64(k[4+len(id.ID):]), nil
}

// DecodeItemID parse the []byte into an ItemID.
// Returns NoItemIDError if the len([]byte) < 2
func DecodeItemID(b []byte) (out ItemID, err error) {
	if len(b) < 2 {
		return out, NoItemIDError
	}
	out.Type = uint8(binary.BigEndian.Uint16(b[0:2]))
	out.ID = b[2:]
	return
}

// AliasFromEvent returns the destination ItemID
// contained in the given event Payload
func AliasFromEvent(evt Event) (ItemID, error) {
	if len(evt.Type) != 1 && evt.Type[0] != aliasEventType[0] {
		return ItemID{}, NotAliasEvent
	}
	out := ItemID{
		Type: uint8(binary.BigEndian.Uint16(evt.Payload[0:2])),
		ID:   evt.Payload[2:],
	}
	return out, nil
}
