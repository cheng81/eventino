package log

import (
	"encoding/binary"

	"github.com/cheng81/eventino/internal/eventino"
)

// EventID represents an log item
type EventID struct {
	// Prefix is used by Schema and Data layer to
	// partition the event log - should really
	// be a bit
	Prefix    uint8
	Timestamp uint64
	Index     uint16
}

// Event is a log item
type Event struct {
	Meta    byte
	ID      EventID // set when reading. Ignored on Put
	Payload []byte
}

// EventMatcher implements event filtering
type EventMatcher func(EventID, Event) bool

// EventFolder implements event folding facility
type EventFolder func(interface{}, EventID, Event) (interface{}, error)

// Encode returns the []byte representation to be used as badger key
func (eid EventID) Encode() (out []byte) {
	out = make([]byte, 13)
	out[0] = eventino.PfxLog
	binary.BigEndian.PutUint16(out[1:3], uint16(eid.Prefix))
	binary.BigEndian.PutUint64(out[3:11], eid.Timestamp)
	binary.BigEndian.PutUint16(out[11:13], eid.Index)
	return
}

// DecodeEventID reads the bytes and fills the *EventID.
// Might return NoLogItemIDError
func DecodeEventID(b []byte, eid *EventID) error {
	if b[0] != eventino.PfxLog {
		return NoLogEventIDError
	}
	eid.Prefix = uint8(binary.BigEndian.Uint16(b[1:3]))
	eid.Timestamp = binary.BigEndian.Uint64(b[3:11])
	eid.Index = binary.BigEndian.Uint16(b[11:13])
	return nil
}
