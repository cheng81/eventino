package log

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger"
)

// const logItemPfx byte = 101 // byte('e')

// NoLogEventIDError is returned when trying to decode
// a []byte as EventID that does not encode an EventID
var NoLogEventIDError error

func init() {
	NoLogEventIDError = errors.New("Not a log item id")
}

// NewEventID produce an EventID from the given prefix,
// timestamp and index
func NewEventID(prefix uint8, ts uint64, idx uint16) EventID {
	return EventID{
		Prefix:    prefix,
		Timestamp: ts,
		Index:     idx,
	}
}

// NewEventIDNow produce an EventID for the gien prefix,
// with timestamp = `uint64(time.Now().UnixName())` and index 0
func NewEventIDNow(prefix uint8) EventID {
	return EventID{
		Prefix:    prefix,
		Timestamp: uint64(time.Now().UnixNano()),
		Index:     0,
	}
}

func decodeEvent(item *badger.Item) (out Event, err error) {
	id := EventID{}
	if err = DecodeEventID(item.Key(), &id); err != nil {
		return
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return
	}
	out = Event{
		ID:      id,
		Meta:    item.UserMeta(),
		Payload: val,
	}
	return
}
