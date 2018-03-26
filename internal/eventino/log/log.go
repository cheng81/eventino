package log

import (
	"time"

	"github.com/cheng81/eventino/internal/eventino"
	"github.com/dgraph-io/badger"
)

// Put inserts an event in the log
func Put(txn *badger.Txn, eventIDPrefix uint8, event Event) (out EventID, err error) {
	return PutUnsafe(txn, eventIDPrefix, uint64(time.Now().UnixNano()), event)
}

// PutUnsafe inserts an event in the log at ts timestamp.
// Only use for initial data dump
func PutUnsafe(txn *badger.Txn, prefix uint8, ts uint64, event Event) (out EventID, err error) {
	// instantiate the key
	out = NewEventID(prefix, ts, 0)
	// search for a free spot
	var key []byte
	for {
		key = out.Encode()
		_, err = txn.Get(key)
		if err == nil {
			out.Index++
			continue
		}
		if err == badger.ErrKeyNotFound {
			break
		}
		return out, err
	}
	// set the event
	err = txn.SetWithMeta(key, event.Payload, event.Meta)
	return
}

// Replicate should only be used when the eventino instance is in
// replica mode - it inserts the event at the precise timestamp/index
// parameters
func Replicate(txn *badger.Txn, event Event) error {
	return txn.SetWithMeta(event.ID.Encode(), event.Payload, event.Meta)
}

// Get retrieve the event at the specified index
func Get(txn *badger.Txn, eid EventID) (out Event, err error) {
	key := eid.Encode()
	item, err := txn.Get(key)
	if err != nil {
		return
	}
	return decodeEvent(item)
}

// Range retrieve a chunk of events from the log
func Range(txn *badger.Txn, from EventID, to EventID, max int) ([]Event, *EventID, error) {
	// out := make([]Event, max)
	var out []Event
	var nextEventID *EventID
	var err error

	pfx := from.Encode()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(pfx); iter.ValidForPrefix([]byte{eventino.PfxLog}); iter.Next() {
		item := iter.Item()
		eid := EventID{}
		err = DecodeEventID(item.Key(), &eid)
		if err != nil {
			return nil, nil, err
		}
		// can't go past to
		if eid.Timestamp > to.Timestamp || (eid.Timestamp == to.Timestamp && eid.Index > to.Index) {
			break
		}
		// got enough events
		if len(out) == max {
			nextEventID = &eid
			break
		}

		evt, err := decodeEvent(item)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, evt)
	}

	return out, nextEventID, err
}

// RangeMatch retrieve a chunk of events from the log satisfying the given matcher
func RangeMatch(txn *badger.Txn, from EventID, to EventID, max int, m EventMatcher) ([]Event, *EventID, error) {
	out := make([]Event, max)
	var nextEventID *EventID
	var err error

	pfx := from.Encode()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	added := 0
	defer iter.Close()
	for iter.Seek(pfx); iter.ValidForPrefix([]byte{eventino.PfxLog}); iter.Next() {
		item := iter.Item()
		eid := EventID{}
		err = DecodeEventID(item.Key(), &eid)
		if err != nil {
			return nil, nil, err
		}
		// can't go past to
		if eid.Timestamp > to.Timestamp || (eid.Timestamp == to.Timestamp && eid.Index > to.Index) {
			break
		}
		// got enough events
		if added == max {
			nextEventID = &eid
			break
		}

		evt, err := decodeEvent(item)
		if err != nil {
			return nil, nil, err
		}

		add := m(eid, evt)
		if add {
			out[added] = evt
			added++
		}
	}

	return out[0:added], nextEventID, err
}
