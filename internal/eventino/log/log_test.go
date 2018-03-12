package log

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func withTempDB(fn func(*badger.DB) error) error {
	dbDir := fmt.Sprintf("/tmp/badger-%d", time.Now().UnixNano())
	log.Println("start")
	opts := badger.DefaultOptions
	opts.Dir = dbDir
	opts.ValueDir = dbDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return err
	}
	if err = fn(db); err != nil {
		return err
	}
	size, _ := DirSize(dbDir)
	sizeMB := float64(size) / 1024.0 / 1024.0
	fmt.Println("DB dir size: ", sizeMB, "MB")

	defer db.Close()
	defer os.RemoveAll(dbDir)
	return nil
}

func TestPutGet(t *testing.T) {
	withTempDB(func(db *badger.DB) error {
		var err error
		var eventID EventID
		origEvt := Event{Meta: 0, Payload: []byte("0")}
		err = db.Update(func(txn *badger.Txn) error {
			eventID, err = Put(txn, 1, origEvt)
			return err
		})
		if err != nil {
			return err
		}

		var loadedEvt Event
		err = db.View(func(txn *badger.Txn) error {
			loadedEvt, err = Get(txn, eventID)
			return err
		})
		if err != nil {
			return err
		}

		if loadedEvt.Meta != origEvt.Meta {
			t.Fatal("!= meta", uint(loadedEvt.Meta))
		}
		if string(loadedEvt.Payload) != string(origEvt.Payload) {
			t.Fatal("!= payload", string(loadedEvt.Payload))
		}
		return err
	})
}

func TestOrder(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		startTs := time.Now()
		err = db.Update(func(txn *badger.Txn) (err error) {
			for i := 0; i < 10; i++ {
				_, err = Put(txn, 1, Event{Meta: 2, Payload: []byte(fmt.Sprintf("%d", i))})
				if err != nil {
					return
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}
		var events []Event
		err = db.View(func(txn *badger.Txn) (err error) {
			events, _, err = Range(txn,
				NewEventID(1, uint64(startTs.UnixNano()), 0),
				NewEventID(1, uint64(time.Now().UnixNano()), 0), 100)
			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}
		if len(events) != 10 {
			t.Fatal("not loaded enough events", len(events))
		}
		if events[0].Meta != 2 {
			t.Fatal("evt.0-meta != 2", events[0].Meta)
		}

		evtExpect := func(e Event, expected string) {
			if string(e.Payload) != expected {
				t.Fatalf("expected %s but got %s instead", expected, string(e.Payload))
			}
		}

		evtExpect(events[0], "0")
		evtExpect(events[5], "5")
		evtExpect(events[9], "9")

		return
	})
}

func TestReplicate(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		ts := uint64(time.Now().UnixNano())
		err = db.Update(func(txn *badger.Txn) (err error) {
			for i := 0; i < 10; i++ {
				_, err = PutUnsafe(txn, 1, ts, Event{Meta: 2, Payload: []byte(fmt.Sprintf("%d", i))})
				if err != nil {
					return
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var nextID *EventID
			from := NewEventID(0, ts, 0)
			to := NewEventIDNow(0)
			events, nextID, err := Range(txn, from, to, 100)
			if nextID != nil {
				t.Fatal("nextID should be nil", nextID)
			}
			if err != nil {
				return
			}
			for idx, event := range events {
				if event.ID.Prefix != 1 {
					t.Fatal("wrong ID prefix", event.ID.Prefix, 1)
				}
				if event.ID.Timestamp != ts {
					t.Fatal("wrong ID timestamp", event.ID.Timestamp, ts)
				}
				if event.ID.Index != uint16(idx) {
					t.Fatal("wrong ID index", event.ID.Index, idx)
				}
				if string(event.Payload) != fmt.Sprintf("%d", idx) {
					t.Fatal("wrong Event payload", string(event.Payload), idx)
				}
			}

			if err = withTempDB(func(db2 *badger.DB) error { return testReplicate(t, events, db2, from, to) }); err != nil {
				t.Fatal("cannot replicate", err)
			}

			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}

		return
	})
}

func testReplicate(t *testing.T, origEvents []Event, db *badger.DB, from, to EventID) (err error) {
	t.Log("replicate events", len(origEvents))
	err = db.Update(func(txn *badger.Txn) (err error) {
		for _, evt := range origEvents {
			if err = Replicate(txn, evt); err != nil {
				return
			}
		}
		t.Log("done replicating")
		return
	})
	if err != nil {
		t.Fatal("cannot write", err)
		return
	}
	err = db.View(func(txn *badger.Txn) (err error) {
		var nextID *EventID
		events, nextID, err := Range(txn, from, to, 100)
		if nextID != nil {
			t.Fatal("nextID should be nil", nextID)
		}
		if err != nil {
			return
		}
		eqEvt := func(a, b Event) bool {
			if a.ID.Prefix != b.ID.Prefix {
				t.Fatal("mismatch prefix", a.ID, b.ID)
				return false
			}
			if a.ID.Timestamp != b.ID.Timestamp {
				t.Fatal("mismatch timestamp", a.ID, b.ID)
				return false
			}
			if a.ID.Index != b.ID.Index {
				t.Fatal("mismatch index", a.ID, b.ID)
				return false
			}
			if a.Meta != b.Meta {
				t.Fatal("mismatch meta", a.Meta, b.Meta)
				return false
			}
			if string(a.Payload) != string(b.Payload) {
				t.Fatal("mismatch payload", string(a.Payload), string(b.Payload))
				return false
			}

			return true
		}
		for idx, event := range events {
			if !eqEvt(event, origEvents[idx]) {
				t.Fatal("events mismatch", event, origEvents[idx])
			}
			// if event.ID.Prefix != 1 {
			// 	t.Fatal("wrong ID prefix", event.ID.Prefix, 1)
			// }
			// if event.ID.Timestamp != ts {
			// 	t.Fatal("wrong ID timestamp", event.ID.Timestamp, ts)
			// }
			// if event.ID.Index != uint16(idx) {
			// 	t.Fatal("wrong ID index", event.ID.Index, idx)
			// }
			// if string(event.Payload) != fmt.Sprintf("%d", idx) {
			// 	t.Fatal("wrong Event payload", string(event.Payload), idx)
			// }
		}

		return
	})
	if err != nil {
		t.Fatal("cannot read", err)
	}
	return
}

func TestOrderPrefix(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		ts := uint64(time.Now().UnixNano())
		err = db.Update(func(txn *badger.Txn) (err error) {
			if _, err = Put(txn, 1, Event{Meta: 0, Payload: []byte("first")}); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
			if _, err = Put(txn, 0, Event{Meta: 0, Payload: []byte("second")}); err != nil {
				return
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}
		var events []Event
		err = db.View(func(txn *badger.Txn) (err error) {
			events, _, err = Range(txn,
				NewEventID(0, ts, 0),
				NewEventID(1, uint64(time.Now().UnixNano()), 0), 100)
			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}
		if len(events) != 2 {
			t.Fatal("loaded != 2 events", len(events))
		}
		if string(events[0].Payload) != "second" {
			t.Fatal("0.payload is not 'second'", string(events[0].Payload))
		}
		if string(events[1].Payload) != "first" {
			t.Fatal("1.payload is not 'first'", string(events[1].Payload))
		}

		return
	})
}

func TestMatch(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		startT := uint64(time.Now().UnixNano())
		err = db.Update(func(txn *badger.Txn) (err error) {
			for i := 0; i < 100; i++ {
				_, err = Put(txn, 1, Event{Meta: 2, Payload: []byte(fmt.Sprintf("%d", i))})
				if err != nil {
					return
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			m := func(eid EventID, ev Event) bool {
				if ev.Meta == 2 {
					var n int
					fmt.Sscanf(string(ev.Payload), "%d", &n)
					return n%2 == 0
				}
				return false
			}
			var evts []Event
			// var last *EventID
			if evts, _, err = RangeMatch(txn, NewEventID(1, startT, 0), NewEventID(1, uint64(time.Now().UnixNano()), 0), 100, m); err != nil {
				return
			}
			if len(evts) != 50 {
				t.Fatal("num events should be 50", len(evts))
			}

			for _, evt := range evts {
				var val int
				fmt.Sscanf(string(evt.Payload), "%d", &val)
				if val%2 != 0 {
					t.Fatal("payload should be even", string(evt.Payload))
				}
			}

			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		return
	})
}
