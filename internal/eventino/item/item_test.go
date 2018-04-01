package item

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cheng81/eventino/internal/eventino/log"
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
	fmt.Println("start")
	opts := badger.DefaultOptions
	opts.Dir = dbDir
	opts.ValueDir = dbDir
	db, err := badger.Open(opts)
	if err != nil {
		fmt.Println(err)
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

func TestCreate(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		// ItemID
		id := NewItemID(0, []byte("item0"))
		// Create an Item
		err = db.Update(func(txn *badger.Txn) (err error) {
			err = Create(txn, id)
			return
		})
		if err != nil {
			t.Fatal("Cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var item Item
			item, err = Get(txn, id, 0, 100)
			if err != nil {
				return err
			}
			if item.LatestVsn != 0 {
				t.Fatal("vsn should be 0", item.LatestVsn)
			}
			if item.LoadedVsn != 0 {
				t.Fatal("loaded vsn should be 0", item.LoadedVsn)
			}
			if item.ID.Type != id.Type {
				t.Fatal("mismatch ID.Type", item.ID.Type, id.Type)
			}
			if len(item.ID.ID) != len(id.ID) {
				t.Fatal("mismatch ID.ID", item.ID.ID, id.ID)
			}
			if len(item.Events) != 1 {
				t.Fatal("should have loaded 1 event (created)", len(item.Events))
			}
			if !IsCreatedEvent(item.Events[0]) {
				t.Fatal("1st event should be CREATED", item.Events)
			}

			return
		})
		if err != nil {
			t.Fatal("Cannot read", err)
		}

		return
	})
}

func TestAlias(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		// ItemID
		id := NewItemID(0, []byte("item0"))
		alias := NewItemID(0, []byte("alias.item0"))
		// Create an Item
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = Create(txn, id); err != nil {
				return
			}
			err = Alias(txn, id, alias)
			return
		})
		if err != nil {
			t.Fatal("Cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var item Item
			item, err = GetByAlias(txn, alias, 0, 100)
			if err != nil {
				return err
			}
			if item.LatestVsn != 1 {
				t.Fatal("vsn should be 1", item.LatestVsn)
			}
			if item.LoadedVsn != 1 {
				t.Fatal("loaded vsn should be 1", item.LoadedVsn)
			}
			if item.ID.Type != id.Type {
				t.Fatal("mismatch ID.Type", item.ID.Type, id.Type)
			}
			if len(item.ID.ID) != len(id.ID) {
				t.Fatal("mismatch ID.ID", item.ID.ID, id.ID)
			}
			if len(item.Events) != 2 {
				t.Fatal("should have loaded 2 events (created, aliased)", len(item.Events))
			}
			if !IsCreatedEvent(item.Events[0]) {
				t.Fatal("1st event should be CREATED", item.Events)
			}

			if !IsAliasEvent(item.Events[1]) {
				t.Fatal("1st event should be ALIASED", item.Events)
			}

			return
		})
		if err != nil {
			t.Fatal("Cannot read", err)
		}

		return
	})
}

func TestPut(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		id := NewItemID(0, []byte("foobar"))
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = Create(txn, id); err != nil {
				return
			}
			var vsn uint64
			if vsn, err = Put(txn, id, Event{Kind: 0, Type: []byte("event.type.0"), Payload: []byte("event.type.0-payload")}); err != nil {
				return
			}
			if vsn != 1 {
				t.Fatal("vsn should be 1", vsn)
			}

			return
		})
		if err != nil {
			t.Fatal("Cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var item Item
			if item, err = Get(txn, id, 0, 0); err != nil {
				return
			}
			if item.LatestVsn != 1 {
				t.Fatal("latest vsn should be 1", item.LatestVsn)
			}
			if item.LoadedVsn != 1 {
				t.Fatal("loaded vsn should be 1", item.LoadedVsn)
			}
			if len(item.Events) != 2 {
				t.Fatal("item should have 2 events", len(item.Events))
			}
			if !IsCreatedEvent(item.Events[0]) {
				t.Fatal("item event.0 should be CREATED", string(item.Events[0].Type))
			}
			if string(item.Events[1].Type) != "event.type.0" {
				t.Fatal("item event.1 TYPE should be event.type.0", string(item.Events[1].Type))
			}
			return
		})
		if err != nil {
			t.Fatal("Cannot read", err)
		}
		return
	})
}

func TestDelete(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		id := NewItemID(0, []byte("foobar"))
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = Create(txn, id); err != nil {
				return
			}
			if err = Alias(txn, id, NewItemID(0, []byte("alias.foobar"))); err != nil {
				return
			}
			et := []byte("type.test")
			for i := 0; i < 100; i++ {
				v := []byte(fmt.Sprintf("%d", i+1))
				if _, err = Put(txn, id, Event{Kind: 0, Type: et, Payload: v}); err != nil {
					return
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.Update(func(txn *badger.Txn) (err error) {
			return Delete(txn, id)
		})
		if err != nil {
			t.Fatal("cannot write (delete)", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()

			for iter.Rewind(); iter.Valid(); iter.Next() {
				// should only have log event items
				it := iter.Item()
				k := it.Key()
				if k[0] != byte('e') {
					t.Fatal("wrong key prefix - ", rune(k[0]))
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot read (delete)", err)
		}

		return
	})
}

func TestView(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		id := NewItemID(0, []byte("foobar"))
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = Create(txn, id); err != nil {
				return
			}
			et := []byte("type.test")
			for i := 0; i < 100; i++ {
				v := []byte(fmt.Sprintf("%d", i+1))
				if _, err = Put(txn, id, Event{Kind: 0, Type: et, Payload: v}); err != nil {
					return
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			v := func(acc interface{}, evt Event, _ uint64) (interface{}, bool, error) {
				if string(evt.Type) == "type.test" {
					i := acc.(int)
					var a int
					fmt.Sscanf(string(evt.Payload), "%d", &a)
					return i + a, false, nil
				}
				return acc, false, nil
			}
			var out interface{}
			if out, _, err = View(txn, id, 0, v, 0); err != nil {
				return
			}
			if out.(int) != 5050 {
				t.Fatal("sum 1..100 should be 5050", out.(int))
			}
			return
		})
		return
	})
}

type testView struct{}

func (tv testView) EncodeState(v interface{}) []byte {
	fmt.Println("ENCODING", v)
	return []byte(fmt.Sprintf("%d", v.(int)))
}
func (tv testView) DecodeState(v []byte) (interface{}, error) {
	var out int
	fmt.Sscanf(string(v), "%d", &out)
	fmt.Println("DECODED", out)
	return out, nil
}
func (tv testView) Fold(v interface{}, evt Event, _ uint64) (interface{}, bool, error) {
	if string(evt.Type) == "type.test" {
		i := v.(int)
		var a int
		fmt.Sscanf(string(evt.Payload), "%d", &a)
		fmt.Println("FOLDING", i, "->", a)
		return i + a, false, nil
	}
	return v, false, nil
}

func TestPersistentView(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		viewName := []byte("testView")
		id := NewItemID(0, []byte("foobar"))
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = Create(txn, id); err != nil {
				return
			}
			fmt.Println("adding test events")
			et := []byte("type.test")
			for i := 0; i < 50; i++ {
				v := []byte(fmt.Sprintf("%d", i+1))
				if _, err = Put(txn, id, Event{Kind: 0, Type: et, Payload: v}); err != nil {
					return
				}
			}

			if err = SyncPersistentView(txn, id, viewName, testView{}, 0); err != nil {
				return
			}

			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var v interface{}
			if _, v, err = GetView(txn, id, viewName, testView{}); err != nil {
				return
			}
			if v.(int) != 1275 {
				t.Fatal("sum 1..50 should be 1275", v.(int))
			}
			return
		})

		err = db.Update(func(txn *badger.Txn) (err error) {
			fmt.Println("adding test events (2)")
			et := []byte("type.test")
			for i := 50; i < 100; i++ {
				v := []byte(fmt.Sprintf("%d", i+1))
				if _, err = Put(txn, id, Event{Kind: 0, Type: et, Payload: v}); err != nil {
					return
				}
			}

			if err = SyncPersistentView(txn, id, viewName, testView{}, 0); err != nil {
				return
			}

			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var v interface{}
			if _, v, err = GetView(txn, id, viewName, testView{}); err != nil {
				return
			}
			if v.(int) != 5050 {
				t.Fatal("sum 1..100 should be 5050", v.(int))
			}
			return
		})

		return
	})
}

// TODO: test alias delete
// TODO: test replicate

func TestRangePrefix(t *testing.T) {
	tStart := time.Now().UnixNano()

	id1 := NewItemID(0, []byte("foo-foo1"))
	id2 := NewItemID(0, []byte("bar-bar1"))
	id3 := NewItemID(0, []byte("foo-foo2"))

	mkEvt := func(id ItemID) [][]string {
		return [][]string{
			[]string{"evt.0", fmt.Sprintf("%s-evt.0", string(id.ID))},
			[]string{"evt.1", fmt.Sprintf("%s-evt.1", string(id.ID))},
			[]string{"evt.2", fmt.Sprintf("%s-evt.2", string(id.ID))},
			[]string{"evt.3", fmt.Sprintf("%s-evt.3", string(id.ID))},
		}
	}

	withTempDB(func(db *badger.DB) (err error) {
		err = db.Update(func(txn *badger.Txn) (err error) {
			if err = createItem(txn, id1, mkEvt(id1)); err != nil {
				return
			}
			if err = createItem(txn, id2, mkEvt(id2)); err != nil {
				return
			}
			if err = createItem(txn, id3, mkEvt(id3)); err != nil {
				return
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			evts, nextID, err := RangePrefix(txn, NewItemID(0, []byte("foo-")), log.NewEventID(0, uint64(tStart), 0), log.NewEventIDNow(0), 100)
			if err != nil {
				return
			}
			if nextID != nil {
				t.Fatal("should have loaded all events", nextID)
			}
			if len(evts) != 10 { // 2 create and 8 custom
				t.Fatal("loaded events should be 10", len(evts))
			}
			for _, evt := range evts {
				if string(evt.ID.ID) != "foo-foo1" && string(evt.ID.ID) != "foo-foo2" {
					t.Fatal("loaded event id should be foo1 or foo2", string(evt.ID.ID))
				}
				if !IsCreatedEvent(evt.Event) {
					if string(evt.Event.Type) != "evt.0" && string(evt.Event.Type) != "evt.1" && string(evt.Event.Type) != "evt.2" && string(evt.Event.Type) != "evt.3" {
						t.Fatal("loaded event type should be created or evt.[0..3]", string(evt.Event.Type))
					}
				}
			}
			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}
		return
	})
}

func createItem(txn *badger.Txn, id ItemID, events [][]string) (err error) {
	if err = Create(txn, id); err != nil {
		return
	}
	for _, strs := range events {
		evt := NewEvent(0, []byte(strs[0]), []byte(strs[1]))
		if _, err = Put(txn, id, evt); err != nil {
			return
		}
	}
	return
}
