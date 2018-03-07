package entity

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/cheng81/eventino/internal/eventino/schema/schemagob"

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

func TestCreate(t *testing.T) {
	startT := time.Now()
	withTempDB(func(db *badger.DB) (err error) {
		entID := []byte("cheng")
		if err = mkSchema(db); err != nil {
			t.Fatal("cannot create schema", err)
		}
		err = db.Update(func(txn *badger.Txn) (err error) {
			var entTyp schema.EntityType
			if entTyp, err = schema.GetEntityType(txn, schemagob.Factory().Decoder(), "User", 100); err != nil {
				return
			}
			if err = NewEntity(txn, entTyp, entID); err != nil {
				return
			}
			createdRec := map[string]interface{}{
				"Name":   "daCheng",
				"Paying": true,
			}
			if _, err = Put(txn, entTyp, entID, "Created", createdRec); err != nil {
				return
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			viewT := time.Now()
			var entTyp schema.EntityType
			if entTyp, err = schema.GetEntityType(txn, schemagob.Factory().Decoder(), "User", 100); err != nil {
				return
			}
			var ent Entity
			if ent, err = Get(txn, entTyp, entID, 100); err != nil {
				return
			}
			t.Log("Entity.Type", ent.Type)
			t.Log("Entity.ID", string(ent.ID))
			if string(ent.ID) != string(entID) {
				t.Fatal("entity ID mismatch", string(ent.ID), string(entID))
			}
			evt := ent.Events[0]
			if evt.Type.Name != "Created" {
				t.Fatal("1st event should be Created")
			}
			if evt.Timestamp.Before(startT) {
				t.Fatal("1st event timestamp should be after start time", evt.Timestamp)
			}
			if evt.Timestamp.After(viewT) {
				t.Fatal("1st event timestamp shoudl be before view time", evt.Timestamp)
			}
			rec, ok := evt.Payload.(map[string]interface{})
			if !ok {
				t.Fatal("1st event payload should be record")
			}
			if rec["Name"] != "daCheng" {
				t.Fatal("1st event payload Name=daCheng", rec)
			}
			if rec["Paying"] != true {
				t.Fatal("1st event payload Paying=true", rec)
			}
			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}
		return
	})
}

func mkSchema(db *badger.DB) error {
	return db.Update(func(txn *badger.Txn) (err error) {
		if err = schema.CreateEntityType(txn, "User"); err != nil {
			return
		}
		factory := schemagob.Factory()
		createdSchema := factory.For(schema.Record)
		boolSchema := factory.For(schema.Bool)
		stringSchema := factory.For(schema.String)

		bldr := createdSchema.(schema.RecordSchemaBuilder)
		bldr.SetName("CreatedRecord").
			SetField("Name", stringSchema).
			SetField("Paying", boolSchema)
		if err = schema.CreateEntityEventType(txn, "User", "Created", createdSchema); err != nil {
			return
		}

		return
	})
}
