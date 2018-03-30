package entity

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/cheng81/eventino/internal/eventino/schema/schemaavro"

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
	log.Println("start", dbDir)
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
			if entTyp, err = schema.GetEntityType(txn, schemaavro.Factory().Decoder(), "User", 100); err != nil {
				return
			}
			if err = NewEntity(txn, entTyp, entID); err != nil {
				return
			}
			createdRec := map[string]interface{}{
				"Name":   "daCheng",
				"Paying": true,
			}

			if _, err = Put(txn, entTyp, entID, schema.NewEventSchemaID("Created", 0), createdRec); err != nil {
				return
			}

			tags := []string{"awesome", "slice", "of", "tags"}
			if _, err = Put(txn, entTyp, entID, schema.NewEventSchemaID("Tags", 0), tags); err != nil {
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
			if entTyp, err = schema.GetEntityType(txn, schemaavro.Factory().Decoder(), "User", 100); err != nil {
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

			evt = ent.Events[1]
			if evt.Type.Name != "Tags" {
				t.Fatal("2nd event should be Tags")
			}
			arr, ok := evt.Payload.([]interface{})
			if !ok {
				t.Fatal("2nd event payload should be array", evt.Payload)
			}
			tags := []string{"awesome", "slice", "of", "tags"}
			for i, tag := range tags {
				if arr[i] != tag {
					t.Fatal("tags should be ordered equally", i, tag, arr[i])
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

func TestCreateUpdate(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		entID := []byte("chengg")
		if err = mkSchema(db); err != nil {
			t.Fatal("cannot create schema", err)
		}
		err = db.Update(func(txn *badger.Txn) (err error) {
			var entTyp schema.EntityType
			if entTyp, err = schema.GetEntityType(txn, schemaavro.Factory().Decoder(), "User", 100); err != nil {
				return
			}
			if err = NewEntity(txn, entTyp, entID); err != nil {
				return
			}
			createdRec := map[string]interface{}{
				"Name":   "daCheng",
				"Paying": true,
			}
			if _, err = Put(txn, entTyp, entID, schema.NewEventSchemaID("Created", 0), createdRec); err != nil {
				return
			}
			updatedRec := map[string]interface{}{
				"Email":    "daCheng@daCheng.com",
				"Username": "daCheng",
				"Useful":   nil,
			}
			if _, err = Put(txn, entTyp, entID, schema.NewEventSchemaID("Updated", 0), updatedRec); err != nil {
				return
			}
			updatedRec = map[string]interface{}{
				"Email":    "info@daCheng.com",
				"Username": "daddaCheng",
				"Useful":   nil,
			}
			if _, err = Put(txn, entTyp, entID, schema.NewEventSchemaID("Updated", 0), updatedRec); err != nil {
				return
			}
			return
		})
		if err != nil {
			t.Fatal("cannot write", err)
		}

		err = db.View(func(txn *badger.Txn) (err error) {
			var entTyp schema.EntityType
			if entTyp, err = schema.GetEntityType(txn, schemaavro.Factory().Decoder(), "User", 100); err != nil {
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
			ensure := func(rec map[string]interface{}, key string, expected interface{}) {
				if rec[key] != expected {
					t.Fatalf("Expected %s to be %v, but got %v - %v", key, expected, rec[key], rec)
				}
			}
			evt := ent.Events[0]
			if evt.Type.Name != "Created" {
				t.Fatal("1st event should be Created")
			}
			rec, ok := evt.Payload.(map[string]interface{})
			if !ok {
				t.Fatal("1st event payload should be record")
			}
			ensure(rec, "Name", "daCheng")
			ensure(rec, "Paying", true)

			evt = ent.Events[1]
			rec, ok = evt.Payload.(map[string]interface{})
			if !ok {
				t.Fatal("2nd event payload should be record")
			}
			if evt.Type.Name != "Updated" {
				t.Fatal("2nd event should be Updated", evt.Type.Name)
			}
			ensure(rec, "Email", "daCheng@daCheng.com")
			ensure(rec, "Username", "daCheng")

			evt = ent.Events[2]
			rec, ok = evt.Payload.(map[string]interface{})
			if !ok {
				t.Fatal("3rd event payload should be record")
			}
			if evt.Type.Name != "Updated" {
				t.Fatal("3rd event should be Updated", evt.Type.Name)
			}
			ensure(rec, "Email", "info@daCheng.com")
			ensure(rec, "Username", "daddaCheng")

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
		factory := schemaavro.Factory()
		if err = schema.CreateEntityType(txn, factory.Decoder(), "User"); err != nil {
			return
		}
		boolSchema := factory.SimpleType(schema.Bool)
		stringSchema := factory.SimpleType(schema.String)
		nullSchema := factory.SimpleType(schema.Null)

		createdBldr := factory.NewRecord().
			SetName("UserCreatedRecord").
			SetField("Name", stringSchema).
			SetField("Paying", boolSchema)
		if err = schema.CreateEntityEventType(txn, "User", "Created", createdBldr.ToDataSchema()); err != nil {
			return
		}

		updatedBldr := factory.NewRecord().
			SetName("UserUpdatedRecord").
			SetField("Email", stringSchema).
			SetField("Username", stringSchema).
			SetField("Useful", nullSchema)
		if err = schema.CreateEntityEventType(txn, "User", "Updated", updatedBldr.ToDataSchema()); err != nil {
			return
		}

		arr := factory.NewArray(stringSchema)
		if err = schema.CreateEntityEventType(txn, "User", "Tags", arr); err != nil {
			return
		}

		return
	})
}
