package eventino

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestCreateEntType(t *testing.T) {
	withTempDB(func(db *badger.DB) (err error) {
		evt := NewEventino(db, schemaavro.Factory())
		vsn, err := evt.CreateEntityType("foo")
		if err != nil {
			t.Fatal("should not fail on create an entity type")
		}
		if vsn != 1 {
			t.Fatal("vsn should be 1")
		}

		vsn, err = evt.CreateEntityType("bar")
		if err != nil {
			t.Fatal("should not fail on create an entity type")
		}
		if vsn != 2 {
			t.Fatal("vsn should be 2")
		}
		return
	})
}
