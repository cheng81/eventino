package script

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cheng81/eventino/internal/eventino/entity"
	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/cheng81/eventino/internal/eventino/schema/schemaavro"
	"github.com/dgraph-io/badger"
	"github.com/robertkrimen/otto"
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

func TestOtto3(t *testing.T) {
	src := `
	function foo(a, b) {return a + b};
	foo;
	`
	vm := otto.New()
	val, err := vm.Run(src)
	if err != nil {
		t.Fatal("cannot eval", err)
	}
	if !val.IsFunction() {
		t.Fatal("should be function", val, val.Class())
	}
	outVal, err := val.Call(otto.NullValue(), 3, 2)
	if err != nil {
		t.Fatal("Cannot call", err)
	}
	outIVal, err := outVal.ToInteger()
	if err != nil {
		t.Fatal("Cannot cast to integer", err)
	}
	if outIVal != 5 {
		t.Fatal("result is not 5", outIVal)
	}

	// iVal, err := val.ToInteger()
	// if err != nil {
	// 	t.Fatal("cannot toInt", err)
	// }
	// if iVal != 42 {
	// 	t.Fatal("val is not 42", iVal)
	// }
	// t.Log(iVal)
}

func TestOtto2(t *testing.T) {
	src := `
	function foo(a,b) {return a+b;}
	function bar(a,b) {return a*b;}
	var o = {
		foo: foo,
		bar: bar
	};
	o;
	`

	vm := otto.New()

	val, err := vm.Run(src)
	if err != nil {
		t.Fatal("cannot eval", err)
	}
	// val, err := vm.Get("output")
	// if err != nil {
	// 	t.Fatal("cannot get output", err)
	// }
	t.Log("output", val)
	obj := val.Object()
	fooFun, err := obj.Get("foo")
	if err != nil {
		t.Fatal("cannot get foo")
	}
	fooVal, err := fooFun.Call(otto.NullValue(), 2, 3) //val.Object().Call("foo", 2, 3)
	if err != nil {
		t.Fatal("cannot call foo", err)
	}
	intVal, err := fooVal.ToInteger()
	if err != nil {
		t.Fatal("cannot convert to int", err)
	}
	if intVal != 5 {
		t.Fatal("foo(2,3) should be 5")
	}
}

func TestOtto(t *testing.T) {
	vm := otto.New()

	// src := `
	// function doit(obj, val) {
	// 	obj['answer'] = 42;
	// 	return obj;
	// }
	// return doit(theObj, theVal)
	// `

	src := `
	theObj['answer'] = theVal;
	`
	script, err := vm.Compile("script.js", src)
	// script, err := vm.Eval(src)
	// if err != nil {
	// 	t.Fatal("cannot eval", err)
	// }
	// t.Log("evaled", script, script.Class())
	obj := map[string]interface{}{}
	if err = vm.Set("theObj", obj); err != nil {
		t.Fatal("cannot set theObj", err)
	}
	if err = vm.Set("theVal", 42); err != nil {
		t.Fatal("cannot set theVal", err)
	}
	val, err := vm.Run(script)
	// val, err := vm.Call(src, nil, obj, 42)
	// val, err := script.Call(otto.NullValue(), obj, 42)
	if err != nil {
		t.Fatal("cannot call", err)
	}
	t.Log("out val", val)
	if obj["answer"] != 42 {
		t.Fatal("obj answer is not 42", obj)
	}
	theObj, err := vm.Get("theObj")
	if err != nil {
		t.Fatal("cannot get val", err)
	}
	valNative, err := theObj.Export()
	if err != nil {
		t.Fatal("cannot export val", err)
	}

	valMap, ok := valNative.(map[string]interface{})
	if !ok {
		t.Fatal("cannot coerce to map[string]interface{}")
	}
	if valMap["answer"] != 42 {
		t.Fatal("exported answer is not 42", valMap)
	}
}

const testObj string = `
var handlers = {
	Created_0: function accumCreated (acc, evt, vsn) {
		acc['name'] = evt['Name'];
		acc['paying'] = evt['Paying'];
		acc['latest'] = vsn;
		return acc;
	},
	Updated_0: function accumUpdated (acc, evt, vsn) {
		acc['email'] = evt['Email'];
		acc['latest'] = vsn;
		return acc;
	},
	Updated_1: function accumUpdated (acc, evt, vsn) {
		acc['email'] = evt['Mail'];
		acc['phone'] = evt['Phone'];
		acc['latest'] = vsn;
		return acc;
	},
};
handlers;
`

const testFun string = `
function maybeUpdate(acc, key, val) {
	acc[key] = val || acc[key]
}
function handle(eventName, eventVsn, evt, acc, vsn) {
	if (eventVsn > 0) {
		console.log("oh oh oh, too new", eventName, eventVsn);
		return acc;
	}
	acc['latest'] = vsn;
	
	maybeUpdate(acc, 'name', evt['Name']);
	maybeUpdate(acc, 'paying', evt['Paying']);
	maybeUpdate(acc, 'email', evt['Email']);
	return acc;
}
handle;
`

func TestScriptParse(t *testing.T) {
	vm := otto.New()
	_, err := vm.Compile("", testObj)
	if err != nil {
		t.Fatal("cannot compile", err)
	}
}

func TestView(t *testing.T) {
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
			if err = entity.NewEntity(txn, entTyp, entID); err != nil {
				return
			}
			createdRec := map[string]interface{}{
				"Name":   "daCheng",
				"Paying": true,
			}
			if _, err = entity.Put(txn, entTyp, entID, schema.NewEventSchemaID("Created", 0), createdRec); err != nil {
				return
			}
			updatedRec := map[string]interface{}{
				"Email":    "daCheng@daCheng.com",
				"Username": "daCheng",
				"Useful":   nil,
			}
			if _, err = entity.Put(txn, entTyp, entID, schema.NewEventSchemaID("Updated", 0), updatedRec); err != nil {
				return
			}
			updatedRec = map[string]interface{}{
				"Email":    "info@daCheng.com",
				"Username": "daddaCheng",
				"Useful":   nil,
			}
			if _, err = entity.Put(txn, entTyp, entID, schema.NewEventSchemaID("Updated", 0), updatedRec); err != nil {
				return
			}

			// if err = updateSchema(txn); err != nil {
			// 	return
			// }

			updatedRec = map[string]interface{}{
				"Mail":     "wow@muchmail-suchcheng.com",
				"Username": "diddicheng",
				"Phone":    "555-5555-55",
			}

			if _, err = entity.Put(txn, entTyp, entID, schema.NewEventSchemaID("Updated", 1), updatedRec); err != nil {
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
			var out interface{}
			var outMap map[string]interface{}

			// test obj handlers
			if out, _, err = View(txn, testObj, entTyp, entID, 0); err != nil {
				return err
			}
			t.Log("out", out)
			outMap = out.(map[string]interface{})
			t.Log("outMap", outMap)
			if outMap["name"] != "daCheng" {
				t.Fatal("name not right", outMap)
			}
			fmt.Println("output", outMap)

			// test fun handler
			if out, _, err = View(txn, testFun, entTyp, entID, 0); err != nil {
				return err
			}
			t.Log("out", out)
			outMap = out.(map[string]interface{})
			t.Log("outMap", outMap)
			if outMap["name"] != "daCheng" {
				t.Fatal("name not right", outMap)
			}
			fmt.Println("output", outMap)
			return
		})
		if err != nil {
			t.Fatal("cannot read", err)
		}
		return
	})
}

func updateSchema(txn *badger.Txn) error {
	factory := schemaavro.Factory()
	updated2Bldr := factory.NewRecord().
		SetName("UserUpdatedRecord").
		SetField("Mail", factory.SimpleType(schema.String)).
		SetField("Username", factory.SimpleType(schema.String)).
		SetField("Phone", factory.SimpleType(schema.String))
	vsn, err := schema.UpdateEventType(txn, "User", "Updated", updated2Bldr.ToDataSchema())
	if err != nil {
		return err
	}
	if vsn != 1 {
		return errors.New("vsn should be 1")
	}
	return nil
}

func mkSchema(db *badger.DB) error {
	return db.Update(func(txn *badger.Txn) (err error) {
		if err = schema.CreateEntityType(txn, "User"); err != nil {
			return
		}
		factory := schemaavro.Factory()
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

		return updateSchema(txn)
	})
}
