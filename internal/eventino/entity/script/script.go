package script

import (
	"fmt"

	"github.com/cheng81/eventino/internal/eventino/entity"
	"github.com/cheng81/eventino/internal/eventino/schema"
	"github.com/dgraph-io/badger"
	"github.com/robertkrimen/otto"
)

//type ViewFoldFunc func(interface{}, EntityEvent, uint64) (interface{}, bool, error)
func buildViewFun(vm *otto.Otto, handler otto.Value) entity.ViewFoldFunc {
	fmt.Println("called buildViewFun")
	var fn func(interface{}, entity.EntityEvent, uint64) (interface{}, error)
	nullVal := otto.NullValue()

	if handler.IsFunction() {
		fn = func(acc interface{}, evt entity.EntityEvent, vsn uint64) (interface{}, error) {
			val, err := handler.Call(nullVal, evt.Type.Name, evt.Type.VSN, evt.Payload, acc, vsn)
			if err != nil {
				return nil, err
			}
			return val.Export()
		}
	} else {
		objHandler := handler.Object()
		fn = func(acc interface{}, evt entity.EntityEvent, vsn uint64) (interface{}, error) {
			k := fmt.Sprintf("%s_%d", evt.Type.Name, evt.Type.VSN)
			evtHandler, err := objHandler.Get(k)
			fmt.Println("objHandler k", k, evtHandler)
			if err != nil {
				// TODO: just discard event in this case
				return nil, err
			}
			if evtHandler.IsFunction() {
				val, err := evtHandler.Call(nullVal, acc, evt.Payload, vsn)
				fmt.Println("objHandler called", k, val)
				if err != nil {
					return nil, err
				}
				return val.Export()
			}
			return acc, nil
		}
	}

	return func(acc interface{}, evt entity.EntityEvent, vsn uint64) (interface{}, bool, error) {
		fmt.Println("SCRIPT CALLED", acc, evt.Type)
		val, err := fn(acc, evt, vsn)
		if err != nil {
			return nil, true, err
		}
		return val, false, nil
	}
}

func View(txn *badger.Txn, src string, entType schema.EntityType, ID []byte, fromVsn uint64) (interface{}, uint64, error) {
	vm := otto.New()
	handler, err := vm.Run(src)
	if err != nil {
		fmt.Println("Cannot compile", err)
	}
	initial := map[string]interface{}{}
	fmt.Println("about to call view")
	return entity.View(txn, entType, ID, fromVsn, buildViewFun(vm, handler), initial)
}
