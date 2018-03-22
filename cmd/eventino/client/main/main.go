package main

import (
	"encoding/json"
	"fmt"

	"github.com/robertkrimen/otto"
	"github.com/robertkrimen/otto/repl"

	"github.com/cheng81/eventino/cmd/eventino/client"
	"github.com/cheng81/eventino/cmd/eventino/common"
)

func main() {
	var port int
	fmt.Sscanf(common.Getenv("EVENTINO_PORT", "7890"), "%d", &port)
	addr := common.Getenv("EVENTINO_ADDR", "localhost")

	client := client.NewClient(addr, port)
	eventino := client.Eventino()

	// err := client.Start()
	// if err != nil {
	// 	panic(err)
	// }

	vm := otto.New()
	vm.Set("connect", client.Start)
	vm.Set("disconnect", client.Stop)
	vm.Set("newEntity", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 2 {
			fmt.Println("newEntity expects 2 argument")
			return otto.UndefinedValue()
		}
		entName, _ := call.ArgumentList[0].Export()
		id, _ := call.ArgumentList[1].Export()
		err := eventino.NewEntity(entName.(string), []byte(id.(string)))
		if err != nil {
			fmt.Println("ERROR>", err.Error())
		}
		return otto.UndefinedValue()
	})
	vm.Set("storeEvent", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 4 {
			fmt.Println("storeEvent expects 4 argument")
			return otto.UndefinedValue()
		}
		entName, _ := call.ArgumentList[0].Export()
		id, _ := call.ArgumentList[1].Export()
		evtName, _ := call.ArgumentList[2].Export()
		evt, _ := call.ArgumentList[3].Export()
		vsn, err := eventino.Put(entName.(string), []byte(id.(string)), evtName.(string), evt)
		if err != nil {
			fmt.Println("ERROR>", err.Error())
		}
		out, _ := otto.ToValue(vsn)
		return out
	})
	vm.Set("createEntityType", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 1 {
			fmt.Println("createEntityType expects 1 argument")
			return otto.UndefinedValue()
		}
		name, _ := call.ArgumentList[0].Export()
		vsn, err := eventino.CreateEntityType(name.(string))
		if err != nil {
			fmt.Println("ERROR>", err.Error())
			return otto.UndefinedValue()
		}
		out, _ := otto.ToValue(vsn)
		return out
	})
	// vm.Set("createEntityType", eventino.CreateEntityType)
	vm.Set("createEventType", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 3 {
			fmt.Println("createEventType expects 3 arguments")
			return otto.UndefinedValue()
		}
		entName, _ := call.ArgumentList[0].Export()
		evtName, _ := call.ArgumentList[1].Export()
		specs, _ := call.ArgumentList[2].Export()
		vsn, err := eventino.CreateEventType(entName.(string), evtName.(string), specs)
		if err != nil {
			fmt.Println("ERROR>", err.Error())
			return otto.UndefinedValue()
		}
		out, _ := otto.ToValue(vsn)
		return out
	})
	// vm.Set("createEventType", eventino.CreateEventType)
	vm.Set("loadSchema", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 1 {
			fmt.Println("createEventType expects 1 argument")
			return otto.UndefinedValue()
		}

		vsn, _ := call.ArgumentList[0].Export()
		jsonEnc, err := eventino.LoadSchema(uint64(vsn.(int64)))
		if err != nil {
			fmt.Println("ERROR>", err.Error())
			return otto.UndefinedValue()
		}
		var schemaNative interface{}
		json.Unmarshal(jsonEnc, &schemaNative)
		out, _ := otto.ToValue(schemaNative)
		return out

	})
	vm.Set("schema", func(call otto.FunctionCall) otto.Value {
		schemaStr := client.AvroSchema()
		var res []map[string]interface{}
		err := json.Unmarshal([]byte(schemaStr), &res)
		if err != nil {
			panic(err)
		}
		val, _ := vm.ToValue(res)
		return val
	})

	repl.RunWithOptions(vm, repl.Options{Prompt: "eventino> ", Autocomplete: true})
}
