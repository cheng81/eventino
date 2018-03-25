package main

import (
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro"

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
			return otto.UndefinedValue()
		}
		out, _ := otto.ToValue(vsn)
		return out
	})
	vm.Set("getEntity", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 3 {
			fmt.Println("getEntity expects 3 argument")
			return otto.UndefinedValue()
		}
		entName, _ := call.ArgumentList[0].Export()
		id, _ := call.ArgumentList[1].Export()
		vsn, _ := call.ArgumentList[2].Export()

		ent, err := eventino.GetEntity(entName.(string), []byte(id.(string)), uint64(vsn.(int64)))
		if err != nil {
			fmt.Println("ERROR>", err.Error())
			return otto.UndefinedValue()
		}

		obj, _ := vm.Object("({})")
		obj.Set("type", ent.Type.Name)
		obj.Set("typeVsn", int64(ent.Type.VSN))
		obj.Set("id", string(ent.ID))
		obj.Set("vsn", int64(ent.VSN))
		obj.Set("latest_vsn", int64(ent.LatestVSN))

		ottoEvts := make([]otto.Value, len(ent.Events))
		for i, nEvt := range ent.Events {
			evt, _ := vm.Object("({})")
			evt.Set("type", nEvt.Type.ToString())
			evt.Set("ts", nEvt.Timestamp.UnixNano())
			evt.Set("data", nEvt.Payload)
			ottoEvts[i] = evt.Value()
		}

		obj.Set("events", ottoEvts)

		// well, this _is_ quite insane..
		// b, err := json.Marshal(ottoEntity)
		// if err != nil {
		// 	fmt.Println("cannot marshal json", err)
		// 	return otto.UndefinedValue()
		// }
		// objStr := fmt.Sprintf("(%s)", string(b))
		// obj, err := vm.Object(objStr)
		// if err != nil {
		// 	fmt.Println("cannot objectify marshalled json", objStr, err)
		// 	return otto.UndefinedValue()
		// }
		// out, err := otto.ToValue(obj)
		// if err != nil {
		// 	fmt.Println("ERROR-ENCODE-ENTITY", err.Error())
		// }
		out := obj.Value()
		// fmt.Printf("otto.entity %+v\n", out)
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
		loadedVsn, _, err := eventino.LoadSchema(uint64(vsn.(int64)))
		if err != nil {
			fmt.Println("ERROR>", err.Error())
			return otto.UndefinedValue()
		}
		out, _ := otto.ToValue(loadedVsn)
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

	vm.Set("codecValidate", func(call otto.FunctionCall) otto.Value {
		scm, _ := call.Argument(0).Export()
		itm, _ := call.Argument(1).Export()

		b, _ := json.Marshal(scm)
		codec, err := goavro.NewCodec(string(b))
		if err != nil {
			out, _ := otto.ToValue(fmt.Sprintf("cannot make codec: %s", err.Error()))
			return out
		}
		_, err = codec.BinaryFromNative(nil, itm)
		if err != nil {
			out, _ := otto.ToValue(fmt.Sprintf("cannot encode: %s", err.Error()))
			return out
		}
		out, _ := otto.ToValue("success!")
		return out
	})

	repl.RunWithOptions(vm, repl.Options{Prompt: "eventino> ", Autocomplete: true})
}
