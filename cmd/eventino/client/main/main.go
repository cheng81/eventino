package main

import (
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

	err := client.Start()
	if err != nil {
		panic(err)
	}

	vm := otto.New()
	vm.Set("reconnect", client.Start)
	vm.Set("disconnect", client.Stop)
	vm.Set("createEntityType", eventino.CreateEntityType)

	repl.RunWithOptions(vm, repl.Options{Prompt: "eventino> ", Autocomplete: true})
}
