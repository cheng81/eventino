package main

import (
	"fmt"
	"os"

	"github.com/cheng81/eventino/cmd/eventino/server"
	"github.com/cheng81/eventino/pkg/eventino/common"
	"github.com/dgraph-io/badger"
)

func main() {
	opts := badger.DefaultOptions
	dbDir := getdir()
	opts.Dir = dbDir
	opts.ValueDir = dbDir

	if common.Envset("EVENTINO_RM") {
		defer func() {
			fmt.Println("Exiting. Remove:", dbDir)
			os.RemoveAll(dbDir)
		}()
	}

	var port int
	fmt.Sscanf(common.Getenv("EVENTINO_PORT", "7890"), "%d", &port)

	srv, err := server.NewServer(port, opts)
	if err != nil {
		fmt.Println("cannot start eventino server", err)
		panic(err)
	}

	fmt.Printf("Eventino running on :%d\n", port)
	if err = srv.Start(); err != nil {
		fmt.Println("cannot start eventino server", err)
		panic(err)
	}

	fmt.Println("Eventino exiting")
}

func getdir() string {
	return common.Getenv("EVENTINO_DATADIR", "/tmp/eventino")
}
