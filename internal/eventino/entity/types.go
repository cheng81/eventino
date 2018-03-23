package entity

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	EventKindEntity = 8
)

var EventVSNNotFound error

func init() {
	EventVSNNotFound = errors.New("Event-VSN not found")
}

// TODO: just replace this with schema schema id?
type EntityEventType struct {
	Name string
	VSN  uint64
}

func (e EntityEventType) ToString() string {
	return fmt.Sprintf("%s_%d", e.Name, e.VSN)
}

func EventNameIDFromString(encoded string) EntityEventType {
	toks := strings.Split(encoded, "_")
	var vsn uint64
	fmt.Sscanf(toks[len(toks)-1], "%d", &vsn)
	name := strings.Join(toks[0:len(toks)-1], "_")
	return EntityEventType{Name: name, VSN: vsn}
}

type EntityEvent struct {
	Timestamp time.Time
	Type      EntityEventType
	Payload   interface{}
}

type EntityType struct {
	Name string
	VSN  uint64
}

type Entity struct {
	Type      EntityType
	VSN       uint64
	LatestVSN uint64
	ID        []byte
	Events    []EntityEvent
}

type ViewFoldFunc func(interface{}, EntityEvent, uint64) (interface{}, bool, error)
