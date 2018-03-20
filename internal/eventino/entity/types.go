package entity

import (
	"errors"
	"time"
)

const (
	EventKindEntity = 8
)

var EventVSNNotFound error

func init() {
	EventVSNNotFound = errors.New("Event-VSN not found")
}

type EntityEventType struct {
	Name string
	VSN  uint64
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
	Type   EntityType
	ID     []byte
	Events []EntityEvent
}

type ViewFoldFunc func(interface{}, EntityEvent, uint64) (interface{}, bool, error)
