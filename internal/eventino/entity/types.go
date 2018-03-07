package entity

import "time"

const (
	EventKindEntity = 8
)

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
