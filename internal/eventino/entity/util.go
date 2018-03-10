package entity

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/cheng81/eventino/internal/eventino/item"
	"github.com/cheng81/eventino/internal/eventino/schema"
)

func entityID(typ schema.EntityType, ID []byte) item.ItemID {
	typName := []byte(typ.Name)
	l := len(typName)
	b := make([]byte, l+len(ID)+1)
	b[l] = byte(':')
	copy(b[0:], typName)
	copy(b[l+1:], ID)
	return item.NewItemID(1, b)
}

func entityEvt(typ schema.EntityType, evtID schema.EventSchemaID, payload interface{}) (out item.Event, err error) {
	var evtType []byte
	var evtPayload []byte
	var scm schema.DataSchema
	var ok bool

	out = item.Event{Kind: EventKindEntity}

	if scm, ok = typ.Events[evtID]; !ok {
		err = errors.New("Event not found in Entity schema")
		return
	}
	if !scm.Valid(payload) {
		err = errors.New("Wrong payload for event schema")
		return
	}

	if evtType, err = eventTypeFromSchema(typ, evtID); err != nil {
		return
	}
	if evtPayload, err = scm.Encoder().Encode(payload); err != nil {
		return
	}

	out.Type = evtType
	out.Payload = evtPayload

	return
}

type evttypeWire struct {
	Entity string
	Event  string
	VSN    uint64
}

func eventTypeFromSchema(typ schema.EntityType, evtID schema.EventSchemaID) ([]byte, error) {
	v := evttypeWire{typ.Name, evtID.Name, evtID.VSN}
	return encode(v)
}

func encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(b []byte, v interface{}) error {
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	return dec.Decode(v)
}

func mapEvents(typ schema.EntityType, evts []item.Event) ([]EntityEvent, error) {
	out := make([]EntityEvent, 0, len(evts))
	for _, evt := range evts {
		if evt.Kind == EventKindEntity {
			entEvt, err := mapEvent(typ, evt)
			if err != nil {
				return out, err
			}
			out = append(out, entEvt)
		}
	}
	return out, nil
}

func mapEvent(typ schema.EntityType, evt item.Event) (out EntityEvent, err error) {
	evttyp := evttypeWire{}
	if err = decode(evt.Type, &evttyp); err != nil {
		return
	}
	var scm schema.DataSchema
	var exists bool
	var payload interface{}

	if scm, exists = typ.Events[schema.EventSchemaID{Name: evttyp.Event, VSN: evttyp.VSN}]; !exists {
		err = errors.New("Cannot find event-VSN in entity schema")
		return
	}
	dec := scm.Decoder()
	if payload, err = dec.Decode(evt.Payload); err != nil {
		return
	}

	out = EntityEvent{
		Type:      EntityEventType{Name: evttyp.Event, VSN: evttyp.VSN},
		Timestamp: time.Unix(0, int64(evt.LogID.Timestamp)),
		Payload:   payload,
	}
	return
}
