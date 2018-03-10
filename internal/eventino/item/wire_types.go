package item

// payload of a log.Event
type eventWire struct {
	ID        ItemID
	EventType []byte
	Payload   []byte
}

// holds list of ItemID
type aliasesWire struct {
	Aliases []ItemID
}

type viewWire struct {
	Vsn  uint64
	View []byte
}
