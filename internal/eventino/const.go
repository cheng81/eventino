package eventino

// main prefixes - these effectively partition the badger key space
const (
	// PfxLog prefixes all keys of the log (i.e. all events)
	PfxLog byte = 101 // byte('e')
	// PfxItem prefixes all items keys
	PfxItem byte = 105 // byte('i')
	// PfxAlias prefixes all aliases keys
	PfxAlias byte = 97 // byte('a')
)

// event kinds - used to partition the log key space
const (
	// EventKindSchema is a log.Event Kind that identifies schema events
	EventKindSchema byte = 4
	// EventKindEntity is a log.Event Kind that identifies entity events
	EventKindEntity byte = 8
)
