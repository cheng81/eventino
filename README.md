# Eventino #

## TODO ##

### log ###

- [x] add event to log
- [x] unsafe add event to log (used for import data)
- [x] get event range
- [x] replica log

### item ###

- [x] create,add event,delete item
- [x] alias item
- [x] view
- [x] persistent view
- [x] replica item

### Schema ###

- [x] versioned schema
- [x] versioned entities schema
- [x] versioned entity events schema
- [x] Create entity type
- [ ] Store "index" of entity type (to be used on ent.type delete)
- [x] Delete entity type
- [ ] Drop entity "items" on delete entity type
- [x] Create new event schema
- [x] Update event schema
- [ ] types store (records, enums to be used for events, or persistent views)

### Schema - avro ###

- [x] Encode/decode types with avro
- [x] Basic types (string, boolean, null)
- [ ] Basic types (int, float, ...)
- [x] Record
- [ ] Array
- [ ] Union
- [ ] Enum

### Entities ###

- [x] Basic create
- [ ] Create - add index
- [x] Add event
- [x] Get entity
- [x] Delete entity
- [x] View (disposable)
- [ ] Persistent view

### Script ###

- [x] test entity view with otto (js)
- [ ] decide how to use script capabilities

### Server ###

TBD once the underlying layers are somewhat stable

- [ ] basic replica
- [ ] RPC server over TCP (avro, schema, entity)
- [ ] RPC client over TCP (avro, schema, entity)
- [ ] Subscriptions, single entities
- [ ] Subscriptions, multiple entities
- [ ] Subscriptions, matching events
- [ ] Subscriptions, multi entities, multi server

