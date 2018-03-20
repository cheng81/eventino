# Eventino #

## Design ##

## Layers ##

### log ###

Storage layer for low-level time-ordered "stuff" (for the lack of a better name, those are called "events").

To each event is assigned a `key`: a `key` consists of:
 - a `prefix byte`, which is used to partition the log space
  - the only usage so far is to be able to store `schema` events in a different space than the `entity` ones. This can be used to:
    - bootstrap an eventino instance with only schema events
    - ability to back-import (AKA `unsafe`) entities from a previous point in time: even tough the schema is defined at point `x` in time, and an entity is set to be created at point `y` with `x > y`, since the events for the schema are stored in a different key space, an eventino instance can be bootstrapped by first loading the schema, and only after replaying the entity events.
 - a `timestamp uint64`, which for all practical purposes is derived from `uint64(time.Now().UnixNano())` when `Put`ting an event. This ensure each event key is monotonically increasing (sort of)
 - an `index uint16`. For extra-level of security, since it is possible to `unsafe` put an event in the log (which allows for entities back-port), if multiple events are put into the same timestamp, each one of those will be assigned an increasing index. It might be a sort of premature optimization, or defensive coding. Time will tell.

Responsibilities:
 - ensure each event is written to the store
 - ensure the latest event `key` is "bigger" than every key presently in the store
 - ability to "replicate" an event - needed to run an eventino server in "replica" mode
 - ability to query the store by range of event `key` (from..to)
 - ability to query the store by range of event `key`_and_ match function

### item ###

Slightly higher level interface on `item`s.
An item consists of an `ID` and a set of `ItemEvent`.
What can one do with an item:
 - create an item, with a given `ID`
 - delete an item from an `ID`
  - obviously the underlying log events are not deleted, only a bunch of satellite info
 - add an event, specifying `ID`, and event `kind,type,payload`
 - alias, from an alias `ID` to a source `ID`
 - check if an item exists
 - get an item latest version
  - an item `version` is just the number of events an item has

An item `ID` consists of:
 - a `type byte` (which maps to the log `prefix byte`)
 - a `ID []byte` the identifier, proper

An item `event` consists of:
 - a `kind byte`, used to help during decoding - it at least help distinguish between `system` (e.g. `CREATED`, `DELETED`) and `user` events
 - a `type []byte`
 - a `payload []byte`
 - the `log EventID` is available to, for instance, know at which `timestamp` the event happened

TODO:
facilitate range-item deletion,
change VSN key from `<itemid>:v` to `v:<itemid>`, so that we can iterate on keys `v:<itemid-pfx>`

#### Aliases ####

An `item` can be aliased, which means that a link can be created between two `ItemID`.
There cannot exists two items with the same alias, so this facility could be used to encode unique constraints, for example.

#### View WIP ####

A `view` is a fold of an item events, that produces some state. This area is highly WIP, and it's been worked out on higher layers (schema and entitities), so the support code in the item layer is purposefully very light: the only support for views is the `View` function, which accepts an item specs (`ID` and a `version`) and a `ViewFoldFunc`, which updates the state given an event.

#### Persistent view WIP ####

While getting an item which consists of just its constituent events could be useful on its own right (event sourcing system do just that), I recognize that most of the time, we just need some sort of current state.
We can query an item using the aforementioned `view` system, which generates the state every time the query is called. Alternatively we can store the view state using a `persistent view`, which can be updated at any time.
Again this is highly WIP. Nothing is stable yet.

### schema ###

The schema is implemented using the underlying layers, so that the schema too should just be another item that can then be replicated across an eventino cluster, etc etc.
This area is, obviously, WIP, and nothing is set in stone. I'm trying to reuse as much as possible things that people are already using, while also maintaining compatibility with other languages. The latter is the reason why, after a short work with `gob`, I decided to try to use `avro` (the linkedin lib) as the method to declare, and encode/decode, schemas.
This also means that when I'll implement the client/server part, `avro` will be the natural way to express RPC messages.

So far, this is what I imagine an eventino schema should feature:

- entities: a name + collection of event types
- event type: a name (and vsn, more on this later) + schema def
- schema def: an enum, simple or complex type descriptor
- enum descriptor: name (+vsn) and (only string?) values
- record descriptor: name(+vsn) and fields descriptors

The basic types are, for now: `null, bool, int, float, string, bytes`
`enums` can be created (predefined list of strings).
Complex types are: `array`, `union`, `optional` and `records` (yes optional is basically `union(null, T)`)

For example, a schema could be:

    {
      entities: {
        user: {
          [created,0]: {ref: [user.created,0]},
          [updated,0]: {ref: [user.created,0]},
          [updated,1]: {ref: [user.created,1]'}
        }
      }
      records: {
        [user.created,0]: {
          username: {type: 'string'}
          ...
        },
        [user.updated,0]: {...},
        [user.updated,1]: {...}
      }
    }

Which describes an `user` entity with 2 (yes, 2) events, `created` and `updated`.
The `updated` event has 2 versions, which references 2 versions of the `user.created` record. This allows for schema evolution, without too much headache: any version of a record, enum, event type is retained, nothing gets deleted. Which also means that "views" will have to cater for all the versions of all events (or, well, the one they are interested into).
I believe this is a sane choice. If things get too crazy (dunno, hundreds of different versions), a strategy could be to set up a replica that maps older versions to the latest known, and use that replica going forward (random idea from the spur of the moment). My belief is that domain events tend to stick, so it shouldn't be a huge problem to update an event every now and then.
The nice thing is, this way nothing breaks (at least, not in a "crash and burn" way): one can update the schema, records, and existing code should still be perfectly fine. Perhaps it would make sense to send deprecation notices to clients that connects with an outdated schema versions (yes, on connect the client will need to provide the version of the schema it can handle), but for example, events that have been created after the given version will simply not be sent.

note: I found some difficulties with avro, though. At first, I tried a custom -gob- way to store the schema, e.g. a record, but then I realized that either way, I'll need to encode the schema language itself with avro (or whatever other serialization mechanism), and then troubles began, since ideally I'd need a couple of mutually recursive data types, which apparently is not really supported by avro. You can see what I came up with in the `schemaavro/factory.go` file.

## TODO ##

### log ###

- [x] add event to log
- [x] unsafe add event to log (used for import data)
- [x] get event range
- [x] replica log

### item ###

- [x] create,add event,delete item
- [x] create item alias
- [ ] remove item alias
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

