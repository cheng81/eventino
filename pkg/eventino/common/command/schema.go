package command

import (
	"github.com/cheng81/eventino/internal/eventino/schema/schemaavro"
)

type CreateEntityType struct {
	Name string
}

func (c *CreateEntityType) Is(m map[string]interface{}) bool {
	_, ok := m["createEntityType"]
	return ok
}
func (c *CreateEntityType) Encode() map[string]interface{} {
	return map[string]interface{}{
		"createEntityType": map[string]interface{}{
			"name": c.Name,
		},
	}
}
func (c *CreateEntityType) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.Name = m["createEntityType"].(map[string]interface{})["name"].(string)
	}
}
func (c *CreateEntityType) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "createEntityType",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "name",
			},
		},
	}
}

type SchemaResponse struct {
	Operation string
	VSN       uint64
}

func (c *SchemaResponse) Is(m map[string]interface{}) bool {
	_, ok := m["schemaResponse"]
	return ok
}
func (c *SchemaResponse) Encode() map[string]interface{} {
	return map[string]interface{}{
		"schemaResponse": map[string]interface{}{
			"operation": c.Operation,
			"vsn":       int64(c.VSN),
		},
	}
}
func (c *SchemaResponse) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.Operation = m["schemaResponse"].(map[string]interface{})["operation"].(string)
		c.VSN = uint64(m["schemaResponse"].(map[string]interface{})["vsn"].(int64))
	}
}

func (c *SchemaResponse) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "schemaResponse",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "operation",
			},
			map[string]interface{}{
				"type": "long",
				"name": "vsn",
			},
		},
	}
}

type CreateEntityEventType struct {
	EntityType string
	EventName  string
	MetaSchema map[string]interface{}
}

func (c *CreateEntityEventType) Is(m map[string]interface{}) bool {
	_, ok := m["createEntityEventType"]
	return ok
}
func (c *CreateEntityEventType) Encode() map[string]interface{} {
	return map[string]interface{}{
		"createEntityEventType": map[string]interface{}{
			"entityType": c.EntityType,
			"eventName":  c.EventName,
			"metaSchema": c.MetaSchema,
		},
	}
}
func (c *CreateEntityEventType) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.EntityType = m["createEntityEventType"].(map[string]interface{})["entityType"].(string)
		c.EventName = m["createEntityEventType"].(map[string]interface{})["eventName"].(string)
		c.MetaSchema = m["createEntityEventType"].(map[string]interface{})["metaSchema"].(map[string]interface{})
	}
}

func (c *CreateEntityEventType) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "createEntityEventType",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "entityType",
			},
			map[string]interface{}{
				"type": "string",
				"name": "eventName",
			},
			map[string]interface{}{
				"name": "metaSchema",
				"type": schemaavro.MetaSchema,
			},
		},
	}
}

type LoadSchema struct {
	VSN uint64
}

func (c *LoadSchema) Is(m map[string]interface{}) bool {
	_, ok := m["loadSchema"]
	return ok
}
func (c *LoadSchema) Encode() map[string]interface{} {
	return map[string]interface{}{
		"loadSchema": map[string]interface{}{
			"vsn": int64(c.VSN),
		},
	}
}
func (c *LoadSchema) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.VSN = uint64(m["loadSchema"].(map[string]interface{})["vsn"].(int64))
	}
}
func (c *LoadSchema) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "loadSchema",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "long",
				"name": "vsn",
			},
		},
	}
}

type LoadSchemaReply struct {
	VSN     uint64
	Encoded []byte
}

func (c *LoadSchemaReply) Is(m map[string]interface{}) bool {
	_, ok := m["loadSchemaReply"]
	return ok
}

func (c *LoadSchemaReply) Encode() map[string]interface{} {
	return map[string]interface{}{
		"loadSchemaReply": map[string]interface{}{
			"vsn":     int64(c.VSN),
			"encoded": c.Encoded,
		},
	}
}

func (c *LoadSchemaReply) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.VSN = uint64(m["loadSchemaReply"].(map[string]interface{})["vsn"].(int64))
		c.Encoded = m["loadSchemaReply"].(map[string]interface{})["encoded"].([]byte)
	}
}

func (c *LoadSchemaReply) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "loadSchemaReply",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "long",
				"name": "vsn",
			},
			map[string]interface{}{
				"name": "encoded",
				"type": "bytes",
			},
		},
	}
}
