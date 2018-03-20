package command

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

type EntityCreated struct {
	Name string
	VSN  uint64
}

func (c *EntityCreated) Is(m map[string]interface{}) bool {
	_, ok := m["entityCreated"]
	return ok
}
func (c *EntityCreated) Encode() map[string]interface{} {
	return map[string]interface{}{
		"entityCreated": map[string]interface{}{
			"name": c.Name,
			"vsn":  int64(c.VSN),
		},
	}
}
func (c *EntityCreated) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.Name = m["entityCreated"].(map[string]interface{})["name"].(string)
		c.VSN = uint64(m["entityCreated"].(map[string]interface{})["vsn"].(int64))
	}
}
func (c *EntityCreated) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "entityCreated",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "name",
			},
			map[string]interface{}{
				"type": "long",
				"name": "vsn",
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
