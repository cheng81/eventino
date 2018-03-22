package command

type CreateEntity struct {
	Type string
	ID   []byte
}

func (c *CreateEntity) Is(m map[string]interface{}) bool {
	_, ok := m["createEntity"]
	return ok
}
func (c *CreateEntity) Encode() map[string]interface{} {
	return map[string]interface{}{
		"createEntity": map[string]interface{}{
			"type": c.Type,
			"id":   c.ID,
		},
	}
}
func (c *CreateEntity) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.Type = m["createEntity"].(map[string]interface{})["type"].(string)
		c.ID = m["createEntity"].(map[string]interface{})["id"].([]byte)
	}
}
func (c *CreateEntity) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "createEntity",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "type",
			},
			map[string]interface{}{
				"type": "bytes",
				"name": "id",
			},
		},
	}
}
