package command

type ErrorResponse struct {
	Message string
}

func NewErrorMessage(err error) *ErrorResponse {
	return &ErrorResponse{Message: err.Error()}
}

func (c *ErrorResponse) Is(m map[string]interface{}) bool {
	_, ok := m["errorResponse"]
	return ok
}
func (c *ErrorResponse) Encode() map[string]interface{} {
	return map[string]interface{}{
		"errorResponse": map[string]interface{}{
			"message": c.Message,
		},
	}
}
func (c *ErrorResponse) Decode(m map[string]interface{}) {
	if c.Is(m) {
		c.Message = m["errorResponse"].(map[string]interface{})["message"].(string)
	}
}
func (c *ErrorResponse) AvroSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "record",
		"name": "errorResponse",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"type": "string",
				"name": "message",
			},
		},
	}
}
