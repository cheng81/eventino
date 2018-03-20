package command

type Command interface {
	Is(map[string]interface{}) bool
	Encode() map[string]interface{}
	Decode(map[string]interface{})
	AvroSchema() map[string]interface{}
}
