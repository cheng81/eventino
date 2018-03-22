package command

type Command interface {
	Is(map[string]interface{}) bool
	Encode() map[string]interface{}
	Decode(map[string]interface{})
	AvroSchema() map[string]interface{}
}

func IsCommand(key string, cmd map[string]interface{}) bool {
	_, ok := cmd[key]
	return ok
}

func IsData(cmd map[string]interface{}) bool {
	return IsCommand("data", cmd)
}
