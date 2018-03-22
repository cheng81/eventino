package common

import (
	"encoding/json"

	"github.com/cheng81/eventino/cmd/eventino/common/command"
	"github.com/linkedin/goavro"
)

var NetCodec *goavro.Codec
var initialSchema []map[string]interface{}

func init() {
	initialSchema = []map[string]interface{}{
		new(command.CreateEntityType).AvroSchema(),
		new(command.SchemaResponse).AvroSchema(),
		new(command.ErrorResponse).AvroSchema(),
		new(command.CreateEntityEventType).AvroSchema(),
		new(command.LoadSchema).AvroSchema(),
		new(command.LoadSchemaReply).AvroSchema(),
		new(command.CreateEntity).AvroSchema(),
		map[string]interface{}{"type": "boolean"},
		map[string]interface{}{"type": "long"},
	}

	var err error
	NetCodec, err = makeCodec(initialSchema)
	if err != nil {
		panic(err)
	}
}

func NetCodecWithSchema(scm map[string]interface{}) (*goavro.Codec, error) {
	complete := append(initialSchema, scm)
	return makeCodec(complete)
}

func makeCodec(scm []map[string]interface{}) (*goavro.Codec, error) {
	schema, err := json.Marshal(scm)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(string(schema))
}
