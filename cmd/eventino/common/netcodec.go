package common

import (
	"encoding/json"

	"github.com/cheng81/eventino/cmd/eventino/common/command"
	"github.com/linkedin/goavro"
)

var NetCodec *goavro.Codec

func init() {
	schema, err := json.Marshal([]map[string]interface{}{
		new(command.CreateEntityType).AvroSchema(),
		new(command.EntityCreated).AvroSchema(),
		new(command.SchemaResponse).AvroSchema(),
		new(command.ErrorResponse).AvroSchema(),
	})
	if err != nil {
		panic(err)
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		panic(err)
	}
	NetCodec = codec
}
