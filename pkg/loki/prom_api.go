package loki

import (
	"fmt"
	"loki-client-test/pkg/logproto"

	"github.com/gogo/protobuf/proto"
)

type promApi struct{}
type PromApi interface {
	Decode(buf []byte) (*logproto.PushRequest, error)
}

func NewPromAPI() *promApi {
	return &promApi{}
}

func (p promApi) Decode(buf []byte) (*logproto.PushRequest, error) {
	var logEntry logproto.PushRequest
	err := proto.Unmarshal(buf, &logEntry)
	if err != nil {
		fmt.Println("failed to unmarshal logEntry", err)
		return nil, err
	}
	return &logEntry, nil
}

