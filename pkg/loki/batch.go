package loki

import (
	"fmt"
	"loki-client-test/pkg/logproto"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

type batch struct {
	streams   map[string]*logproto.Stream
	bytes     int
	createdAt time.Time
}

func newBatch(entries ...entry) *batch {
	b := &batch{
		streams:   map[string]*logproto.Stream{},
		bytes:     0,
		createdAt: time.Now(),
	}
	for _, e := range entries {
		b.add(e)
	}
	return b
}

func (b *batch) add(e entry) {
	b.bytes += len(e.Line)

	labels := e.labels.String()
	fmt.Println("labels", labels)
	if stream, ok := b.streams[labels]; ok {
		stream.Entries = append(stream.Entries, e.Entry)
		return
	}

	b.streams[labels] = &logproto.Stream{
		Labels:  labels,
		Entries: []logproto.Entry{e.Entry},
	}

}

func (b *batch) encode() ([]byte, int, error) {
	req, entriesCount := b.createPushRequest()
	buf, err := proto.Marshal(req)
	if err != nil {
		return nil, 0, err
	}
	buf = snappy.Encode(nil, buf)
	return buf, entriesCount, nil
}

func (b *batch) createPushRequest() (*logproto.PushRequest, int) {
	req := logproto.PushRequest{
		Streams: make([]logproto.Stream, 0, len(b.streams)),
	}

	entriesCount := 0
	for _, stream := range b.streams {
		req.Streams = append(req.Streams, *stream)
		entriesCount += len(stream.Entries)
	}
	return &req, entriesCount
}

func (b *batch) sizeBytesAfter(entry entry) int {
	return b.bytes + len(entry.Line)
}

func (b *batch) age() time.Duration {
	return time.Since(b.createdAt)
}
