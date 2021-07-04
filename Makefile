protos:
	protoc -I=./vendor:./pkg/logproto --gofast_out=paths=source_relative:./pkg/logproto/ pkg/logproto/logproto.proto

clean:	
	rm ./pkg/logproto/logproto.pb.go