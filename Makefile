.PHONY: proto

default:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-extldflags "-static"' -o ./deploy/build/bifrost ./main.go

proto:
	protoc --proto_path=./proto --go_out=plugins=grpc:./pb ./proto/*.proto

clean:
	rm -rf ./pb/*.go