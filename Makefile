.PHONY: vendor

vendor:
	go mod tidy ; go mod vendor

build:
	go build -mod=vendor -o ./bin/scheduler

test:
	go test ./...

fmt:
	go fmt ./...