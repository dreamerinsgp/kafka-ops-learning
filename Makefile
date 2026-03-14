.PHONY: build test

build:
	mkdir -p build && go build -o build/kafka-ops ./cmd

test:
	go test ./...
