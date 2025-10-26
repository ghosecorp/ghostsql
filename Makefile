.PHONY: all build server test clean run

all: build

build: server

server:
	@echo "Building GhostSQL server..."
	@mkdir -p bin
	go build -o bin/ghostsql-server ./cmd/ghostsql-server

test:
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning..."
	rm -rf bin/ data/

run: build
	@echo "Starting GhostSQL server..."
	./bin/ghostsql-server

fmt:
	@echo "Formatting code..."
	go fmt ./...

vet:
	@echo "Vetting code..."
	go vet ./...