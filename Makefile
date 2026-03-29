.PHONY: build test lint bench proto docker docker-up docker-down clean

BINARY_NAME=tikr
DEV_IMAGE=tikr-dev
PROD_IMAGE=tikr
DOCKER_COMPOSE=docker compose -f docker/docker-compose.yml

# --- All build/test runs inside Docker ---

# Build the dev image (used for test/build/lint)
dev-image:
	docker build -f docker/Dockerfile.dev -t $(DEV_IMAGE) .

# Run all tests inside Docker
test: dev-image
	docker run --rm $(DEV_IMAGE) go test -race -v ./pkg/...

# Build the binary inside Docker (output to bin/)
build: dev-image
	docker run --rm -v $(PWD)/bin:/app/bin $(DEV_IMAGE) \
		sh -c "CGO_ENABLED=1 go build -o /app/bin/$(BINARY_NAME) ./cmd/tikr"

# Lint inside Docker
lint: dev-image
	docker run --rm $(DEV_IMAGE) \
		sh -c "go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest && golangci-lint run ./..."

# Benchmark inside Docker
bench: dev-image
	docker run --rm $(DEV_IMAGE) go test -bench=. -benchmem ./benchmarks/...

# Protobuf generation inside Docker
proto: dev-image
	docker run --rm -v $(PWD)/pkg:/app/pkg $(DEV_IMAGE) \
		sh -c "go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
		       go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
		       buf generate proto"

# --- Production Docker ---

# Build the production image
docker:
	docker build -f docker/Dockerfile -t $(PROD_IMAGE):latest .

# Start full stack (tikr + kafka)
docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f

# Clean
clean:
	rm -rf bin/
	docker rmi $(DEV_IMAGE) $(PROD_IMAGE) 2>/dev/null || true
