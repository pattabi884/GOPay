.PHONY: up down test lint build migrate-all

up:
	docker compose up -d

down:
	docker compose down

test:
	go test ./...

lint:
	golangci-lint run ./...

build:
	go build ./...

migrate-all:
	@echo "migrations will be wired service by service"
