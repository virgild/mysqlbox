.DEFAULT_GOAL := lint

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: lint-deps
lint-deps:
	@go install honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: lint
lint:
	@staticcheck -f stylish ./...

.PHONY: test
test:
	@go test -p 1 -test.count 1 ./...

.PHONY: gosec-deps
gosec-deps:
	@go install github.com/securego/gosec/v2/cmd/gosec@latest

gosec:
	@gosec -quiet -exclude=G104 ./...
