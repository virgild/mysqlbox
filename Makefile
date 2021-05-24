.DEFAULT_GOAL := lint

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: lint-deps
lint-deps:
	@GOMODULE111=off go install honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: lint
lint:
	@staticcheck -f stylish ./...

.PHONY: test
test:
	@go test -test.v -test.count 1 ./...
