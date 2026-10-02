GOLANGCI_LINT_VERSION = v2.12.2

.PHONY: prereqs
prereqs:
	test -f ./bin/golangci-lint-${GOLANGCI_LINT_VERSION} || ( \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s ${GOLANGCI_LINT_VERSION} \
		&& mv ./bin/golangci-lint ./bin/golangci-lint-${GOLANGCI_LINT_VERSION})

.PHONY: vendors
vendors:
	go mod tidy && go mod vendor

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: lint
lint: prereqs
	./bin/golangci-lint-${GOLANGCI_LINT_VERSION} run ./... --timeout=5m

.PHONY: compile
compile:
	go build ./...

.PHONY: build
build: fmt compile

.PHONY: test
test:
	go test ./...
