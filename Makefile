.PHONY: all build clean install uninstall fmt simplify check run test

install:
	@go mod tidy

run: install
	@go run --tags sqlite_json main.go 

test:
	@go clean -testcache && go test -v -tags sqlite_json ./test/...