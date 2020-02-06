GOTEST_PACKAGES = $(shell go list ./...)

gomod:
	go mod download

gotest: gomod
	go test -race -v -cover -coverprofile coverage.out $(GOTEST_PACKAGES)

gobench: gomod
	go test -race -bench=. -benchmem $(GOTEST_PACKAGES)

golint:
	golangci-lint run -v