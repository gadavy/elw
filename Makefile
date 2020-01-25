NAMESPACE = github.com/TermiusOne/elw

GOFLAGS = CGO_ENABLED=0 GOOS=linux GOARCH=amd64
GOTEST_PACKAGES = $(shell go list ./...)

gomod:
	go mod download

gotest: gomod
	go test -race -v -cover -coverprofile coverage.out $(GOTEST_PACKAGES)

gobench: gomod
	go test -bench=. -benchmem

golint:
	golangci-lint run -v