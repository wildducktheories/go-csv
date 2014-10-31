install: qa
	go install
	cd csv-select && make install
	cd uniquify && make install
	cd surrogate-keys && make install

qa:	fmt vet lint

fmt:
	gofmt -s -w *.go
	cd csv-select && make fmt
	cd uniquify && make fmt
	cd surrogate-keys && make fmt

vet:
	go vet *.go
	cd csv-select && make vet
	cd uniquify && make vet
	cd surrogate-keys && make vet

lint:
	golint *.go
	cd csv-select && make lint
	cd uniquify && make lint
	cd surrogate-keys && make lint
