install: qa
	go install

qa:	fmt vet lint

fmt:
	gofmt -s -w *.go

vet:
	go vet *.go

lint:
	golint *.go