.PHONY: build-cni-server
build-cni-server:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o  $(CURDIR)/dist/multi-ovn-cni-server $(CURDIR)/cmd/daemon/cniserver.go
    
	
.PHONY: build-cni
build-cni:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o  $(CURDIR)/dist/multi-ovn $(CURDIR)/cmd/cni/cni.go

.PHONY: build-controller
build-controller:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o  $(CURDIR)/dist/multi-ovn-controller $(CURDIR)/cmd/controller/controller.go
    
.PHONY: build-all
build-all: build-cni-server build-cni build-controller

.PHONY: clean
clean:
	rm -rf $(CURDIR)/dist


