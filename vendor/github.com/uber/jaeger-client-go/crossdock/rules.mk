XDOCK_YAML=crossdock/docker-compose.yml

.PHONY: crossdock-linux-bin
crossdock-linux-bin:
	CGO_ENABLED=0 GOOS=linux time go build -a -installsuffix cgo -o crossdock/crossdock ./crossdock

.PHONY: crossdock
crossdock: crossdock-linux-bin
	docker-compose -f $(XDOCK_YAML) kill go
	docker-compose -f $(XDOCK_YAML) rm -f go
	docker-compose -f $(XDOCK_YAML) build go
	docker-compose -f $(XDOCK_YAML) run crossdock 2>&1 | tee run-crossdock.log
	grep 'Tests passed!' run-crossdock.log

.PHONY: crossdock-fresh
crossdock-fresh: crossdock-linux-bin
	docker-compose -f $(XDOCK_YAML) kill
	docker-compose -f $(XDOCK_YAML) rm --force
	docker-compose -f $(XDOCK_YAML) pull
	docker-compose -f $(XDOCK_YAML) build
	docker-compose -f $(XDOCK_YAML) run crossdock

.PHONE: crossdock-logs
crossdock-logs:
	docker-compose -f $(XDOCK_YAML) logs
