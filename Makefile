.PHONY: all
all:
	task default

.PHONY: test
test:
	task test

.PHONY: lint
lint:
	task lint

.PHONY: format
format:
	task format

.PHONY: mock
mock:
	task mock

.PHONY: changelog
changelog:
	task changelog

.PHONY: tidy
tidy:
	task tidy
