IMAGE :=
PWD := $(shell pwd)

.PHONY: bazel-cache
bazel-cache:
	go build \
		-ldflags "-s -w" \
		-trimpath \
		-o $(@) \
		.

image:
	docker build \
		-t znly/bazel-cache \
		.
