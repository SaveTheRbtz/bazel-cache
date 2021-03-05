IMAGE := znly/bazel-cache
VERSION := 0.0.3

.PHONY: bazel-cache
bazel-cache:
	go build -ldflags "-s -w" -trimpath -o $(@) .

.PHONY: image
image:
	docker build -t $(IMAGE):$(VERSION) .
