IMAGE := znly/bazel-cache

.PHONY: bazel-cache
bazel-cache:
	go build -ldflags "-s -w" -trimpath -o $(@) .

image:
	docker build -t $(IMAGE) .
