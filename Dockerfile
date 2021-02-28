FROM golang:1.16.0-alpine3.13 as build
WORKDIR /app
ADD . /app
RUN go get -d -v ./...
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath -o bazel-cache

FROM gcr.io/distroless/static-debian10
COPY --from=build /app /
CMD ["/bazel-cache"]
