
FROM golang:1.15.2-alpine3.12 as build
WORKDIR /app
ADD . /app
RUN go get -d -v ./...
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath -o bazel-cache

FROM gcr.io/distroless/static-debian10
COPY --from=build /app /
CMD ["/bazel-cache"]
