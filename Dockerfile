FROM golang:1.14 as builder
WORKDIR /go/src/github.com/covine/bifrost
COPY . .
RUN CGO_ENABLED=0 go build -o bifrost -a -ldflags '-extldflags "-static"' .


FROM alpine:3.8
WORKDIR /
COPY --from=builder /go/src/github.com/covine/bifrost .
EXPOSE 1883

CMD ["/bifrost"]