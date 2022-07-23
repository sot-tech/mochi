FROM golang:alpine AS build-env
LABEL maintainer "SOT-TECH <service@sot-te.ch>"

# Install OS-level dependencies.
RUN apk add --no-cache curl git

# Copy our source code into the container.
WORKDIR /go/src/github.com/sot-tech/mochi
COPY . /go/src/github.com/sot-tech/mochi

# Install our golang dependencies and compile our binary.
RUN CGO_ENABLED=0 go install ./cmd/mochi

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=build-env /go/bin/mochi /mochi
COPY --from=build-env /go/src/github.com/sot-tech/mochi/dist/example_config.yaml /etc/mochi.yaml

RUN adduser -D mochi

# Expose a docker interface to our binary.
# Tracker ports
EXPOSE 6969/tcp
EXPOSE 6969/udp
# Metrics port
EXPOSE 6880/tcp

# Drop root privileges
USER mochi

ENTRYPOINT ["/mochi"]
